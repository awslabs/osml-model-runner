import argparse
import json
import logging
import multiprocessing
import tempfile
import uuid
from pathlib import Path
from queue import Queue

import boto3
from osgeo import gdal, gdalconst

from detection_service import FeatureDetector
from feature_table import FeatureTable
from image_utils import generate_crops_for_region
from job_table import JobTable
from metrics import configure_metrics, start_metrics, stop_metrics, now, metric_scope
from result_storage import ResultStorage
from tile_worker import ImageTileWorker


def run(args):
    # TODO: Region?
    sqs_client = boto3.client('sqs')

    while True:

        logging.debug("Checking SQS queue for images to process ...")
        sqs_response = sqs_client.receive_message(
            QueueUrl=args.image_queue,
            AttributeNames=['All'],
            MessageAttributeNames=['All'],
            MaxNumberOfMessages=1,
            VisibilityTimeout=20 * 60,
            WaitTimeSeconds=20
        )

        logging.info("Received image processing request {}".format(str(sqs_response)))

        # If the wait time expires without any reads the messages property will be missing
        if 'Messages' not in sqs_response:
            continue

        for message in sqs_response['Messages']:
            logging.info('Processing message {}'.format(message['MessageId']))

            message_body = message['Body']
            logging.debug('Message Body {}'.format(message_body))

            # TODO: Add exception handling incase the message body isn't valid JSON
            image_request = json.loads(message_body)

            # TODO: Validate that the image request has the necessary information

            try:
                process_image_request(image_request)

                # Remove the message from the queue since it has been successfully processed
                sqs_client.delete_message(
                    QueueUrl=args.image_queue,
                    ReceiptHandle=message['ReceiptHandle']
                )

            except:
                pass
                # TODO: Handle processing exception here, reset visibility timeout to 0 to make immediately available


@metric_scope
def process_image_request(image_request, metrics) -> None:
    try:
        region_start_time = now()
        job_table = JobTable(args.job_table)

        # Tile size chosen to keep the output nitf size smaller than the 5 MB limit for feeding batch
        # Need to investigate use of compression and adjust tile size appropriately
        # Need to harden this to retry or reprocess in case we go over the limit
        tile_size = (1024, 1024)
        overlap = (100, 100)

        image_url = image_request['imageURL']

        job_table.image_started(image_url)

        logging.info('Starting processing of {}'.format(image_url))

        image_type = get_image_type(image_url)
        metrics.put_dimensions({"ImageFormat": image_type})

        image_queue = Queue()
        tile_workers = []
        for _ in range(multiprocessing.cpu_count() * int(args.workers_per_cpu)):
            feature_detector = FeatureDetector(image_request['modelName'])
            feature_table = FeatureTable(args.feature_table, tile_size, overlap)
            worker = ImageTileWorker(image_queue, feature_detector, feature_table)
            worker.start()
            tile_workers.append(worker)
        logging.info("Setup pool of {} tile workers".format(len(tile_workers)))

        # Use GDAL to open the image object in S3. Note that we're using GDALs s3 driver to
        # read directly from the object store as needed to complete the image operations
        metadata_start_time = now()
        image_gdalvfs = image_url.replace("s3:/", "/vsis3", 1)
        logging.info('Loading image with GDAL virtual file system {}'.format(image_gdalvfs))
        ds = gdal.Open(image_gdalvfs)
        if ds is None:
            logging.info("Skipping: %s - GDAL Unable to Process", image_gdalvfs)
            return
        logging.info("GDAL Parsed Image of size: %d x %d", ds.RasterXSize, ds.RasterYSize)
        metadata_end_time = now()
        metrics.put_metric("MetadataLatency", (metadata_end_time - metadata_start_time), "Microseconds")

        # Calculate a set of ML engine sized regions that we need to process for this image and
        # setup a temporary directory to store the temporary files. The entire directory will be
        # deleted at the end of this image's processing
        # Bounds are: UL corner (row, column) , dimensions (w, h)
        full_image_bounds = ((0, 0), (ds.RasterXSize, ds.RasterYSize))
        # TODO: Instead of processing the full image here decide if we want to break the image into multiple
        #       regions and distribute them to other processors across a second SQS queue.

        # Figure out what type of image this is. If we have values that don't fit in an 8 bit byte
        # assume that the image is 11+ bits per pixel and output it as an unsigned short.
        scale_params = []
        num_bands = ds.RasterCount
        for band_num in range(1, num_bands + 1):
            band = ds.GetRasterBand(band_num)
            (lo, hi, avg, std) = band.GetStatistics(True, True)
            scale_params.append([lo, hi, lo, hi])
            output_type = gdalconst.GDT_Byte
            if hi > 255:
                output_type = gdalconst.GDT_UInt16

        total_tile_count = 0
        with tempfile.TemporaryDirectory() as tmp:

            for region in generate_crops_for_region(full_image_bounds, tile_size, overlap):
                # Create a temp file name for the NITF encoded region
                region_image_filename = '{}-region-{}-{}-{}-{}.nitf'.format(str(uuid.uuid4()),
                                                                            region[0][0],
                                                                            region[0][1],
                                                                            region[1][0],
                                                                            region[1][1])

                tmp_image_path = Path(tmp, region_image_filename)

                # Use GDAL to create a NITF encoding of the image region
                # From GDAL documentation:
                #   srcWin --- subwindow in pixels to extract: [left_x, top_y, width, height]
                #   format --- output format ("GTiff", etc...)
                tiling_start_time = now()
                logging.info("Creating image tile: %s", tmp_image_path.absolute())
                gdal.Translate(str(tmp_image_path.absolute()), ds,
                               srcWin=[region[0][1], region[0][0], region[1][0], region[1][1]],
                               scaleParams=scale_params,
                               outputType=output_type,
                               format="nitf")
                tiling_end_time = now()
                metrics.put_metric("TilingLatency", (tiling_end_time - tiling_start_time), "Microseconds")

                # GDAL doesn't always generate errors so we need to make sure the NITF encoded region was
                # actually created.
                if not tmp_image_path.is_file():
                    logging.error("GDAL unable to create tile %s. Does not exist!", tmp_image_path.absolute())
                    continue
                else:
                    logging.info("Created %s size %s", tmp_image_path.absolute(),
                                 sizeof_fmt(tmp_image_path.stat().st_size))

                # Put the image info on the tile worker queue allowing each tile to be processed in
                # parallel.
                image_info = {
                    'image_path': tmp_image_path,
                    'region': region,
                    'image_id': image_url
                }
                total_tile_count += 1
                image_queue.put(image_info)

            # Put enough empty messages on the queue to shut down the workers
            for i in range(len(tile_workers)):
                image_queue.put(None)

            # Wait for all the workers to finish gracefully before we cleanup the temp directory
            for worker in tile_workers:
                worker.join()

        logging.info("Model Runner Stats Processed {} image tiles for a {} x {} image.".format(
            total_tile_count, ds.RasterXSize, ds.RasterYSize))

        # Read all the features from DDB and write the results to S3
        # TODO: This is the final image processing step that should be triggered at the end of all the regions
        result_storage = ResultStorage(image_request['outputBucket'], image_request['outputPrefix'])
        feature_table = FeatureTable(args.feature_table, tile_size, overlap)
        features = feature_table.get_all_features(image_url)
        result_storage.write_to_s3(image_url, features)

        # Record completion time of this image and some basic statistics about the image
        job_table.image_ended(image_url)
        job_table.image_stats(image_url, total_tile_count, ds.RasterXSize, ds.RasterYSize)

        region_end_time = now()

        # Write CloudWatch Metrics to the Logs
        metrics.put_metric("NumberOfRegions", 1, "Count")
        metrics.put_metric("NumberOfTiles", total_tile_count, "Count")
        metrics.put_metric("RegionLatency", (region_end_time - region_start_time), "Microseconds")

    except Exception as e:
        logging.error("Failed to process image!")
        logging.exception(e)


def get_image_type(image_url) -> str:
    split = image_url.rsplit(".", 1)
    if len(split) == 2:
        upper_type = split[1].upper()
        if upper_type == "NTF":
            upper_type = "NITF"
        elif upper_type == "TIF":
            upper_type = "TIFF"
        return upper_type
    return "UNKNOWN"


def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def configure_logging(verbose: bool):
    """
    Setup logging for this application
    """
    logging_level = logging.INFO
    if verbose:
        print("################## VERBOSE ##################")
        print(verbose)
        print(type(verbose))
        logging_level = logging.DEBUG

    root_logger = logging.getLogger()
    root_logger.setLevel(logging_level)

    ch = logging.StreamHandler()
    ch.setLevel(logging_level)
    formatter = logging.Formatter('%(levelname)-8s %(message)s')
    ch.setFormatter(formatter)

    root_logger.addHandler(ch)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-iq', '--image_queue', default=None)
    parser.add_argument('-rq', '--region_queue', default=None)
    parser.add_argument('-ft', '--feature_table', default=None)
    parser.add_argument('-jt', '--job_table', default=None)
    parser.add_argument('-wpc', '--workers_per_cpu', default=1)
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()

    configure_logging(args.verbose)

    configure_metrics("OversightML/ModelRunner", "cw")
    start_metrics()
    run(args)
    stop_metrics()
