import logging
import multiprocessing
import os
import signal
import tempfile
import time
import uuid
from pathlib import Path
from queue import Queue

from osgeo import gdal, gdalconst

from .detection_service import FeatureDetector
from .exceptions import RetryableJobException
from .feature_table import FeatureTable
from .image_utils import generate_crops_for_region
from .job_table import JobTable
from .metrics import now, metric_scope
from .result_storage import ResultStorage
from .status_monitor import StatusMonitor
from .tile_worker import ImageTileWorker
from .work_queue import WorkQueue

WORKERS_PER_CPU = os.environ['WORKERS_PER_CPU']
JOB_TABLE = os.environ['JOB_TABLE']
FEATURE_TABLE = os.environ['FEATURE_TABLE']
IMAGE_QUEUE = os.environ['IMAGE_QUEUE']
REGION_QUEUE = os.environ['REGION_QUEUE']
CP_API_ENDPOINT = os.environ['CP_API_ENDPOINT']

# Global signal term
run = True


def handler_stop_signals(signum, frame):
    global run
    run = False


signal.signal(signal.SIGINT, handler_stop_signals)
signal.signal(signal.SIGTERM, handler_stop_signals)


def monitor_work_queues():
    image_work_queue = WorkQueue(IMAGE_QUEUE, wait_seconds=0, visible_seconds=20 * 60)
    image_requests_iter = iter(image_work_queue)
    region_work_queue = WorkQueue(REGION_QUEUE, wait_seconds=10, visible_seconds=20 * 60)
    region_requests_iter = iter(region_work_queue)
    status_monitor = StatusMonitor(CP_API_ENDPOINT)

    while run:

        logging.info("Checking work queue for regions to process ...")
        (receipt_handle, region_request) = next(region_requests_iter)

        if region_request is not None:
            try:
                process_region_request(region_request)
                region_work_queue.finish_request(receipt_handle)
            except RetryableJobException as re:
                region_work_queue.reset_request(receipt_handle, visibility=0)
            except Exception as e:
                region_work_queue.finish_request(receipt_handle)
        else:

            logging.info("Checking work queue for images to process ...")
            (receipt_handle, image_request) = next(image_requests_iter)

            if image_request is not None:
                try:
                    process_image_request(image_request, region_work_queue, status_monitor)
                    image_work_queue.finish_request(receipt_handle)
                except RetryableJobException as re:
                    image_work_queue.reset_request(receipt_handle, visibility=0)
                except Exception as e:
                    image_work_queue.finish_request(receipt_handle)


@metric_scope
def process_image_request(image_request, region_work_queue, status_monitor, metrics) -> None:
    job_table = None
    image_id = None
    job_arn = None
    try:
        job_table = JobTable(JOB_TABLE)

        # Region size chosen to break large images into pieces that can be handled by a single tile worker
        region_size = (20480, 20480)
        region_overlap = (100, 100)

        tile_dimension = int(image_request['imageProcessorTileSize'])
        overlap_dimension = int(image_request['imageProcessorTileOverlap'])
        tile_size = (tile_dimension, tile_dimension)
        overlap = (overlap_dimension, overlap_dimension)
        tile_format = image_request['imageProcessorTileFormat']
        job_arn = image_request['jobArn']
        # TODO: Update to support multiple images in request
        image_url = image_request['imageUrls'][0]
        image_id = image_request['jobId'] + ":" + image_url
        output_bucket = image_request['outputBucket']
        output_prefix = image_request['outputPrefix']
        model_name = image_request['imageProcessor']

        status_monitor.processing_event(job_arn, "IN_PROGRESS", "Started Processing")

        if model_name == "aws-oversightml-internalnoop-model":
            status_monitor.processing_event(job_arn, "COMPLETED", "NOOP Model Finished")
            return

        logging.info('Starting processing of {}'.format(image_url))

        job_table.image_started(image_id)

        image_type = get_image_type(image_url)
        metrics.put_dimensions({"ImageFormat": image_type})

        ds = load_gdal_dataset(image_url, metrics)

        # Calculate a set of ML engine sized regions that we need to process for this image and
        # setup a temporary directory to store the temporary files. The entire directory will be
        # deleted at the end of this image's processing
        # Bounds are: UL corner (row, column) , dimensions (w, h)
        full_image_bounds = ((0, 0), (ds.RasterXSize, ds.RasterYSize))

        regions = list(generate_crops_for_region(full_image_bounds, region_size, region_overlap))

        job_table.image_stats(image_id, len(regions), ds.RasterXSize, ds.RasterYSize)

        region_request = {
            'ImageID': image_id,
            'imageURL': image_url,
            'outputBucket': output_bucket,
            'outputPrefix': output_prefix,
            'modelName': model_name,
            'tileSize': tile_size,
            'tileOverlap': overlap,
            'tileFormat': tile_format
        }

        for region_number in range(1, len(regions)):
            logging.info("Queue region {}: {}".format(region_number, regions[region_number]))
            region_request['region_bounds'] = regions[region_number]
            region_work_queue.send_request(region_request)

        logging.info("Processing region {}: {}".format(0, regions[0]))
        region_request['region_bounds'] = regions[0]
        process_region_request(region_request, raster_dataset=ds)

        while not job_table.is_image_complete(image_id):
            # TODO: This is a hack, at a minimum put in a max retries or some other way to avoid hanging this worker
            logging.info("Waiting for other regions to complete ...")
            time.sleep(5)

        # Read all the features from DDB and write the results to S3
        result_storage = ResultStorage(region_request['outputBucket'], region_request['outputPrefix'])
        feature_table = FeatureTable(FEATURE_TABLE, tile_size, overlap)
        features = feature_table.get_all_features(image_url)
        result_storage.write_to_s3(image_url, features)

        # Record completion time of this image
        job_table.image_ended(image_id)

        status_monitor.processing_event(job_arn, "COMPLETED", "Successfully Completed Processing")

    except Exception as e:
        logging.error("Failed to process image!")
        logging.exception(e)

        try:
            if job_table is not None and image_id is not None:
                job_table.image_ended(image_id)
        except Exception as status_error:
            logging.error("Unable to update region status in job table")
            logging.exception(status_error)

        try:
            status_monitor.processing_event(job_arn, "FAILED", str(e))

        except Exception as status_error:
            logging.error("Unable to update region status in status monitor")
            logging.exception(status_error)

        raise


@metric_scope
def process_region_request(region_request, raster_dataset=None, metrics=None) -> None:
    job_table = None
    image_id = None
    try:
        region_start_time = now()
        job_table = JobTable(JOB_TABLE)

        tile_size = region_request['tileSize']
        overlap = region_request['tileOverlap']
        tile_format = region_request['tileFormat'].lower()
        image_id = region_request['imageID']
        image_url = region_request['imageURL']

        logging.info('Starting processing of {} {}'.format(image_url, region_request['region_bounds']))

        image_type = get_image_type(image_url)
        metrics.put_dimensions({"ImageFormat": image_type})

        image_queue = Queue()
        tile_workers = []
        for _ in range(multiprocessing.cpu_count() * int(WORKERS_PER_CPU)):
            feature_detector = FeatureDetector(region_request['modelName'])
            feature_table = FeatureTable(FEATURE_TABLE, tile_size, overlap)
            worker = ImageTileWorker(image_queue, feature_detector, feature_table)
            worker.start()
            tile_workers.append(worker)
        logging.info("Setup pool of {} tile workers".format(len(tile_workers)))

        if raster_dataset is None:
            raster_dataset = load_gdal_dataset(image_url, metrics)

        # Bounds are: UL corner (row, column) , dimensions (w, h)
        region_bounds = region_request['region_bounds']

        # Figure out what type of image this is and calculate a scale that does not force any range remapping
        # TODO: Consider adding an option to have this driver perform the DRA. That option would change the
        #       scale_params output by this calculation
        output_type, scale_params = get_type_and_scales(raster_dataset)

        # Calculate a set of ML engine sized regions that we need to process for this image and
        # setup a temporary directory to store the temporary files. The entire directory will be
        # deleted at the end of this image's processing
        total_tile_count = 0
        with tempfile.TemporaryDirectory() as tmp:

            for tile_bounds in generate_crops_for_region(region_bounds, tile_size, overlap):
                # Create a temp file name for the NITF encoded region
                region_image_filename = '{}-region-{}-{}-{}-{}.{}'.format(str(uuid.uuid4()),
                                                                          tile_bounds[0][0],
                                                                          tile_bounds[0][1],
                                                                          tile_bounds[1][0],
                                                                          tile_bounds[1][1],
                                                                          tile_format
                                                                          )

                tmp_image_path = Path(tmp, region_image_filename)

                # Use GDAL to create an encoded tile of the image region
                # From GDAL documentation:
                #   srcWin --- subwindow in pixels to extract: [left_x, top_y, width, height]
                #   format --- output format ("GTiff", etc...)
                tiling_start_time = now()
                logging.info("Creating image tile: %s", tmp_image_path.absolute())
                gdal.Translate(str(tmp_image_path.absolute()), raster_dataset,
                               srcWin=[tile_bounds[0][1], tile_bounds[0][0], tile_bounds[1][0], tile_bounds[1][1]],
                               scaleParams=scale_params,
                               outputType=output_type,
                               format=tile_format)
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
                    'region': tile_bounds,
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
            total_tile_count, raster_dataset.RasterXSize, raster_dataset.RasterYSize))

        job_table.region_complete(image_id)

        region_end_time = now()

        # Write CloudWatch Metrics to the Logs
        metrics.put_metric("NumberOfRegions", 1, "Count")
        metrics.put_metric("NumberOfTiles", total_tile_count, "Count")
        metrics.put_metric("RegionLatency", (region_end_time - region_start_time), "Microseconds")

    except Exception as e:
        logging.error("Failed to process image region!")
        logging.exception(e)

        try:
            if job_table is not None and image_id is not None:
                job_table.region_complete(image_id, error=True)
        except Exception as status_error:
            logging.error("Unable to update region status in job table")
            logging.exception(status_error)

        raise


def load_gdal_dataset(image_url, metrics=None):
    # Use GDAL to open the image object in S3. Note that we're using GDALs s3 driver to
    # read directly from the object store as needed to complete the image operations
    metadata_start_time = now()
    image_gdalvfs = image_url.replace("s3:/", "/vsis3", 1)
    logging.info('Loading image with GDAL virtual file system {}'.format(image_gdalvfs))
    ds = gdal.Open(image_gdalvfs)
    if ds is None:
        logging.info("Skipping: %s - GDAL Unable to Process", image_gdalvfs)
        raise ValueError("GDAL Unable to Load: {}".format(image_url))
    logging.info("GDAL Parsed Image of size: %d x %d", ds.RasterXSize, ds.RasterYSize)
    metadata_end_time = now()
    if metrics is not None:
        metrics.put_metric("MetadataLatency", (metadata_end_time - metadata_start_time), "Microseconds")
    return ds


def get_type_and_scales(raster_dataset):
    scale_params = []
    num_bands = raster_dataset.RasterCount
    output_type = gdalconst.GDT_Byte
    min = 0
    max = 255
    for band_num in range(1, num_bands + 1):
        band = raster_dataset.GetRasterBand(band_num)
        output_type = band.DataType
        if output_type == gdalconst.GDT_Byte:
            min = 0
            max = 255
        elif output_type == gdalconst.GDT_UInt16:
            min = 0
            max = 65535
        elif output_type == gdalconst.GDT_Int16:
            min = -32768
            max = 32767
        elif output_type == gdalconst.GDT_UInt32:
            min = 0
            max = 4294967295
        elif output_type == gdalconst.GDT_Int32:
            min = -2147483648
            max = 2147483647
        else:
            logging.warning("Image uses unsupported GDAL datatype {}. Defaulting to [0,255] range".format(output_type))

        scale_params.append([min, max, min, max])

    return output_type, scale_params


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
