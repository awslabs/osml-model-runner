import logging
import multiprocessing
import os
import signal
import tempfile
import time
import uuid
from pathlib import Path
from queue import Queue
from typing import Any, Dict, Optional, Tuple

import geojson
import shapely.geometry
import shapely.wkt
from osgeo import gdal
from shapely.geometry import Polygon

from .credentials_utils import get_credentials_for_assumed_role
from .detection_service import FeatureDetector
from .exceptions import RetryableJobException
from .feature_table import FeatureTable
from .gdal_utils import (
    GDALConfigEnv,
    get_type_and_scales,
    load_gdal_dataset,
    set_gdal_default_configuration,
)
from .image_utils import ImageDimensions, generate_crops_for_region
from .job_table import JobTable
from .metrics import metric_scope, now
from .model_runner_api import ImageRequest, RegionRequest, TileCompression, TileFormats
from .result_storage import ResultStorage
from .status_monitor import StatusMonitor
from .tile_worker import ImageTileWorker
from .work_queue import WorkQueue

WORKERS_PER_CPU = os.environ.get("WORKERS_PER_CPU", "1")
JOB_TABLE = os.environ.get("JOB_TABLE", "oversight-ml-jobs")
FEATURE_TABLE = os.environ.get("FEATURE_TABLE", "oversight-ml-features")
IMAGE_QUEUE = os.environ.get("IMAGE_QUEUE", "oversight-image-queue")
REGION_QUEUE = os.environ.get("REGION_QUEUE", "oversight-region-queue")
CP_API_ENDPOINT = os.environ.get("CP_API_ENDPOINT", "")

# This global variable is setup so that SIGINT and SIGTERM can be used to stop the loop
# continuously monitoring the region and image work queues.
run = True


def handler_stop_signals(signum, frame):
    global run
    run = False


signal.signal(signal.SIGINT, handler_stop_signals)
signal.signal(signal.SIGTERM, handler_stop_signals)


def monitor_work_queues():
    # In the processing below the region work queue is checked first and will wait for up to 10
    # seconds to start work. Only if no regions need to be processed in that time will this worker
    # check to see if a new image can be started. Ultimately this setup is intended to ensure that
    # all of the regions for an image are completed by the cluster before work begins on more
    # images.
    image_work_queue = WorkQueue(IMAGE_QUEUE, wait_seconds=0, visible_seconds=20 * 60)
    image_requests_iter = iter(image_work_queue)
    region_work_queue = WorkQueue(REGION_QUEUE, wait_seconds=10, visible_seconds=20 * 60)
    region_requests_iter = iter(region_work_queue)
    status_monitor = StatusMonitor(CP_API_ENDPOINT)
    job_table = JobTable(JOB_TABLE)

    # Setup the GDAL configuration options that should remain unchanged for the life of this
    # execution
    set_gdal_default_configuration()

    while run:

        logging.debug("Checking work queue for regions to process ...")
        (receipt_handle, region_request_attributes) = next(region_requests_iter)

        if region_request_attributes is not None:
            try:
                region_request = RegionRequest(region_request_attributes)
                process_region_request(region_request, job_table)
                region_work_queue.finish_request(receipt_handle)
            except RetryableJobException:
                region_work_queue.reset_request(receipt_handle, visibility=0)
            except Exception:
                region_work_queue.finish_request(receipt_handle)
        else:

            logging.debug("Checking work queue for images to process ...")
            (receipt_handle, image_request_message) = next(image_requests_iter)

            if image_request_message is not None:
                try:
                    image_request = ImageRequest.from_external_message(image_request_message)
                    if not image_request.is_valid():
                        logging.error("Invalid Image Request! {}".format(image_request_message))
                        raise ValueError("Invalid Image Request")

                    process_image_request(
                        image_request, region_work_queue, status_monitor, job_table
                    )
                    image_work_queue.finish_request(receipt_handle)
                except RetryableJobException:
                    image_work_queue.reset_request(receipt_handle, visibility=0)
                except Exception:
                    image_work_queue.finish_request(receipt_handle)


@metric_scope
def process_image_request(
    image_request: ImageRequest, region_work_queue, status_monitor, job_table, metrics
) -> None:
    try:
        # TODO: The long term goal is to support AWS provided models hosted by this service as well
        #       as customer provided models where we're managing the endpoints internally. For an
        #       initial release we can limit processing to customer managed SageMaker Model
        #       Endpoints hence this check. The other type options should not be advertised in the
        #       API but we are including the name/type structure in the API to allow expansion
        #       through a non-breaking API change.
        if (
            image_request.model_hosting_type is None
            or image_request.model_hosting_type.casefold() != "SM_ENDPOINT".casefold()
        ):
            status_monitor.processing_event(
                image_request.job_arn,
                "FAILED",
                "Implementation only supports SageMaker Model Endpoints",
            )
            return

        status_monitor.processing_event(image_request.job_arn, "IN_PROGRESS", "Started Processing")

        if image_request.model_name == "aws-oversightml-internalnoop-model":
            status_monitor.processing_event(
                image_request.job_arn, "COMPLETED", "NOOP Model Finished"
            )
            return

        if image_request.image_url is None:
            status_monitor.processing_event(
                image_request.job_arn,
                "FAILED",
                "No image URL specified. Image URL is required.",
            )
            return

        logging.info("Starting processing of {}".format(image_request.image_url))

        job_table.image_started(image_request.image_id)

        image_type = get_image_type(image_request.image_url)

        if metrics:
            metrics.put_dimensions({"ImageFormat": image_type})

        # If this request contains an execution role retrieve credentials that will be used to
        # access data
        assumed_credentials = None
        if image_request.execution_role is not None:
            assumed_credentials = get_credentials_for_assumed_role(image_request.execution_role)

        # This will update the GDAL configuration options to use the security credentials for this
        # request. Any GDAL managed AWS calls (i.e. incrementally fetching pixels from a dataset
        # stored in S3) within this "with" statement will be made using customer credentials. At
        # the end of the "with" scope the credentials will be removed.
        with GDALConfigEnv().with_aws_credentials(assumed_credentials):
            # Use GDAL to access the dataset and geo positioning metadata
            image_gdalvfs = image_request.image_url.replace("s3:/", "/vsis3", 1)
            logging.info("Loading image with GDAL virtual file system {}".format(image_gdalvfs))
            ds, camera_model = load_gdal_dataset(image_gdalvfs, metrics)

            # Determine how much of this image should be processed.
            # Bounds are: UL corner (row, column) , dimensions (w, h)
            processing_bounds = calculate_processing_bounds(image_request.roi, ds, camera_model)
            if not processing_bounds:
                logging.info("Requested ROI does not intersect image. Nothing to do")
                job_table.image_ended(image_request.image_id)
                status_monitor.processing_event(
                    image_request.job_arn, "FAILED", "ROI Has No Intersection With Image"
                )
            else:
                # Calculate a set of ML engine sized regions that we need to process for this image
                # Region size chosen to break large images into pieces that can be handled by a
                # single tile worker
                region_size: ImageDimensions = (20480, 20480)
                region_overlap: ImageDimensions = image_request.tile_overlap
                regions = list(
                    generate_crops_for_region(processing_bounds, region_size, region_overlap)
                )

                job_table.image_stats(
                    image_request.image_id, len(regions), ds.RasterXSize, ds.RasterYSize
                )

                region_request_shared_values = {
                    "image_id": image_request.image_id,
                    "image_url": image_request.image_url,
                    "output_bucket": image_request.output_bucket,
                    "output_prefix": image_request.output_prefix,
                    "model_name": image_request.model_name,
                    "model_hosting_type": image_request.model_hosting_type,
                    "tile_size": image_request.tile_size,
                    "tile_overlap": image_request.tile_overlap,
                    "tile_format": image_request.tile_format,
                    "tile_compression": image_request.tile_compression,
                    "execution_role": image_request.execution_role,
                }

                # Process the image regions. This worker will process the first region of this
                # image since it has already loaded the dataset from S3 and is ready to go. Any
                # additional regions will be queued for processing by other workers in this
                # cluster.
                for region_number in range(1, len(regions)):
                    logging.info(
                        "Queue region {}: {}".format(region_number, regions[region_number])
                    )
                    region_request = RegionRequest(
                        region_request_shared_values, region_bounds=regions[region_number]
                    )
                    # Send the attributes of the region request as the message.
                    region_work_queue.send_request(region_request.__dict__)

                logging.info("Processing region {}: {}".format(0, regions[0]))
                region_request = RegionRequest(
                    region_request_shared_values, region_bounds=regions[0]
                )
                process_region_request(region_request, job_table, raster_dataset=ds)

        while not job_table.is_image_complete(image_request.image_id):
            # TODO: This is a hack, at a minimum put in a max retries or some other way to avoid
            #       hanging this worker
            logging.info("Waiting for other regions to complete ...")
            time.sleep(5)

        # Read all the features from DDB. The feature table handles removing duplicates
        feature_table = FeatureTable(
            FEATURE_TABLE, image_request.tile_size, image_request.tile_overlap
        )
        features = feature_table.get_all_features(image_request.image_id)

        # Create a geometry for each feature in the result. The geographic coordinates of these
        # features are computed using the camera model provided in the image metadata
        if camera_model:
            camera_model.geolocate_detections(features)
        else:
            logging.warning(
                "Dataset {} did not have a geo transform. Results are not geo-referenced.".format(
                    image_request.image_url
                )
            )

        # Write the results to S3
        result_storage = ResultStorage(
            image_request.output_bucket, image_request.output_prefix, assumed_credentials
        )
        result_storage.write_to_s3(image_request.image_id, features)

        # Record completion time of this image
        job_table.image_ended(image_request.image_id)

        status_monitor.processing_event(
            image_request.job_arn, "COMPLETED", "Successfully Completed Processing"
        )

    except Exception as e:
        logging.error("Failed to process image!")
        logging.exception(e)

        try:
            if job_table is not None and image_request.image_id is not None:
                job_table.image_ended(image_request.image_id)
        except Exception as status_error:
            logging.error("Unable to update region status in job table")
            logging.exception(status_error)

        try:
            status_monitor.processing_event(image_request.job_arn, "FAILED", str(e))

        except Exception as status_error:
            logging.error("Unable to update region status in status monitor")
            logging.exception(status_error)

        raise


@metric_scope
def process_region_request(
    region_request: RegionRequest, job_table, raster_dataset=None, metrics=None
) -> None:
    if not region_request.is_valid():
        logging.error("Invalid Region Request! {}".format(region_request.__dict__))
        raise ValueError("Invalid Region Request")

    try:
        region_start_time = now()

        logging.info(
            "Starting processing of {} {}".format(
                region_request.image_url, region_request.region_bounds
            )
        )

        image_type = get_image_type(region_request.image_url)
        if metrics:
            metrics.put_dimensions({"ImageFormat": image_type})

        # If this request contains an execution role retrieve credentials that will be used to
        # access data
        assumed_credentials = None
        if region_request.execution_role is not None:
            assumed_credentials = get_credentials_for_assumed_role(region_request.execution_role)

        image_queue: Queue = Queue()
        tile_workers = []
        for _ in range(multiprocessing.cpu_count() * int(WORKERS_PER_CPU)):
            # Ignoring mypy error - if model_name was None the call to validate the region request
            # at the start of this function would have failed
            feature_detector = FeatureDetector(
                region_request.model_name, assumed_credentials  # type: ignore[arg-type]
            )
            feature_table = FeatureTable(
                FEATURE_TABLE, region_request.tile_size, region_request.tile_overlap
            )
            worker = ImageTileWorker(image_queue, feature_detector, feature_table)
            worker.start()
            tile_workers.append(worker)
        logging.info("Setup pool of {} tile workers".format(len(tile_workers)))

        # This will update the GDAL configuration options to use the security credentials for
        # this request. Any GDAL managed AWS calls (i.e. incrementally fetching pixels from a
        # dataset stored in S3) within this "with" statement will be made using customer
        # credentials. At the end of the "with" scope the credentials will be removed.
        with GDALConfigEnv().with_aws_credentials(assumed_credentials):
            if raster_dataset is None:
                # Ignoring mypy error - if image_url was None the call to validate the region
                # request at the start of this function would have failed
                image_url = region_request.image_url
                image_gdalvfs = image_url.replace("s3:/", "/vsis3", 1)  # type: ignore[attr-defined]
                logging.info("Loading image with GDAL virtual file system {}".format(image_gdalvfs))
                raster_dataset, camera_model = load_gdal_dataset(image_gdalvfs, metrics)

            # Use the request and metadata from the raster dataset to create a set of keyword
            # arguments for the gdal.Translate() function. This will configure that function to
            # create image tiles using the format, compression, etc. needed by the CV container.
            gdal_translate_kwargs = create_gdal_translate_kwargs(region_request, raster_dataset)

            # Calculate a set of ML engine sized regions that we need to process for this image
            # and setup a temporary directory to store the temporary files. The entire directory
            # will be deleted at the end of this image's processing
            total_tile_count = 0
            with tempfile.TemporaryDirectory() as tmp:

                # Ignoring mypy error - if region_bounds was None the call to validate the region
                # request at the start of this function would have failed
                for tile_bounds in generate_crops_for_region(
                    region_request.region_bounds,  # type: ignore[arg-type]
                    region_request.tile_size,
                    region_request.tile_overlap,
                ):
                    # Create a temp file name for the NITF encoded region
                    region_image_filename = "{}-region-{}-{}-{}-{}.{}".format(
                        str(uuid.uuid4()),
                        tile_bounds[0][0],
                        tile_bounds[0][1],
                        tile_bounds[1][0],
                        tile_bounds[1][1],
                        region_request.tile_format,
                    )

                    tmp_image_path = Path(tmp, region_image_filename)

                    # Use GDAL to create an encoded tile of the image region
                    # From GDAL documentation:
                    #   srcWin --- subwindow in pixels to extract: [left_x, top_y, width, height]
                    tiling_start_time = now()
                    logging.info("Creating image tile: %s", tmp_image_path.absolute())
                    gdal.Translate(
                        str(tmp_image_path.absolute()),
                        raster_dataset,
                        srcWin=[
                            tile_bounds[0][1],
                            tile_bounds[0][0],
                            tile_bounds[1][0],
                            tile_bounds[1][1],
                        ],
                        **gdal_translate_kwargs
                    )
                    tiling_end_time = now()
                    if metrics:
                        metrics.put_metric(
                            "TilingLatency", (tiling_end_time - tiling_start_time), "Microseconds"
                        )

                    # GDAL doesn't always generate errors so we need to make sure the NITF encoded
                    # region was actually created.
                    if not tmp_image_path.is_file():
                        logging.error(
                            "GDAL unable to create tile %s. Does not exist!",
                            tmp_image_path.absolute(),
                        )
                        continue
                    else:
                        logging.info(
                            "Created %s size %s",
                            tmp_image_path.absolute(),
                            sizeof_fmt(tmp_image_path.stat().st_size),
                        )

                    # Put the image info on the tile worker queue allowing each tile to be
                    # processed in parallel.
                    image_info = {
                        "image_path": tmp_image_path,
                        "region": tile_bounds,
                        "image_id": region_request.image_id,
                    }
                    total_tile_count += 1
                    image_queue.put(image_info)

            # Put enough empty messages on the queue to shut down the workers
            for i in range(len(tile_workers)):
                image_queue.put(None)

            # Wait for all the workers to finish gracefully before we cleanup the temp directory
            for worker in tile_workers:
                worker.join()

        logging.info(
            "Model Runner Stats Processed {} image tiles for region {}.".format(
                total_tile_count, region_request.region_bounds
            )
        )

        job_table.region_complete(region_request.image_id)

        region_end_time = now()

        # Write CloudWatch Metrics to the Logs
        if metrics:
            metrics.put_metric("NumberOfRegions", 1, "Count")
            metrics.put_metric("NumberOfTiles", total_tile_count, "Count")
            metrics.put_metric(
                "RegionLatency", (region_end_time - region_start_time), "Microseconds"
            )

    except Exception as e:
        logging.error("Failed to process image region!")
        logging.exception(e)

        try:
            if job_table is not None and region_request.image_id is not None:
                job_table.region_complete(region_request.image_id, error=True)
        except Exception as status_error:
            logging.error("Unable to update region status in job table")
            logging.exception(status_error)

        raise


def create_gdal_translate_kwargs(
    region_request: RegionRequest, raster_dataset: gdal.Dataset
) -> Dict[str, Any]:
    """
    This function creates a set of keyword arguments suitable for passing to the gdal.Translate
    function. The values for these options are derived from the region processing request and
    the raster dataset itself.

    See: https://gdal.org/python/osgeo.gdal-module.html#Translate
    See: https://gdal.org/python/osgeo.gdal-module.html#TranslateOptions

    :param region_request: the region request
    :param raster_dataset: the raster dataset to translate
    :return: the dictionary of translate keyword arguments
    """
    # Figure out what type of image this is and calculate a scale that does not force any range
    # remapping
    # TODO: Consider adding an option to have this driver perform the DRA. That option would change
    #       the scale_params output by this calculation
    output_type, scale_params = get_type_and_scales(raster_dataset)

    gdal_translate_kwargs = {
        "scaleParams": scale_params,
        "outputType": output_type,
        "format": region_request.tile_format,
    }

    creation_options = ""
    if region_request.tile_format == TileFormats.NITF:
        # Creation options specific to the NITF raster driver.
        # See: https://gdal.org/drivers/raster/nitf.html
        if region_request.tile_compression is None:
            # Default NITF tiles to JPEG2000 compression if not otherwise specified
            creation_options += "IC=C8"
        elif region_request.tile_compression == TileCompression.J2K:
            creation_options += "IC=C8"
        elif region_request.tile_compression == TileCompression.JPEG:
            creation_options += "IC=C3"
        elif region_request.tile_compression == TileCompression.NONE:
            creation_options += "IC=NC"

    # TODO: Expand this to offer support for compression using other file formats

    if len(creation_options) > 0:
        gdal_translate_kwargs["creationOptions"] = creation_options

    return gdal_translate_kwargs


def calculate_processing_bounds(roi, ds: gdal.Dataset, camera_model):
    processing_bounds: Optional[Tuple[ImageDimensions, ImageDimensions]] = (
        (0, 0),
        (ds.RasterXSize, ds.RasterYSize),
    )
    if roi:
        full_image_area = Polygon(
            [(0, 0), (0, ds.RasterYSize), (ds.RasterXSize, ds.RasterYSize), (ds.RasterXSize, 0)]
        )

        # This is making the assumption that the ROI is a shapely Polygon and it only considers
        # the exterior boundary (i.e. we don't handle cases where the WKT for the ROI has holes).
        # It also assumes that the coordinates of the WKT string are in longitude latitude order
        # to match GeoJSON
        roi_area = camera_model.feature_to_image_shape(
            geojson.Feature(
                geometry=geojson.Polygon(shapely.geometry.mapping(roi)["coordinates"][0])
            )
        )

        if roi_area.intersects(full_image_area):
            area_to_process = roi_area.intersection(full_image_area)

            # Shapely bounds are (minx, miny, maxx, maxy); convert this to the ((r, c), (w, h))
            # expected by the tiler
            processing_bounds = (
                (round(area_to_process.bounds[1]), round(area_to_process.bounds[0])),
                (
                    round(area_to_process.bounds[2] - area_to_process.bounds[0]),
                    round(area_to_process.bounds[3] - area_to_process.bounds[1]),
                ),
            )
        else:
            processing_bounds = None

    return processing_bounds


def get_image_type(image_url) -> str:
    split = image_url.rsplit(".", 1)
    if len(split) == 2:
        upper_type = split[1].upper()
        if upper_type == "NTF" or upper_type == "NITF":
            upper_type = "NITF"
        elif upper_type == "TIF" or upper_type == "TIFF":
            upper_type = "TIFF"
        return upper_type
    return "UNKNOWN"


def sizeof_fmt(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, "Yi", suffix)
