import asyncio
import logging
import multiprocessing
import signal
import tempfile
import time
import uuid
from pathlib import Path
from queue import Queue
from typing import Optional, Tuple

import geojson
import shapely.geometry
import shapely.wkt
from aws_embedded_metrics.config import get_config
from aws_embedded_metrics.metric_scope import metric_scope
from aws_embedded_metrics.unit import Unit
from osgeo import gdal
from shapely.geometry import Polygon

from aws_oversightml_model_runner.classes.camera_model import CameraModel
from aws_oversightml_model_runner.classes.feature_detector import FeatureDetector
from aws_oversightml_model_runner.classes.feature_table import FeatureTable
from aws_oversightml_model_runner.classes.gdal_config import (
    GDALConfigEnv,
    set_gdal_default_configuration,
)
from aws_oversightml_model_runner.classes.image_request import ImageRequest
from aws_oversightml_model_runner.classes.job_table import JobTable
from aws_oversightml_model_runner.classes.region_request import RegionRequest
from aws_oversightml_model_runner.classes.result_storage import ResultStorage
from aws_oversightml_model_runner.classes.status_monitor import StatusMonitor
from aws_oversightml_model_runner.classes.tile_worker import TileWorker
from aws_oversightml_model_runner.classes.timer import Timer
from aws_oversightml_model_runner.classes.work_queue import WorkQueue
from aws_oversightml_model_runner.utils.constants import (
    IMAGE_PROCESSING_ERROR_METRIC,
    INVALID_REQUEST_ERROR_CODE,
    INVALID_ROI_ERROR_CODE,
    NO_IMAGE_URL_ERROR_CODE,
    PROCESSING_FAILURE_ERROR_CODE,
    REGION_LATENCY_METRIC,
    REGION_PROCESSING_ERROR_METRIC,
    REGIONS_PROCESSED_METRIC,
    SERVICE_CONFIG,
    TILE_CREATION_FAILURE_ERROR_CODE,
    TILES_PROCESSED_METRIC,
    TILING_LATENCY_METRIC,
    UNSUPPORTED_MODEL_HOST_ERROR_CODE,
)
from aws_oversightml_model_runner.utils.credentials_helper import get_credentials_for_assumed_role
from aws_oversightml_model_runner.utils.exceptions import RetryableJobException
from aws_oversightml_model_runner.utils.feature_helper import features_to_image_shapes
from aws_oversightml_model_runner.utils.gdal_helper import load_gdal_dataset
from aws_oversightml_model_runner.utils.image_helper import (
    ImageDimensions,
    create_gdal_translate_kwargs,
    generate_crops_for_region,
    get_image_type,
)

# Set up metrics/logging configuration
Config = get_config()
Config.service_name = "AWSOversightML"
Config.log_group_name = "/aws/OversightML/ModelRunner"
Config.namespace = "AWSOversightML"
Config.environment = "local"

# This global variable is setup so that SIGINT and SIGTERM can be used to stop the loop
# continuously monitoring the region and image work queues.
run = True

logger = logging.getLogger(__name__)


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
    image_work_queue = WorkQueue(
        SERVICE_CONFIG.image_queue, wait_seconds=0, visible_seconds=20 * 60
    )
    image_requests_iter = iter(image_work_queue)
    region_work_queue = WorkQueue(
        SERVICE_CONFIG.region_queue, wait_seconds=10, visible_seconds=20 * 60
    )
    region_requests_iter = iter(region_work_queue)
    status_monitor = StatusMonitor(SERVICE_CONFIG.cp_api_endpoint)
    job_table = JobTable(SERVICE_CONFIG.job_table)

    # Setup the GDAL configuration options that should remain unchanged for the life of this
    # execution
    set_gdal_default_configuration()
    try:
        loop = asyncio.get_event_loop()
        while run:

            logger.debug("Checking work queue for regions to process ...")
            (receipt_handle, region_request_attributes) = next(region_requests_iter)

            if region_request_attributes is not None:
                try:
                    region_request = RegionRequest(region_request_attributes)
                    process_region_request(region_request, job_table, event_loop=loop)
                    region_work_queue.finish_request(receipt_handle)
                except RetryableJobException:
                    region_work_queue.reset_request(receipt_handle, visibility=0)
                except Exception:
                    region_work_queue.finish_request(receipt_handle)
            else:

                logger.debug("Checking work queue for images to process ...")
                (receipt_handle, image_request_message) = next(image_requests_iter)

                if image_request_message is not None:
                    try:
                        image_request = ImageRequest.from_external_message(image_request_message)
                        if not image_request.is_valid():
                            logger.error("Invalid Image Request! {}".format(image_request_message))
                            raise ValueError("Invalid Image Request")

                        process_image_request(
                            image_request, region_work_queue, status_monitor, job_table, loop
                        )
                        image_work_queue.finish_request(receipt_handle)
                    except RetryableJobException:
                        image_work_queue.reset_request(receipt_handle, visibility=0)
                    except Exception:
                        image_work_queue.finish_request(receipt_handle)
    finally:
        loop.close()


@metric_scope
def process_image_request(
    image_request: ImageRequest,
    region_work_queue,
    status_monitor,
    job_table,
    event_loop,
    metrics=None,
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
            if metrics:
                metrics.put_dimensions({"ErrorCode": UNSUPPORTED_MODEL_HOST_ERROR_CODE})
                metrics.put_metric(IMAGE_PROCESSING_ERROR_METRIC, 1, Unit.COUNT.value)
            return

        status_monitor.processing_event(image_request.job_arn, "IN_PROGRESS", "Started Processing")

        if image_request.model_name == "aws-oversightml-internalnoop-model":
            status_monitor.processing_event(
                image_request.job_arn, "COMPLETED", "NOOP Model Finished"
            )
            return

        if image_request.image_url is None:
            status_monitor.processing_event(
                image_request.job_arn, "FAILED", "No image URL specified. Image URL is required."
            )
            if metrics:
                metrics.put_dimensions({"ErrorCode": NO_IMAGE_URL_ERROR_CODE})
                metrics.put_metric(IMAGE_PROCESSING_ERROR_METRIC, 1, Unit.COUNT.value)
            return

        logger.info("Starting processing of {}".format(image_request.image_url))

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
            logger.info("Loading image with GDAL virtual file system {}".format(image_gdalvfs))
            ds, camera_model = load_gdal_dataset(image_gdalvfs)

            # Determine how much of this image should be processed.
            # Bounds are: UL corner (row, column) , dimensions (w, h)
            processing_bounds = calculate_processing_bounds(image_request.roi, ds, camera_model)
            if not processing_bounds:
                logger.info("Requested ROI does not intersect image. Nothing to do")
                job_table.image_ended(image_request.image_id)
                status_monitor.processing_event(
                    image_request.job_arn, "FAILED", "ROI Has No Intersection With Image"
                )
                if metrics:
                    metrics.put_dimensions({"ErrorCode": INVALID_ROI_ERROR_CODE})
                    metrics.put_metric(IMAGE_PROCESSING_ERROR_METRIC, 1, Unit.COUNT.value)
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
                    logger.info("Queue region {}: {}".format(region_number, regions[region_number]))
                    region_request = RegionRequest(
                        region_request_shared_values, region_bounds=regions[region_number]
                    )
                    # Send the attributes of the region request as the message.
                    region_work_queue.send_request(region_request.__dict__)

                logger.info("Processing region {}: {}".format(0, regions[0]))
                region_request = RegionRequest(
                    region_request_shared_values, region_bounds=regions[0]
                )
                process_region_request(
                    region_request,
                    job_table,
                    raster_dataset=ds,
                    event_loop=event_loop,
                )

        while not job_table.is_image_complete(image_request.image_id):
            # TODO: This is a hack, at a minimum put in a max retries or some other way to avoid
            #       hanging this worker
            logger.info("Waiting for other regions to complete ...")
            time.sleep(5)

        # Read all the features from DDB. The feature table handles removing duplicates
        feature_table = FeatureTable(
            SERVICE_CONFIG.feature_table, image_request.tile_size, image_request.tile_overlap
        )
        features = feature_table.get_all_features(image_request.image_id)

        # Create a geometry for each feature in the result. The geographic coordinates of these
        # features are computed using the camera model provided in the image metadata
        if camera_model:
            camera_model.geolocate_detections(features)
        else:
            logger.warning(
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
        logger.error("Failed to process image!")
        logger.exception(e)
        if metrics:
            metrics.put_dimensions({"ErrorCode": PROCESSING_FAILURE_ERROR_CODE})
            metrics.put_metric(IMAGE_PROCESSING_ERROR_METRIC, 1, Unit.COUNT.value)
        try:
            if job_table is not None and image_request.image_id is not None:
                job_table.image_ended(image_request.image_id)
        except Exception as status_error:
            logger.error("Unable to update region status in job table")
            logger.exception(status_error)

        try:
            status_monitor.processing_event(image_request.job_arn, "FAILED", str(e))

        except Exception as status_error:
            logger.error("Unable to update region status in status monitor")
            logger.exception(status_error)

        raise


@metric_scope
def process_region_request(
    region_request: RegionRequest,
    job_table: JobTable,
    raster_dataset: gdal.Dataset = None,
    event_loop=None,
    metrics=None,
) -> None:
    if not region_request.is_valid():
        logger.error("Invalid Region Request! {}".format(region_request.__dict__))
        if metrics:
            metrics.put_dimensions({"ErrorCode": INVALID_REQUEST_ERROR_CODE})
            metrics.put_metric(REGION_PROCESSING_ERROR_METRIC, 1, Unit.COUNT.value)
        raise ValueError("Invalid Region Request")

    try:
        with Timer(
            task_str="Processing region {} {}".format(
                region_request.image_url, region_request.region_bounds
            ),
            metric_name=REGION_LATENCY_METRIC,
            logger=logger,
            metrics_logger=metrics,
        ):
            image_type = get_image_type(region_request.image_url)
            if metrics:
                metrics.put_dimensions({"ImageFormat": image_type})

            # If this request contains an execution role retrieve credentials that will be used to
            # access data
            assumed_credentials = None
            if region_request.execution_role is not None:
                assumed_credentials = get_credentials_for_assumed_role(
                    region_request.execution_role
                )

            image_queue: Queue = Queue()
            tile_workers = []
            for _ in range(multiprocessing.cpu_count() * SERVICE_CONFIG.workers_per_cpu):
                # Ignoring mypy error - if model_name was None the call to validate the region
                # request at the start of this function would have failed
                feature_detector = FeatureDetector(
                    region_request.model_name, assumed_credentials  # type: ignore[arg-type]
                )
                feature_table = FeatureTable(
                    SERVICE_CONFIG.feature_table,
                    region_request.tile_size,
                    region_request.tile_overlap,
                )
                # Need to pass in the current event loop due to an issue with threads
                # and aws-embedded-metrics-python
                # https://github.com/awslabs/aws-embedded-metrics-python/issues/14
                worker = TileWorker(image_queue, feature_detector, feature_table, event_loop)
                worker.start()
                tile_workers.append(worker)
            logger.info("Setup pool of {} tile workers".format(len(tile_workers)))

            # This will update the GDAL configuration options to use the security credentials for
            # this request. Any GDAL managed AWS calls (i.e. incrementally fetching pixels from a
            # dataset stored in S3) within this "with" statement will be made using customer
            # credentials. At the end of the "with" scope the credentials will be removed.
            with GDALConfigEnv().with_aws_credentials(assumed_credentials):
                if raster_dataset is None:
                    # Ignoring mypy error - if image_url was None the call to validate the region
                    # request at the start of this function would have failed
                    image_url = region_request.image_url
                    gdalvfs = image_url.replace("s3:/", "/vsis3", 1)  # type: ignore[attr-defined]
                    logger.info("Loading image with GDAL virtual file system {}".format(gdalvfs))
                    raster_dataset, camera_model = load_gdal_dataset(gdalvfs)

                # Use the request and metadata from the raster dataset to create a set of keyword
                # arguments for the gdal.Translate() function. This will configure that function to
                # create image tiles using the format, compression, etc. needed by the CV container.
                gdal_translate_kwargs = create_gdal_translate_kwargs(
                    region_request.tile_format, region_request.tile_compression, raster_dataset
                )

                # Calculate a set of ML engine sized regions that we need to process for this image
                # and setup a temporary directory to store the temporary files. The entire directory
                # will be deleted at the end of this image's processing
                total_tile_count = 0
                with tempfile.TemporaryDirectory() as tmp:

                    # Ignoring mypy error - if region_bounds was None the call to validate the
                    # region request at the start of this function would have failed
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
                        #   srcWin --- subwindow in pixels to extract:
                        #               [left_x, top_y, width, height]
                        absolute_tile_path = tmp_image_path.absolute()
                        with Timer(
                            task_str="Creating image tile: {}".format(absolute_tile_path),
                            metric_name=TILING_LATENCY_METRIC,
                            logger=logger,
                            metrics_logger=metrics,
                        ):
                            gdal.Translate(
                                str(absolute_tile_path),
                                raster_dataset,
                                srcWin=[
                                    tile_bounds[0][1],
                                    tile_bounds[0][0],
                                    tile_bounds[1][0],
                                    tile_bounds[1][1],
                                ],
                                **gdal_translate_kwargs
                            )

                        # GDAL doesn't always generate errors so we need to make sure the NITF
                        # encoded region was actually created.
                        if not tmp_image_path.is_file():
                            logger.error(
                                "GDAL unable to create tile %s. Does not exist!",
                                absolute_tile_path,
                            )
                            if metrics:
                                metrics.put_dimensions(
                                    {"ErrorCode": TILE_CREATION_FAILURE_ERROR_CODE}
                                )
                                metrics.put_metric(
                                    REGION_PROCESSING_ERROR_METRIC, 1, Unit.COUNT.value
                                )
                            continue
                        else:
                            logger.info(
                                "Created %s size %s",
                                absolute_tile_path,
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

            logger.info(
                "Model Runner Stats Processed {} image tiles for region {}.".format(
                    total_tile_count, region_request.region_bounds
                )
            )

            job_table.region_complete(str(region_request.image_id))

        # Write CloudWatch Metrics to the Logs
        if metrics:
            metrics.put_metric(REGIONS_PROCESSED_METRIC, 1, Unit.COUNT.value)
            metrics.put_metric(TILES_PROCESSED_METRIC, total_tile_count, Unit.COUNT.value)

    except Exception as e:
        logger.error("Failed to process image region!")
        logger.exception(e)
        if metrics:
            metrics.put_dimensions({"ErrorCode": PROCESSING_FAILURE_ERROR_CODE})
            metrics.put_metric(REGION_PROCESSING_ERROR_METRIC, 1, Unit.COUNT.value)
        try:
            if job_table is not None and region_request.image_id is not None:
                job_table.region_complete(region_request.image_id, error=True)
        except Exception as status_error:
            logger.error("Unable to update region status in job table")
            logger.exception(status_error)

        raise


def calculate_processing_bounds(roi, ds: gdal.Dataset, camera_model: CameraModel):
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
        roi_area = features_to_image_shapes(
            camera_model,
            [
                geojson.Feature(
                    geometry=geojson.Polygon(shapely.geometry.mapping(roi)["coordinates"][0])
                )
            ],
        )[0]

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


def sizeof_fmt(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, "Yi", suffix)
