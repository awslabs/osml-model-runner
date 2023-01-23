import ast
import json
import logging
import uuid
from datetime import datetime
from decimal import Decimal
from json import dumps
from typing import List, Optional, Tuple

import geojson
import shapely.geometry.base
from aws_embedded_metrics.logger.metrics_logger import MetricsLogger
from aws_embedded_metrics.metric_scope import metric_scope
from aws_embedded_metrics.unit import Unit
from geojson import Feature
from osgeo import gdal
from osgeo.gdal import Dataset

from .api import ImageRequest, InvalidImageRequestException, RegionRequest
from .app_config import MetricLabels, ServiceConfig
from .common import (
    EndpointUtils,
    ImageDimensions,
    ImageRegion,
    ImageRequestStatus,
    ModelHostingOptions,
    Timer,
    build_embedded_metrics_config,
    get_credentials_for_assumed_role,
)
from .database import (
    EndpointStatisticsTable,
    FeatureTable,
    JobItem,
    JobTable,
    RegionRequestItem,
    RegionRequestTable,
)
from .exceptions import (
    AggregateFeaturesException,
    InvalidFeaturePropertiesException,
    InvalidImageURLException,
    LoadImageException,
    ProcessImageException,
    ProcessRegionException,
    RetryableJobException,
    SelfThrottledRegionException,
    UnsupportedModelException,
)
from .gdal import (
    GDALConfigEnv,
    GDALDigitalElevationModelTileFactory,
    get_image_extension,
    load_gdal_dataset,
    set_gdal_default_configuration,
)
from .inference import calculate_processing_bounds, get_source_property
from .photogrammetry import DigitalElevationModel, ElevationModel, SensorModel, SRTMTileSet
from .queue import RequestQueue
from .sink import SinkMode
from .status import StatusMonitor
from .tile_worker import generate_crops, process_tiles, setup_tile_workers

# Set up metrics configuration
Config = build_embedded_metrics_config()

# Set up logging configuration
logger = logging.getLogger(__name__)


class ModelRunner:
    """
    Main class for operating the ModelRunner application. It monitors input queues for processing requests,
    decomposes the image into a set of smaller regions and tiles, invokes an ML model endpoint with each tile, and
    finally aggregates all the results into a single output which can be deposited into the desired output sinks.
    """

    def __init__(self) -> None:
        self.image_request_queue = RequestQueue(ServiceConfig.image_queue, wait_seconds=0)
        self.image_requests_iter = iter(self.image_request_queue)
        self.job_table = JobTable(ServiceConfig.job_table)
        self.region_request_table = RegionRequestTable(ServiceConfig.region_request_table)
        self.endpoint_statistics_table = EndpointStatisticsTable(
            ServiceConfig.endpoint_statistics_table
        )
        self.region_request_queue = RequestQueue(ServiceConfig.region_queue, wait_seconds=10)
        self.region_requests_iter = iter(self.region_request_queue)
        self.status_monitor = StatusMonitor()
        self.elevation_model = ModelRunner.create_elevation_model()
        self.endpoint_utils = EndpointUtils()
        self.running = False

    def run(self) -> None:
        """
        Starts ModelRunner in a loop that continuously monitors the image work queue and region work queue.

        :return: None

        """
        self.monitor_work_queues()

    def stop(self) -> None:
        """
        Stops ModelRunner by setting the global run variable to False.

        :return: None
        """
        self.running = False

    @staticmethod
    def create_elevation_model() -> Optional[ElevationModel]:
        """
        Create an elevation model if the relevant options are set in the service configuration.

        :return: the elevation model or None if not configured
        """
        if ServiceConfig.elevation_data_location:
            return DigitalElevationModel(
                SRTMTileSet(
                    version=ServiceConfig.elevation_data_version,
                    format_extension=ServiceConfig.elevation_data_extension,
                ),
                GDALDigitalElevationModelTileFactory(ServiceConfig.elevation_data_location),
            )

        return None

    def monitor_work_queues(self) -> None:
        """
        Monitors SQS queues for ImageRequest and RegionRequest The region work queue is checked first and will wait for
        up to 10 seconds to start work. Only if no regions need to be processed in that time will this worker check to
        see if a new image can be started. Ultimately this setup is intended to ensure that all the regions for an image
        are completed by the cluster before work begins on more images.

        :return: None
        """
        # Set the running state to True
        self.running = True

        # Set up the GDAL configuration options that should remain unchanged for the life of this execution
        set_gdal_default_configuration()
        try:
            while self.running:
                logger.debug("Checking work queue for regions to process ...")
                (receipt_handle, region_request_attributes) = next(self.region_requests_iter)

                # If we found a region request on the queue
                if region_request_attributes is not None:
                    try:
                        # Parse the message into a working RegionRequest
                        region_request = RegionRequest(region_request_attributes)

                        # If the image request has a s3 url lets augment its path for virtual hosting
                        if "s3:/" in region_request.image_url:
                            # Validate that image exists in S3
                            ImageRequest.validate_image_path(
                                region_request.image_url, region_request.image_read_role
                            )
                            image_path = region_request.image_url.replace("s3:/", "/vsis3", 1)
                        else:
                            image_path = region_request.image_url

                        # Load the image into a GDAL dataset
                        raster_dataset, sensor_model = load_gdal_dataset(image_path)

                        # Get RegionRequestItem if not create new RegionRequestItem
                        region_request_item = self.region_request_table.get_region_request(
                            region_request.region_id, region_request.image_id
                        )
                        if region_request_item is None:
                            region_pixel_bounds = f"{region_request.region_bounds[0]}{region_request.region_bounds[1]}"
                            region_request_item = RegionRequestItem(
                                image_id=region_request.image_id,
                                job_id=region_request.job_id,
                                region_pixel_bounds=region_pixel_bounds,
                                region_id=region_request.region_id,
                            )
                            self.region_request_table.start_region_request(region_request_item)
                            logging.info(
                                "Adding region request: imageid: {0} - regionid: {1}".format(
                                    region_request_item.image_id, region_request_item.region_id
                                )
                            )

                        # Process our region request
                        image_request_item = self.process_region_request(
                            region_request, region_request_item, raster_dataset, sensor_model
                        )

                        # Check if the image is complete
                        if self.job_table.is_image_request_complete(image_request_item):
                            # If so complete the image request
                            self.complete_image_request(region_request)

                        # Update the queue
                        self.region_request_queue.finish_request(receipt_handle)
                    except RetryableJobException:
                        self.region_request_queue.reset_request(
                            receipt_handle, visibility_timeout=0
                        )
                    except SelfThrottledRegionException:
                        self.region_request_queue.reset_request(
                            receipt_handle,
                            visibility_timeout=int(ServiceConfig.throttling_retry_timeout),
                        )
                    except Exception as err:
                        logger.error(
                            "There was a problem processing the region request: {}".format(err)
                        )
                        self.region_request_queue.finish_request(receipt_handle)
                else:
                    logger.debug("Checking work queue for images to process ...")
                    (receipt_handle, image_request_message) = next(self.image_requests_iter)

                    # If we found a request on the queue
                    if image_request_message is not None:
                        try:
                            # Parse the message into a working ImageRequest
                            image_request = ImageRequest.from_external_message(
                                image_request_message
                            )

                            # Check that our image request looks good
                            if not image_request.is_valid():
                                error = "Invalid image request: {}".format(image_request_message)
                                logger.exception(error)
                                raise InvalidImageRequestException(error)

                            # Process the request
                            self.process_image_request(image_request)

                            # Update the queue
                            self.image_request_queue.finish_request(receipt_handle)
                        except RetryableJobException:
                            self.image_request_queue.reset_request(
                                receipt_handle, visibility_timeout=0
                            )
                        except Exception as err:
                            logger.error(
                                "There was a problem processing the image request: {}".format(err)
                            )
                            self.image_request_queue.finish_request(receipt_handle)
        finally:
            # If we stop monitoring the queue set run state to false
            self.running = False

    @metric_scope
    def process_image_request(
        self,
        image_request: ImageRequest,
        metrics: MetricsLogger = None,
    ) -> None:
        """
        Processes ImageRequest objects that are picked up from  queue. Loads the specified image into memory to be
        chipped apart into regions and sent downstream for processing via RegionRequest. This will also process the
        first region chipped from the image. # This worker will process the first region of this image since it has
        already loaded the dataset from S3 and is ready to go. Any additional regions will be queued for processing by
        other workers in this cluster.

        :param image_request: The image request derived from the ImageRequest SQS message
        :param metrics: The metrics logger to use to report metrics.
        :return: None
        """
        if isinstance(metrics, MetricsLogger):
            metrics.set_dimensions()
        try:
            if ServiceConfig.self_throttling:
                max_regions = self.endpoint_utils.calculate_max_regions(
                    image_request.model_name, image_request.model_invocation_role
                )
                # Add entry to the endpoint statistics table
                self.endpoint_statistics_table.upsert_endpoint(
                    image_request.model_name, max_regions
                )

            # Update the image status to started and include relevant image meta-data
            logger.info("Starting processing of {}".format(image_request.image_url))
            image_request_item = JobItem(
                image_id=image_request.image_id,
                job_id=image_request.job_id,
                job_arn=image_request.job_arn,
                tile_size=str(image_request.tile_size),
                tile_overlap=str(image_request.tile_overlap),
                model_name=image_request.model_name,
                model_hosting_type=image_request.model_hosting_type,
                outputs=dumps(image_request.outputs),
                image_url=image_request.image_url,
                image_read_role=image_request.image_read_role,
                feature_properties=dumps(image_request.feature_properties),
            )

            # Start the image processing
            self.job_table.start_image_request(image_request_item)
            self.status_monitor.process_event(
                image_request_item, ImageRequestStatus.STARTED, "Started image request"
            )

            # Check we have a valid image request, throws if not
            self.validate_model_hosting(image_request_item, metrics)

            # Load the relevant image meta data into memory
            image_extension, raster_dataset, sensor_model, all_regions = self.load_image_request(
                image_request_item, image_request.roi, metrics
            )

            if sensor_model is None:
                logging.warning(
                    "Dataset {} did not have a geo transform. Results are not geo-referenced.".format(
                        image_request_item.image_id
                    )
                )

            # If we got valid outputs
            if raster_dataset and all_regions:
                image_request_item.region_count = Decimal(len(all_regions))
                image_request_item.width = Decimal(raster_dataset.RasterXSize)
                image_request_item.height = Decimal(raster_dataset.RasterYSize)

                feature_properties: List[dict] = json.loads(image_request_item.feature_properties)

                # If we can get a valid source metadata from the source image - attach it to features
                # else, just pass in whatever custom features if they were provided
                source_metadata = get_source_property(image_extension, raster_dataset)
                if isinstance(source_metadata, dict):
                    feature_properties.append(source_metadata)

                # Update the feature properties
                image_request_item.feature_properties = json.dumps(feature_properties)

                # Update the image request job to have new derived image data
                image_request_item = self.job_table.update_image_request(image_request_item)

                self.status_monitor.process_event(
                    image_request_item, ImageRequestStatus.IN_PROGRESS, "Processing regions"
                )

                # Place the resulting region requests on the appropriate work queue
                self.queue_region_request(all_regions, image_request, raster_dataset, sensor_model)

        except Exception as err:
            # We failed try and gracefully update our image request
            self.fail_image_request(JobItem(image_request.image_id), err, metrics)

            # Let the application know that we failed to process image
            raise ProcessImageException("Failed to process image region!") from err

    @metric_scope
    def queue_region_request(
        self,
        all_regions: List[ImageRegion],
        image_request: ImageRequest,
        raster_dataset: Dataset,
        sensor_model: Optional[SensorModel],
    ) -> None:
        # Set aside the first region
        first_region = all_regions.pop(0)
        for region in all_regions:
            logger.info("Queueing region: {}".format(region))

            region_pixel_bounds = f"{region[0]}{region[1]}"
            region_id = f"{region_pixel_bounds}-{str(uuid.uuid4())}"
            region_request = RegionRequest(
                image_request.get_shared_values(), region_bounds=region, region_id=region_id
            )

            # Create a new entry to the region request being started
            region_request_item = RegionRequestItem(
                image_id=image_request.image_id,
                job_id=image_request.job_id,
                region_pixel_bounds=region_pixel_bounds,
                region_id=region_id,
            )
            self.region_request_table.start_region_request(region_request_item)
            logging.info(
                "Adding region request: imageid: {0} - regionid: {1}".format(
                    region_request_item.image_id, region_request_item.region_id
                )
            )

            # Send the attributes of the region request as the message.
            self.region_request_queue.send_request(region_request.__dict__)

        # Go ahead and process the first region
        logger.info("Processing first region {}: {}".format(0, first_region))

        region_pixel_bounds = f"{first_region[0]}{first_region[1]}"
        region_id = f"{region_pixel_bounds}-{str(uuid.uuid4())}"
        first_region_request = RegionRequest(
            image_request.get_shared_values(),
            region_bounds=first_region,
            region_id=region_id,
        )

        # Add item to RegionRequestTable
        region_request_item = RegionRequestItem(
            image_id=image_request.image_id,
            job_id=image_request.job_id,
            region_pixel_bounds=region_pixel_bounds,
            region_id=region_id,
        )
        self.region_request_table.start_region_request(region_request_item)
        logging.info(
            "Adding region request: imageid: {0} - regionid: {1}".format(
                region_request_item.image_id, region_request_item.region_id
            )
        )

        # Processes our region request and return the updated item
        image_request_item = self.process_region_request(
            first_region_request, region_request_item, raster_dataset, sensor_model
        )

        # If the image is finished then complete it
        if self.job_table.is_image_request_complete(image_request_item):
            self.complete_image_request(first_region_request)

    @metric_scope
    def process_region_request(
        self,
        region_request: RegionRequest,
        region_request_item: RegionRequestItem,
        raster_dataset: gdal.Dataset,
        sensor_model: Optional[SensorModel] = None,
        metrics: MetricsLogger = None,
    ) -> JobItem:
        """
        Processes RegionRequest objects that are delegated for processing. Loads the specified region of an image into
        memory to be processed by tile-workers. If a raster_dataset is not provided directly it will poll the image
        from the region request.

        :param region_request_item:
        :param region_request: the region request
        :param raster_dataset: the raster dataset containing the region
        :param sensor_model: the sensor model for this raster dataset
        :param metrics: The metrics logger to use to report metrics.
        :return: None
        """
        if isinstance(metrics, MetricsLogger):
            metrics.set_dimensions()
        if not region_request.is_valid():
            logger.error("Invalid Region Request! {}".format(region_request.__dict__))
            if isinstance(metrics, MetricsLogger):
                metrics.put_metric(MetricLabels.INVALID_REQUEST, 1, str(Unit.COUNT.value))
                metrics.put_metric(MetricLabels.REGION_PROCESSING_ERROR, 1, str(Unit.COUNT.value))
            raise ValueError("Invalid Region Request")

        if ServiceConfig.self_throttling:
            max_regions = self.endpoint_utils.calculate_max_regions(
                region_request.model_name, region_request.model_invocation_role
            )
            # Add entry to the endpoint statistics table
            self.endpoint_statistics_table.upsert_endpoint(region_request.model_name, max_regions)
            in_progress = self.endpoint_statistics_table.current_in_progress_regions(
                region_request.model_name
            )

            if in_progress >= max_regions:
                if isinstance(metrics, MetricsLogger):
                    metrics.put_metric(
                        MetricLabels.REGIONS_SELF_THROTTLED, 1, str(Unit.COUNT.value)
                    )
                logger.info(
                    f"Throttling region request. (Max: {max_regions} In-progress: {in_progress}"
                )
                raise SelfThrottledRegionException

        try:
            if ServiceConfig.self_throttling:
                # Increment the endpoint region counter
                self.endpoint_statistics_table.increment_region_count(region_request.model_name)

            with Timer(
                task_str="Processing region {} {}".format(
                    region_request.image_url, region_request.region_bounds
                ),
                metric_name=MetricLabels.REGION_LATENCY,
                logger=logger,
                metrics_logger=metrics,
            ):
                # Set up our threaded tile worker pool
                tile_queue, tile_workers = setup_tile_workers(
                    region_request,
                    sensor_model,
                    self.elevation_model,
                    metrics,
                )

                # Process all our tiles
                total_tile_count = process_tiles(
                    region_request,
                    tile_queue,
                    tile_workers,
                    raster_dataset,
                    metrics,
                )

                # Update table w/ total tile counts
                region_request_item.total_tiles = Decimal(total_tile_count)
                region_request_item = self.region_request_table.update_region_request(
                    region_request_item
                )

            # Update the image request to complete this region
            image_request_item = self.job_table.complete_region_request(region_request.image_id)

            # Update region request table if that region succeeded
            region_request_item = self.region_request_table.complete_region_request(
                region_request_item
            )

            if ServiceConfig.self_throttling:
                # Decrement the endpoint region counter
                self.endpoint_statistics_table.decrement_region_count(region_request.model_name)
            # Write CloudWatch Metrics to the Logs
            if isinstance(metrics, MetricsLogger):
                metrics.put_metric(MetricLabels.REGIONS_PROCESSED, 1, str(Unit.COUNT.value))
                metrics.put_metric(
                    MetricLabels.TILES_PROCESSED, total_tile_count, str(Unit.COUNT.value)
                )
            # Return the updated item
            return image_request_item
        except Exception as err:
            logger.error("Failed to process image region: {}", err)

            # update the table to take in that exception
            region_request_item.message = "Failed to process image region: {0}".format(err)

            if ServiceConfig.self_throttling:
                # Decrement the endpoint region counter
                self.endpoint_statistics_table.decrement_region_count(region_request.model_name)

            return self.fail_region_request(region_request_item, metrics)

    @staticmethod
    def load_image_request(
        image_request_item: JobItem,
        roi: shapely.geometry.base.BaseGeometry,
        metrics: MetricsLogger = None,
    ) -> Tuple[str, Dataset, Optional[SensorModel], List[ImageRegion]]:
        """
        Loads the required image file metadata into memory to be chipped apart into regions and
        distributed for region processing.

        :param roi:
        :param image_request_item: The region request to update.
        :param metrics: The metrics logger to use to report metrics.
        :return: Tuple[Queue, List[TileWorker]: A list of tile workers and the queue that manages them
        """
        # If this request contains an execution role retrieve credentials that will be used to
        # access data
        assumed_credentials = None
        if image_request_item.image_read_role:
            assumed_credentials = get_credentials_for_assumed_role(
                image_request_item.image_read_role
            )

        # This will update the GDAL configuration options to use the security credentials for this
        # request. Any GDAL managed AWS calls (i.e. incrementally fetching pixels from a dataset
        # stored in S3) within this "with" statement will be made using customer credentials. At
        # the end of the "with" scope the credentials will be removed.
        with GDALConfigEnv().with_aws_credentials(assumed_credentials):
            # Use GDAL to access the dataset and geo positioning metadata
            if not image_request_item.image_url:
                if isinstance(metrics, MetricsLogger):
                    metrics.put_metric(MetricLabels.NO_IMAGE_URL, 1, str(Unit.COUNT.value))
                    metrics.put_metric(
                        MetricLabels.IMAGE_PROCESSING_ERROR, 1, str(Unit.COUNT.value)
                    )
                raise InvalidImageURLException("No image URL specified. Image URL is required.")

            # If the image request have a valid s3 image url, otherwise this is a local file
            if "s3:/" in image_request_item.image_url:
                # Validate that image exists in S3
                ImageRequest.validate_image_path(
                    image_request_item.image_url, image_request_item.image_read_role
                )

                image_path = image_request_item.image_url.replace("s3:/", "/vsis3", 1)
            else:
                image_path = image_request_item.image_url

            # Use gdal to load the image url we were given
            raster_dataset, sensor_model = load_gdal_dataset(image_path)
            if isinstance(metrics, MetricsLogger):
                image_extension = get_image_extension(image_path)
                metrics.put_dimensions({"ImageFormat": image_extension})

            # Determine how much of this image should be processed.
            # Bounds are: UL corner (row, column) , dimensions (w, h)
            processing_bounds = calculate_processing_bounds(raster_dataset, roi, sensor_model)
            if not processing_bounds:
                logger.info("Requested ROI does not intersect image. Nothing to do")
                if isinstance(metrics, MetricsLogger):
                    metrics.put_metric(MetricLabels.INVALID_ROI, 1, str(Unit.COUNT.value))
                    metrics.put_metric(
                        MetricLabels.IMAGE_PROCESSING_ERROR, 1, str(Unit.COUNT.value)
                    )
                raise LoadImageException("Failed to create processing bounds for image!")
            else:
                # Calculate a set of ML engine sized regions that we need to process for this image
                # Region size chosen to break large images into pieces that can be handled by a
                # single tile worker
                region_size: ImageDimensions = ast.literal_eval(ServiceConfig.region_size)
                if not image_request_item.tile_overlap:
                    region_overlap = (0, 0)
                else:
                    region_overlap = ast.literal_eval(image_request_item.tile_overlap)

                all_regions = generate_crops(processing_bounds, region_size, region_overlap)

        return image_extension, raster_dataset, sensor_model, all_regions

    def fail_image_request(
        self, image_request_item: JobItem, err: Exception, metrics: MetricsLogger = None
    ):
        """
        Handles failure events/exceptions for image requests and tries to update the status monitor accordingly
        :param err: The exception that caused the failure
        :param image_request_item: The image request that failed.
        :param metrics: The metrics logger to use to report metrics.
        :return: Tuple[Queue, List[TileWorker]: A list of tile workers and the queue that manages them
        """
        logger.exception("Failed to start image processing!: {}".format(err))
        if isinstance(metrics, MetricsLogger):
            metrics.put_metric(MetricLabels.PROCESSING_FAILURE, 1, str(Unit.COUNT.value))
            metrics.put_metric(MetricLabels.IMAGE_PROCESSING_ERROR, 1, str(Unit.COUNT.value))
        self.job_table.end_image_request(image_request_item.image_id)
        self.status_monitor.process_event(image_request_item, ImageRequestStatus.FAILED, str(err))

    def complete_image_request(
        self,
        region_request: RegionRequest,
        metrics: MetricsLogger = None,
    ):
        """
        Runs after every region has completed processing to check if that was the last region and run required
        completion logic for the associated ImageRequest.

        :param metrics:
        :param region_request: The region request to update.
        :return: None
        """
        try:
            # Grab the full image request item from the table
            image_request_item = self.job_table.get_image_request(region_request.image_id)

            # Check if the image request is finished
            logger.info(
                "Last region of image request was completed, aggregating features for image!"
            )
            # Set up our feature table to work with the region quest
            feature_table = FeatureTable(
                ServiceConfig.feature_table, region_request.tile_size, region_request.tile_overlap
            )
            # Aggregate all the features from our job to the right outputs
            features = self.aggregate_features(image_request_item, feature_table)

            features = self.add_properties_to_features(image_request_item, features)

            # Sink the features into the right outputs
            self.sync_features(image_request_item, features)

            if isinstance(metrics, MetricsLogger):
                # Record model used for this image
                metrics.set_dimensions()
                metrics.put_dimensions({"ModelName": image_request_item.model_name})

            # Put our end time on our image_request_item
            completed_image_request_item = self.job_table.end_image_request(
                image_request_item.image_id
            )

            # Ensure we have a valid start time for our record
            if completed_image_request_item.processing_time is not None:
                image_request_status = self.status_monitor.get_image_request_status(
                    completed_image_request_item
                )
                self.status_monitor.process_event(
                    completed_image_request_item, image_request_status, "Completed image processing"
                )
                if isinstance(metrics, MetricsLogger):
                    processing_time = float(completed_image_request_item.processing_time)
                    metrics.put_metric(
                        MetricLabels.IMAGE_LATENCY, processing_time, str(Unit.SECONDS.value)
                    )
            else:
                raise InvalidImageRequestException("ImageRequest has no start time")

        except Exception as err:
            raise AggregateFeaturesException(
                "Failed to aggregate features for region region!"
            ) from err

    def fail_region_request(
        self,
        region_request_item: RegionRequestItem,
        metrics: MetricsLogger = None,
    ) -> JobItem:
        """
        Fails a region if it failed to process successfully and updates the table accordingly before
        raising an exception

        :param region_request_item:
        :param metrics: The metrics logger to use to report metrics.
        :return: None
        """
        if isinstance(metrics, MetricsLogger):
            metrics.put_metric(MetricLabels.PROCESSING_FAILURE, 1, str(Unit.COUNT.value))
            metrics.put_metric(MetricLabels.REGION_PROCESSING_ERROR, 1, str(Unit.COUNT.value))
        try:
            region_request_item = self.region_request_table.complete_region_request(
                region_request_item, error=True
            )

            return self.job_table.complete_region_request(region_request_item.image_id, error=True)
        except Exception as status_error:
            logger.error("Unable to update region status in job table")
            logger.exception(status_error)
        raise ProcessRegionException("Failed to process image region!")

    def validate_model_hosting(self, image_request: JobItem, metrics: MetricsLogger = None):
        """
        Validates that the image request is valid. If not, raises an exception.

        :param image_request:
        :param metrics: The metrics logger to use to report metrics.
        :return: None
        """
        # TODO: The long term goal is to support AWS provided models hosted by this service as well
        #       as customer provided models where we're managing the endpoints internally. For an
        #       initial release we can limit processing to customer managed SageMaker Model
        #       Endpoints hence this check. The other type options should not be advertised in the
        #       API but we are including the name/type structure in the API to allow expansion
        #       through a non-breaking API change.
        if (
            not image_request.model_hosting_type
            or image_request.model_hosting_type is ModelHostingOptions.NONE
            or image_request.model_hosting_type.casefold() != "SM_ENDPOINT".casefold()
        ):
            error = "Application only supports SageMaker Model Endpoints"
            self.status_monitor.process_event(
                image_request,
                ImageRequestStatus.FAILED,
                "Application only supports SageMaker Model Endpoints",
            )
            if isinstance(metrics, MetricsLogger):
                metrics.put_metric(MetricLabels.UNSUPPORTED_MODEL_HOST, 1, str(Unit.COUNT.value))
                metrics.put_metric(MetricLabels.IMAGE_PROCESSING_ERROR, 1, str(Unit.COUNT.value))
            raise UnsupportedModelException(error)

    @staticmethod
    def aggregate_features(
        image_request_item: JobItem,
        feature_table: FeatureTable,
    ) -> List[geojson.Feature]:
        """
        For a given image processing job - aggregate all the features that were collected for it and
        put them in the correct output sync locations.
         :param feature_table: the table storing features from all completed regions
         :param image_request_item: the image request
        """
        # Ensure we are given a validate tile size and overlap
        if image_request_item.tile_size and image_request_item.tile_overlap:
            # Read all the features from DDB. The feature table handles removing duplicates
            features = feature_table.get_features(image_request_item.image_id)
        else:
            raise AggregateFeaturesException(
                "Tile size and overlap must be provided for feature aggregation"
            )

        return features

    @staticmethod
    def sync_features(image_request_item: JobItem, features: List[Feature]):
        # Ensure we hae outputs defined for where to dump our features
        if image_request_item.outputs:
            logging.info("Writing aggregate feature for job '{}'".format(image_request_item.job_id))
            for sink in ImageRequest.outputs_to_sinks(json.loads(image_request_item.outputs)):
                if sink.mode == SinkMode.AGGREGATE and image_request_item.job_id:
                    sink.write(image_request_item.job_id, features)
        else:
            raise InvalidImageRequestException(
                "No output destinations were defined for this image request!"
            )

    def add_properties_to_features(
        self, image_request_item: JobItem, features: List[geojson.Feature]
    ) -> List[geojson.Feature]:
        """
        Add arbitrary and controlled property dictionaries to geojson feature properties

        :param image_request_item: The job table item for an image request
        :param features: The list of features to update
        :return: Updated list of features
        """
        try:
            feature_properties: List[dict] = json.loads(image_request_item.feature_properties)
            for feature in features:

                # Update the features with their inference metadata
                feature["properties"].update(
                    self.get_inference_metadata_property(
                        image_request_item, feature["properties"]["inferenceTime"]
                    )
                )

                # For the custom provided feature properties, update
                for feature_property in feature_properties:
                    feature["properties"].update(feature_property)

                # Remove unneeded feature properties if they are present
                if feature["properties"].get("inferenceTime"):
                    del feature["properties"]["inferenceTime"]
                if feature["properties"].get("bounds_imcoords"):
                    del feature["properties"]["bounds_imcoords"]
                if feature["properties"].get("detection_score"):
                    del feature["properties"]["detection_score"]
                if feature["properties"].get("feature_types"):
                    del feature["properties"]["feature_types"]
                if feature["properties"].get("image_id"):
                    del feature["properties"]["image_id"]

        except Exception as e:
            logging.exception(e)
            raise InvalidFeaturePropertiesException(
                "Could not apply custom properties to features!"
            )
        return features

    @staticmethod
    def get_inference_metadata_property(image_request_item: JobItem, inference_time: str) -> dict:
        """
        Create an inference dictionary property to append to geojson features

        :param image_request_item: The job table item for an image request
        :param inference_time: The time the inference was made in epoch millisec
        :return: An inference metadata dictionary property to attach to features
        """
        seconds = float(image_request_item.start_time) / 1000.0
        receive_time = datetime.fromtimestamp(seconds).isoformat()
        inference_metadata_property = {
            "inferenceMetadata": {
                "jobId": image_request_item.job_id,
                "filePath": image_request_item.image_url,
                "receiveTime": receive_time,
                "inferenceTime": inference_time,
            }
        }
        return inference_metadata_property
