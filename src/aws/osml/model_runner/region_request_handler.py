#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

import logging
from typing import Optional

from aws_embedded_metrics.logger.metrics_logger import MetricsLogger
from aws_embedded_metrics.metric_scope import metric_scope
from aws_embedded_metrics.unit import Unit
from osgeo import gdal

from aws.osml.photogrammetry import SensorModel

from .api import RegionRequest
from .app_config import MetricLabels, ServiceConfig
from .common import ObservableEvent, RequestStatus, Timer
from .database import ImageRequestItem, ImageRequestTable, RegionRequestItem, RegionRequestTable
from .exceptions import ProcessRegionException
from .status import RegionStatusMonitor
from .tile_worker import TilingStrategy, process_tiles, setup_tile_workers

# Set up logging configuration
logger = logging.getLogger(__name__)


class RegionRequestHandler:
    """
    Class responsible for handling RegionRequest processing.
    """

    def __init__(
        self,
        region_request_table: RegionRequestTable,
        image_request_table: ImageRequestTable,
        region_status_monitor: RegionStatusMonitor,
        tiling_strategy: TilingStrategy,
        config: ServiceConfig,
    ) -> None:
        """
        Initialize the RegionRequestHandler with the necessary dependencies.

        :param region_request_table: The table that handles region requests.
        :param image_request_table: The image request table for image/region processing.
        :param region_status_monitor: A monitor to track region request status.
        :param tiling_strategy: The strategy for handling image tiling.
        :param config: Configuration settings for the service.
        """
        self.region_request_table = region_request_table
        self.image_request_table = image_request_table
        self.region_status_monitor = region_status_monitor
        self.tiling_strategy = tiling_strategy
        self.config = config
        self.on_region_complete = ObservableEvent()

    @metric_scope
    def process_region_request(
        self,
        region_request: RegionRequest,
        region_request_item: RegionRequestItem,
        raster_dataset: gdal.Dataset,
        sensor_model: Optional[SensorModel] = None,
        metrics: MetricsLogger = None,
    ) -> ImageRequestItem:
        """
        Processes RegionRequest objects that are delegated for processing. Loads the specified region of an image into
        memory to be processed by tile-workers. If a raster_dataset is not provided directly it will poll the image
        from the region request.

        :param region_request: RegionRequest = the region request
        :param region_request_item: RegionRequestItem = the region request to update
        :param raster_dataset: gdal.Dataset = the raster dataset containing the region
        :param sensor_model: Optional[SensorModel] = the sensor model for this raster dataset
        :param metrics: MetricsLogger = the metrics logger to use to report metrics.

        :return: ImageRequestItem
        """
        logger.info(
            "Starting region processing.",
            extra={"tag": "TIMELINE EVENT", "job_id": region_request.job_id, "region_id": region_request.region_id},
        )
        if isinstance(metrics, MetricsLogger):
            metrics.set_dimensions()

        if not region_request.is_valid():
            logger.error(f"Invalid Region Request! {region_request.__dict__}")
            raise ValueError("Invalid Region Request")

        if isinstance(metrics, MetricsLogger):
            image_format = str(raster_dataset.GetDriver().ShortName).upper()
            metrics.put_dimensions(
                {
                    MetricLabels.OPERATION_DIMENSION: MetricLabels.REGION_PROCESSING_OPERATION,
                    MetricLabels.MODEL_NAME_DIMENSION: region_request.model_name,
                    MetricLabels.INPUT_FORMAT_DIMENSION: image_format,
                }
            )

        try:
            with Timer(
                task_str=f"Processing region {region_request.image_url} {region_request.region_bounds}",
                metric_name=MetricLabels.DURATION,
                logger=logger,
                metrics_logger=metrics,
            ):
                self.region_request_table.start_region_request(region_request_item)
                logger.debug(f"Starting region request: region id: {region_request_item.region_id}")

                # Set up our threaded tile worker pool
                tile_queue, tile_workers = setup_tile_workers(region_request, sensor_model, self.config.elevation_model)

                # Process all our tiles
                total_tile_count, failed_tile_count = process_tiles(
                    self.tiling_strategy,
                    region_request_item,
                    tile_queue,
                    tile_workers,
                    raster_dataset,
                    sensor_model,
                )

                # Update table w/ total tile counts
                region_request_item.total_tiles = total_tile_count
                region_request_item.succeeded_tile_count = total_tile_count - failed_tile_count
                region_request_item.failed_tile_count = failed_tile_count
                region_request_item = self.region_request_table.update_region_request(region_request_item)

            # Update the image request to complete this region
            image_request_item = self.image_request_table.complete_region_request(
                region_request.image_id, bool(failed_tile_count)
            )

            # Update region request table if that region succeeded
            region_status = self.region_status_monitor.get_status(region_request_item)
            region_request_item = self.region_request_table.complete_region_request(region_request_item, region_status)

            self.region_status_monitor.process_event(region_request_item, region_status, "Completed region processing")
            self.on_region_complete(image_request_item, region_request_item, region_status)
            logger.info(
                "Completed region processing.",
                extra={"tag": "TIMELINE EVENT", "job_id": region_request.job_id, "region_id": region_request.region_id},
            )

            # Write CloudWatch Metrics to the Logs
            if isinstance(metrics, MetricsLogger):
                metrics.put_metric(MetricLabels.INVOCATIONS, 1, str(Unit.COUNT.value))

            # Return the updated item
            return image_request_item

        except Exception as err:
            failed_msg = f"Failed to process image region: {err}"
            logger.error(failed_msg)
            # Update the table to record the failure
            region_request_item.message = failed_msg
            image_request_item = self.fail_region_request(region_request_item)
            self.on_region_complete(image_request_item, region_request_item, RequestStatus.FAILED)
            return image_request_item

    @metric_scope
    def fail_region_request(
        self,
        region_request_item: RegionRequestItem,
        metrics: MetricsLogger = None,
    ) -> ImageRequestItem:
        """
        Fails a region if it failed to process successfully and updates the table accordingly before
        raising an exception

        :param region_request_item: RegionRequestItem = the region request to update
        :param metrics: MetricsLogger = the metrics logger to use to report metrics.

        :return: None
        """
        if isinstance(metrics, MetricsLogger):
            metrics.put_metric(MetricLabels.ERRORS, 1, str(Unit.COUNT.value))
        try:
            region_status = RequestStatus.FAILED
            region_request_item = self.region_request_table.complete_region_request(region_request_item, region_status)
            self.region_status_monitor.process_event(region_request_item, region_status, "Completed region processing")
            return self.image_request_table.complete_region_request(region_request_item.image_id, error=True)
        except Exception as status_error:
            logger.error("Unable to update region status in job table")
            logger.exception(status_error)
            raise ProcessRegionException("Failed to process image region!")
