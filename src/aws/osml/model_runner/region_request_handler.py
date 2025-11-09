#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
from typing import Optional, Tuple

from aws_embedded_metrics.logger.metrics_logger import MetricsLogger
from aws_embedded_metrics.metric_scope import metric_scope
from aws_embedded_metrics.unit import Unit
from osgeo import gdal

from aws.osml.photogrammetry import SensorModel

from .api import ModelInvokeMode, RegionRequest
from .api import ModelInvokeMode, RegionRequest
from .app_config import MetricLabels, ServiceConfig
from .common import EndpointUtils, RequestStatus, Timer
from .database import EndpointStatisticsTable, JobItem, JobTable, RegionRequestItem, RegionRequestTable, TileRequestTable
from .exceptions import ProcessRegionException, SelfThrottledRegionException
from .queue import RequestQueue
from .status import RegionStatusMonitor
from .tile_worker import BatchTileProcessor, AsyncTileProcessor, TileProcessor, TilingStrategy, setup_submission_tile_workers, setup_tile_workers, setup_upload_tile_workers, setup_batch_submission_worker
from .utilities import S3Manager

# Set up logging configuration
logger = logging.getLogger(__name__)

S3_MANAGER = S3Manager()

class RegionRequestHandler:
    """
    Class responsible for handling RegionRequest processing.
    """

    def __init__(
        self,
        region_request_table: RegionRequestTable,
        job_table: JobTable,
        region_status_monitor: RegionStatusMonitor,
        endpoint_statistics_table: EndpointStatisticsTable,
        tiling_strategy: TilingStrategy,
        endpoint_utils: EndpointUtils,
        config: ServiceConfig,
        tile_request_table: Optional[TileRequestTable] = None,
        tile_request_queue: Optional[RequestQueue] = None,
    ) -> None:
        """
        Initialize the RegionRequestHandler with the necessary dependencies.

        :param region_request_table: The table that handles region requests.
        :param job_table: The job table for image/region processing.
        :param region_status_monitor: A monitor to track region request status.
        :param endpoint_statistics_table: Table for tracking endpoint statistics.
        :param tiling_strategy: The strategy for handling image tiling.
        :param endpoint_utils: Utility class for handling endpoint-related operations.
        :param config: Configuration settings for the service.
        """
        self.region_request_table = region_request_table
        self.job_table = job_table
        self.region_status_monitor = region_status_monitor
        self.endpoint_statistics_table = endpoint_statistics_table
        self.tiling_strategy = tiling_strategy
        self.endpoint_utils = endpoint_utils
        self.config = config

        self.tile_request_queue = tile_request_queue
        self.tile_request_table = tile_request_table

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

        :param region_request: RegionRequest = the region request
        :param region_request_item: RegionRequestItem = the region request to update
        :param raster_dataset: gdal.Dataset = the raster dataset containing the region
        :param sensor_model: Optional[SensorModel] = the sensor model for this raster dataset
        :param metrics: MetricsLogger = the metrics logger to use to report metrics.

        :return: JobItem
        """

        if region_request.model_invoke_mode == ModelInvokeMode.SM_ENDPOINT_ASYNC:
            return self.process_region_request_async(
                region_request=region_request,
                region_request_item=region_request_item,
                raster_dataset=raster_dataset,
                sensor_model=sensor_model,
                metrics=metrics,
            )
        elif region_request.model_invoke_mode == ModelInvokeMode.SM_BATCH:
            raise NotImplementedError("Batch processing moved to image level")
            # return self.process_region_request_batch(
            #     region_request=region_request,
            #     region_request_item=region_request_item,
            #     raster_dataset=raster_dataset,
            #     sensor_model=sensor_model,
            #     metrics=metrics,
            # )
        else:
            return self.process_region_request_realtime(
                region_request=region_request,
                region_request_item=region_request_item,
                raster_dataset=raster_dataset,
                sensor_model=sensor_model,
                metrics=metrics,
            )

    @metric_scope
    def fail_region_request(
        self,
        region_request_item: RegionRequestItem,
        metrics: MetricsLogger = None,
    ) -> JobItem:
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
            return self.job_table.complete_region_request(region_request_item.image_id, error=True)
        except Exception as status_error:
            logger.error("Unable to update region status in job table")
            logger.exception(status_error)
            raise ProcessRegionException("Failed to process image region!")

    def process_region_request_realtime(
        self,
        region_request: RegionRequest,
        region_request_item: RegionRequestItem,
        raster_dataset: gdal.Dataset,
        sensor_model: Optional[SensorModel] = None,
        metrics: MetricsLogger = None,
    ) -> JobItem:
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

        if self.config.self_throttling:
            max_regions = self.endpoint_utils.calculate_max_regions(
                region_request.model_name, region_request.model_invocation_role
            )
            # Add entry to the endpoint statistics table
            self.endpoint_statistics_table.upsert_endpoint(region_request.model_name, max_regions)
            in_progress = self.endpoint_statistics_table.current_in_progress_regions(region_request.model_name)

            if in_progress >= max_regions:
                if isinstance(metrics, MetricsLogger):
                    metrics.put_metric(MetricLabels.THROTTLES, 1, str(Unit.COUNT.value))
                logger.warning(f"Throttling region request. (Max: {max_regions} In-progress: {in_progress}")
                raise SelfThrottledRegionException

            # Increment the endpoint region counter
            self.endpoint_statistics_table.increment_region_count(region_request.model_name)

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
                total_tile_count, failed_tile_count = TileProcessor().process_tiles(
                    self.tiling_strategy,
                    region_request,
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
            image_request_item = self.job_table.complete_region_request(region_request.image_id, bool(failed_tile_count))

            # Update region request table if that region succeeded
            region_status = self.region_status_monitor.get_status(region_request_item)
            region_request_item = self.region_request_table.complete_region_request(region_request_item, region_status)

            self.region_status_monitor.process_event(region_request_item, region_status, "Completed region processing")

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
            return self.fail_region_request(region_request_item)

        finally:
            # Decrement the endpoint region counter
            if self.config.self_throttling:
                self.endpoint_statistics_table.decrement_region_count(region_request.model_name)


    def process_region_request_async(
        self,
        region_request: RegionRequest,
        region_request_item: RegionRequestItem,
        raster_dataset: gdal.Dataset,
        sensor_model: Optional[SensorModel] = None,
        metrics: MetricsLogger = None,
    ) -> JobItem:
        """
        Enhanced region request processing with preprocessing hooks and enhanced monitoring.

        This method extends the base implementation while maintaining full compatibility.

        :param region_request: RegionRequest = the region request
        :param region_request_item: RegionRequestItem = the region request to update
        :param raster_dataset: gdal.Dataset = the raster dataset containing the region
        :param sensor_model: Optional[SensorModel] = the sensor model for this raster dataset
        :param metrics: MetricsLogger = the metrics logger to use to report metrics.

        :return: JobItem
        """
        logger.info(f"Async processing region: {region_request.region_id}")

        try:
            # Validate the enhanced region request
            if not region_request.is_valid():
                logger.error(f"Invalid Enhanced Region Request! {region_request.__dict__}")
                raise ValueError("Invalid Enhanced Region Request")

            # Set up enhanced dimensions for metrics
            if isinstance(metrics, MetricsLogger):
                image_format = str(raster_dataset.GetDriver().ShortName).upper()
                metrics.put_dimensions(
                    {
                        "Operation": "EnhancedRegionProcessing",
                        "ModelName": region_request.model_name,
                        "InputFormat": image_format,
                        "HandlerType": "RegionRequestHandler",
                    }
                )

            # Handle self-throttling with enhanced monitoring
            if ServiceConfig.self_throttling:
                max_regions = self.endpoint_utils.calculate_max_regions(
                    region_request.model_name, region_request.model_invocation_role
                )
                self.endpoint_statistics_table.upsert_endpoint(region_request.model_name, max_regions)
                in_progress = self.endpoint_statistics_table.current_in_progress_regions(region_request.model_name)

                if in_progress >= max_regions:
                    if isinstance(metrics, MetricsLogger):
                        metrics.put_metric(MetricLabels.THROTTLES, 1, str(Unit.COUNT.value))
                    logger.warning(f"Throttling region request. (Max: {max_regions} In-progress: {in_progress})")
                    raise SelfThrottledRegionException

                self.endpoint_statistics_table.increment_region_count(region_request.model_name)

            try:
                # Start region request processing
                self.region_request_table.start_region_request(region_request_item)
                logger.debug(f"Enhanced handler starting region request: region id: {region_request_item.region_id}")

                # Set up our threaded tile worker pool
                tile_queue, tile_workers = setup_submission_tile_workers(
                    region_request, sensor_model, ServiceConfig.elevation_model
                )

                # Process tiles using appropriate method
                total_tile_count, failed_tile_count = AsyncTileProcessor(self.tile_request_table).process_tiles(
                    self.tiling_strategy,
                    region_request,
                    region_request_item,
                    tile_queue,
                    tile_workers,
                    raster_dataset,
                    sensor_model,
                )

                # update the expected number of tiles
                region_request_item.total_tiles = total_tile_count
                region_request_item = self.region_request_table.update_region_request(region_request_item)
                image_request_item = self.job_table.get_image_request(region_request.image_id)

                return image_request_item

            except Exception as err:
                failed_msg = f"Enhanced handler failed to process image region: {err}"
                logger.error(failed_msg, exc_info=True)
                region_request_item.message = failed_msg

                # Add enhanced error metrics
                if isinstance(metrics, MetricsLogger):
                    metrics.put_metric(MetricLabels.ERRORS, 1, str(Unit.COUNT.value))

                return self.fail_region_request(region_request_item)

            finally:
                # Decrement the endpoint region counter
                if ServiceConfig.self_throttling:
                    self.endpoint_statistics_table.decrement_region_count(region_request.model_name)

        except Exception as e:
            failed_msg = f"RegionRequestHandler error: {e}"
            logger.error(failed_msg, exc_info=True)
            if isinstance(metrics, MetricsLogger):
                metrics.put_metric(MetricLabels.ERRORS, 1, str(Unit.COUNT.value))

            region_request_item.message = failed_msg
            return self.fail_region_request(region_request_item)

    # def process_region_request_batch(
    #     self,
    #     region_request: RegionRequest,
    #     region_request_item: RegionRequestItem,
    #     raster_dataset: gdal.Dataset,
    #     sensor_model: Optional[SensorModel] = None,
    #     metrics: MetricsLogger = None,
    # ) -> JobItem:
    #     """
    #     Enhanced region request processing with preprocessing hooks and enhanced monitoring.

    #     This method extends the base implementation while maintaining full compatibility.

    #     :param region_request: RegionRequest = the region request
    #     :param region_request_item: RegionRequestItem = the region request to update
    #     :param raster_dataset: gdal.Dataset = the raster dataset containing the region
    #     :param sensor_model: Optional[SensorModel] = the sensor model for this raster dataset
    #     :param metrics: MetricsLogger = the metrics logger to use to report metrics.

    #     :return: JobItem
    #     """
    #     logger.info(f"Batch processing region: {region_request.region_id}")

    #     try:
    #         # Validate the enhanced region request
    #         if not region_request.is_valid():
    #             logger.error(f"Invalid Enhanced Region Request! {region_request.__dict__}")
    #             raise ValueError("Invalid Enhanced Region Request")

    #         # Set up enhanced dimensions for metrics
    #         if isinstance(metrics, MetricsLogger):
    #             image_format = str(raster_dataset.GetDriver().ShortName).upper()
    #             metrics.put_dimensions(
    #                 {
    #                     "Operation": "EnhancedRegionProcessing",
    #                     "ModelName": region_request.model_name,
    #                     "InputFormat": image_format,
    #                     "HandlerType": "RegionRequestHandler",
    #                 }
    #             )

    #         # Handle self-throttling with enhanced monitoring
    #         if ServiceConfig.self_throttling:
    #             max_regions = self.endpoint_utils.calculate_max_regions(
    #                 region_request.model_name, region_request.model_invocation_role
    #             )
    #             self.endpoint_statistics_table.upsert_endpoint(region_request.model_name, max_regions)
    #             in_progress = self.endpoint_statistics_table.current_in_progress_regions(region_request.model_name)

    #             if in_progress >= max_regions:
    #                 if isinstance(metrics, MetricsLogger):
    #                     metrics.put_metric(MetricLabels.THROTTLES, 1, str(Unit.COUNT.value))
    #                 logger.warning(f"Throttling region request. (Max: {max_regions} In-progress: {in_progress})")
    #                 raise SelfThrottledRegionException

    #             self.endpoint_statistics_table.increment_region_count(region_request.model_name)

    #         try:
    #             # Start region request processing
    #             self.region_request_table.start_region_request(region_request_item)
    #             logger.debug(f"Enhanced handler starting region request: region id: {region_request_item.region_id}")

    #             # Set up our threaded tile worker pool
    #             tile_queue, tile_workers = setup_upload_tile_workers(
    #                 region_request, sensor_model, ServiceConfig.elevation_model
    #             )

    #             # Upload tiles to S3
    #             total_tile_count, failed_tile_count = BatchTileProcessor(self.tile_request_table).process_tiles(
    #                 self.tiling_strategy,
    #                 region_request,
    #                 region_request_item,
    #                 tile_queue,
    #                 tile_workers,
    #                 raster_dataset,
    #                 sensor_model,
    #             )

    #             # update the expected number of tiles
    #             region_request_item.total_tiles = total_tile_count
    #             region_request_item = self.region_request_table.update_region_request(region_request_item)
    #             image_request_item = self.job_table.get_image_request(region_request.image_id)

    #             # submit to Batch processing
    #             in_queue, worker = setup_batch_submission_worker(region_request)

    #             # Place the image info onto our processing queue
    #             image_info = dict(
    #                 job_id=region_request.job_id,
    #                 instance_type=region_request.instance_type,
    #                 instance_count=region_request.instance_count
    #             )
    #             in_queue.put(image_info)
    #             in_queue.put(None)
    #             worker.join()

    #             return image_request_item

    #         except Exception as err:
    #             failed_msg = f"Enhanced handler failed to process image region: {err}"
    #             logger.error(failed_msg, exc_info=True)
    #             region_request_item.message = failed_msg

    #             # Add enhanced error metrics
    #             if isinstance(metrics, MetricsLogger):
    #                 metrics.put_metric(MetricLabels.ERRORS, 1, str(Unit.COUNT.value))

    #             return self.fail_region_request(region_request_item)

    #         finally:
    #             # Decrement the endpoint region counter
    #             if ServiceConfig.self_throttling:
    #                 self.endpoint_statistics_table.decrement_region_count(region_request.model_name)

    #     except Exception as e:
    #         failed_msg = f"RegionRequestHandler error: {e}"
    #         logger.error(failed_msg, exc_info=True)
    #         if isinstance(metrics, MetricsLogger):
    #             metrics.put_metric(MetricLabels.ERRORS, 1, str(Unit.COUNT.value))

    #         region_request_item.message = failed_msg
    #         return self.fail_region_request(region_request_item)
