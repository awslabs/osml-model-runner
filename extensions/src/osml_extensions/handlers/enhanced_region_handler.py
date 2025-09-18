#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
import traceback
from typing import Optional, Type

from aws_embedded_metrics.logger.metrics_logger import MetricsLogger
from aws_embedded_metrics.metric_scope import metric_scope
from aws_embedded_metrics.unit import Unit
from osgeo import gdal

from aws.osml.model_runner.api import RegionRequest
from aws.osml.model_runner.app_config import ServiceConfig
from aws.osml.model_runner.common import EndpointUtils
from aws.osml.model_runner.database import EndpointStatisticsTable, JobItem, JobTable, RegionRequestItem, RegionRequestTable
from aws.osml.model_runner.inference import FeatureDetectorFactory
from aws.osml.model_runner.region_request_handler import RegionRequestHandler
from aws.osml.model_runner.status import RegionStatusMonitor
from aws.osml.model_runner.tile_worker import TileWorker, TilingStrategy
from aws.osml.photogrammetry import SensorModel

from osml_extensions import EnhancedServiceConfig
from osml_extensions.errors import ExtensionRuntimeError
from osml_extensions.workers import setup_enhanced_tile_workers

logger = logging.getLogger(__name__)


class EnhancedRegionRequestHandler(RegionRequestHandler):
    """
    Enhanced region request handler with additional monitoring and processing capabilities.
    
    This class maintains full compatibility with the base RegionRequestHandler while adding
    enhanced features for improved performance and monitoring.
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
        factory: Optional[FeatureDetectorFactory] = None,
        tile_worker_class: Optional[Type[TileWorker]] = None,
    ) -> None:
        """
        Initialize the EnhancedRegionRequestHandler with enhanced capabilities.

        :param region_request_table: The table that handles region requests.
        :param job_table: The job table for image/region processing.
        :param region_status_monitor: A monitor to track region request status.
        :param endpoint_statistics_table: Table for tracking endpoint statistics.
        :param tiling_strategy: The strategy for handling image tiling.
        :param endpoint_utils: Utility class for handling endpoint-related operations.
        :param config: Configuration settings for the service.
        :param factory: Optional custom factory for creating detectors.
        :param tile_worker_class: Optional custom tile worker class.
        """
        super().__init__(
            region_request_table,
            job_table,
            region_status_monitor,
            endpoint_statistics_table,
            tiling_strategy,
            endpoint_utils,
            config
        )
        
        self.factory = factory
        self.tile_worker_class = tile_worker_class
        self.enhanced_processing_enabled = self._should_enable_enhanced_processing()
        
        logger.info(f"EnhancedRegionRequestHandler initialized with enhanced_processing: {self.enhanced_processing_enabled}")

    def _should_enable_enhanced_processing(self) -> bool:
        """
        Determine if enhanced processing should be enabled.
        
        :return: True if enhanced processing should be enabled
        """
        # Use EnhancedServiceConfig if available, otherwise use base config
        if isinstance(self.config, EnhancedServiceConfig):
            return self.config.enhanced_monitoring_enabled
        else:
            # Fallback for base ServiceConfig
            return True

    def _enhance_region_processing(self, region_request: RegionRequest) -> RegionRequest:
        """
        Enhance region request processing with additional metadata and validation.
        
        This method can be overridden by subclasses to add custom preprocessing logic.
        
        :param region_request: The region request to enhance
        :return: Enhanced region request
        """
        if not self.enhanced_processing_enabled:
            return region_request
            
        try:
            # For now, just return the region request as-is
            # Future enhancements could include request validation, transformation, etc.
            logger.debug(f"Enhancing region request processing for region: {region_request.region_id}")
            return region_request
        except Exception as e:
            logger.warning(f"Region request enhancement failed: {e}, using original request")
            logger.debug(f"Traceback: {traceback.format_exc()}")
            return region_request

    def _add_enhanced_monitoring(self, metrics: MetricsLogger) -> None:
        """
        Add enhanced monitoring metrics for detailed region processing tracking.
        
        :param metrics: The metrics logger instance
        """
        if not self.enhanced_processing_enabled or not isinstance(metrics, MetricsLogger):
            return
            
        try:
            metrics.put_metric("EnhancedRegionRequestHandler.Invocations", 1, str(Unit.COUNT.value))
            metrics.put_metric("EnhancedRegionRequestHandler.EnhancedProcessing", 1, str(Unit.COUNT.value))
            
            # Add custom dimensions for enhanced tracking
            metrics.put_dimensions({
                "HandlerType": "EnhancedRegionRequestHandler",
                "EnhancedProcessing": "Enabled"
            })
        except Exception as e:
            logger.warning(f"Failed to add enhanced monitoring metrics: {e}")
            logger.debug(f"Traceback: {traceback.format_exc()}")

    def _setup_enhanced_tile_workers(
        self,
        region_request: RegionRequest,
        sensor_model: Optional[SensorModel] = None,
        elevation_model = None,
    ):
        """
        Set up enhanced tile workers with custom factory and worker class.
        
        :param region_request: The region request being processed
        :param sensor_model: Optional sensor model for geolocating features
        :param elevation_model: Optional elevation model
        :return: Tuple of (tile_queue, tile_workers)
        """
        try:
            return setup_enhanced_tile_workers(
                region_request=region_request,
                sensor_model=sensor_model,
                elevation_model=elevation_model,
                factory=self.factory,
                worker_class=self.tile_worker_class
            )
        except Exception as e:
            logger.error(f"Failed to setup enhanced tile workers: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            # Fallback to base implementation
            from aws.osml.model_runner.tile_worker.tile_worker_utils import setup_tile_workers
            return setup_tile_workers(region_request, sensor_model, elevation_model)

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
        Enhanced region request processing with preprocessing hooks and enhanced monitoring.

        This method extends the base implementation while maintaining full compatibility.

        :param region_request: RegionRequest = the region request
        :param region_request_item: RegionRequestItem = the region request to update
        :param raster_dataset: gdal.Dataset = the raster dataset containing the region
        :param sensor_model: Optional[SensorModel] = the sensor model for this raster dataset
        :param metrics: MetricsLogger = the metrics logger to use to report metrics.

        :return: JobItem
        """
        logger.debug(f"EnhancedRegionRequestHandler processing region: {region_request.region_id}")

        try:
            # Add enhanced monitoring metrics
            if isinstance(metrics, MetricsLogger):
                self._add_enhanced_monitoring(metrics)

            # Enhance region request processing
            enhanced_region_request = self._enhance_region_processing(region_request)

            # Validate the enhanced region request
            if not enhanced_region_request.is_valid():
                logger.error(f"Invalid Enhanced Region Request! {enhanced_region_request.__dict__}")
                raise ValueError("Invalid Enhanced Region Request")

            # Set up enhanced dimensions for metrics
            if isinstance(metrics, MetricsLogger):
                image_format = str(raster_dataset.GetDriver().ShortName).upper()
                metrics.put_dimensions({
                    "Operation": "EnhancedRegionProcessing",
                    "ModelName": enhanced_region_request.model_name,
                    "InputFormat": image_format,
                    "HandlerType": "EnhancedRegionRequestHandler"
                })

            # Handle self-throttling with enhanced monitoring
            if self.config.self_throttling:
                max_regions = self.endpoint_utils.calculate_max_regions(
                    enhanced_region_request.model_name, enhanced_region_request.model_invocation_role
                )
                self.endpoint_statistics_table.upsert_endpoint(enhanced_region_request.model_name, max_regions)
                in_progress = self.endpoint_statistics_table.current_in_progress_regions(enhanced_region_request.model_name)

                if in_progress >= max_regions:
                    if isinstance(metrics, MetricsLogger):
                        metrics.put_metric("EnhancedRegionRequestHandler.Throttles", 1, str(Unit.COUNT.value))
                    logger.warning(f"Enhanced handler throttling region request. (Max: {max_regions} In-progress: {in_progress})")
                    from aws.osml.model_runner.exceptions import SelfThrottledRegionException
                    raise SelfThrottledRegionException

                self.endpoint_statistics_table.increment_region_count(enhanced_region_request.model_name)

            try:
                # Start region request processing
                self.region_request_table.start_region_request(region_request_item)
                logger.debug(f"Enhanced handler starting region request: region id: {region_request_item.region_id}")

                # Set up enhanced tile workers
                tile_queue, tile_workers = self._setup_enhanced_tile_workers(
                    enhanced_region_request, sensor_model, self.config.elevation_model
                )

                # Process tiles using enhanced workers
                from aws.osml.model_runner.tile_worker.tile_worker_utils import process_tiles
                total_tile_count, failed_tile_count = process_tiles(
                    self.tiling_strategy,
                    region_request_item,
                    tile_queue,
                    tile_workers,
                    raster_dataset,
                    sensor_model,
                )

                # Update table with tile counts
                region_request_item.total_tiles = total_tile_count
                region_request_item.succeeded_tile_count = total_tile_count - failed_tile_count
                region_request_item.failed_tile_count = failed_tile_count
                region_request_item = self.region_request_table.update_region_request(region_request_item)

                # Complete region processing
                image_request_item = self.job_table.complete_region_request(
                    enhanced_region_request.image_id, bool(failed_tile_count)
                )

                # Update region status
                region_status = self.region_status_monitor.get_status(region_request_item)
                region_request_item = self.region_request_table.complete_region_request(region_request_item, region_status)
                self.region_status_monitor.process_event(
                    region_request_item, region_status, "Enhanced handler completed region processing"
                )

                # Add enhanced completion metrics
                if isinstance(metrics, MetricsLogger):
                    metrics.put_metric("EnhancedRegionRequestHandler.Completions", 1, str(Unit.COUNT.value))
                    metrics.put_metric("EnhancedRegionRequestHandler.TilesProcessed", total_tile_count, str(Unit.COUNT.value))
                    if failed_tile_count > 0:
                        metrics.put_metric("EnhancedRegionRequestHandler.TileFailures", failed_tile_count, str(Unit.COUNT.value))

                logger.info(f"Enhanced handler completed region processing: {region_request.region_id}")
                return image_request_item

            except Exception as err:
                failed_msg = f"Enhanced handler failed to process image region: {err}"
                logger.error(failed_msg)
                logger.error(f"Traceback: {traceback.format_exc()}")
                region_request_item.message = failed_msg
                
                # Add enhanced error metrics
                if isinstance(metrics, MetricsLogger):
                    metrics.put_metric("EnhancedRegionRequestHandler.ProcessingErrors", 1, str(Unit.COUNT.value))
                
                return self.fail_region_request(region_request_item)

            finally:
                # Decrement the endpoint region counter
                if self.config.self_throttling:
                    self.endpoint_statistics_table.decrement_region_count(enhanced_region_request.model_name)

        except Exception as e:
            logger.error(f"EnhancedRegionRequestHandler error: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            if isinstance(metrics, MetricsLogger):
                metrics.put_metric("EnhancedRegionRequestHandler.Errors", 1, str(Unit.COUNT.value))
            
            # Check if we should fallback or re-raise
            # Check if we should fallback or re-raise
            if isinstance(self.config, EnhancedServiceConfig) and not self.config.extension_fallback_enabled:
                raise ExtensionRuntimeError(f"Enhanced region processing failed: {e}") from e
                
            # Re-raise the original exception for fallback handling
            raise