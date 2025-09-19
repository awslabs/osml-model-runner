#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
import traceback
from typing import Optional, Type, Tuple

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
from aws.osml.model_runner.tile_worker.tile_worker_utils import _create_tile
from aws.osml.photogrammetry import SensorModel

from osml_extensions import EnhancedServiceConfig
from osml_extensions.errors import ExtensionRuntimeError
from osml_extensions.registry import register_handler, HandlerType

from .workers import AsyncTileWorkerPool

logger = logging.getLogger(__name__)


@register_handler(
    request_type="async_sm_endpoint",
    handler_type=HandlerType.REGION_REQUEST_HANDLER,
    name="enhanced_region_request_handler",
    description="Enhanced region request handler with async processing capabilities"
)
class EnhancedRegionRequestHandler(RegionRequestHandler):
    """
    Enhanced region request handler with additional monitoring and processing capabilities.
    
    This class maintains full compatibility with the base RegionRequestHandler while adding
    enhanced features for improved performance and monitoring.
    """

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
            # Validate the enhanced region request
            if not region_request.is_valid():
                logger.error(f"Invalid Enhanced Region Request! {region_request.__dict__}")
                raise ValueError("Invalid Enhanced Region Request")

            # Set up enhanced dimensions for metrics
            if isinstance(metrics, MetricsLogger):
                image_format = str(raster_dataset.GetDriver().ShortName).upper()
                metrics.put_dimensions({
                    "Operation": "EnhancedRegionProcessing",
                    "ModelName": region_request.model_name,
                    "InputFormat": image_format,
                    "HandlerType": "EnhancedRegionRequestHandler"
                })

            # Handle self-throttling with enhanced monitoring
            if self.config.self_throttling:
                max_regions = self.endpoint_utils.calculate_max_regions(
                    region_request.model_name, region_request.model_invocation_role
                )
                self.endpoint_statistics_table.upsert_endpoint(region_request.model_name, max_regions)
                in_progress = self.endpoint_statistics_table.current_in_progress_regions(region_request.model_name)

                if in_progress >= max_regions:
                    if isinstance(metrics, MetricsLogger):
                        metrics.put_metric("EnhancedRegionRequestHandler.Throttles", 1, str(Unit.COUNT.value))
                    logger.warning(f"Enhanced handler throttling region request. (Max: {max_regions} In-progress: {in_progress})")
                    from aws.osml.model_runner.exceptions import SelfThrottledRegionException
                    raise SelfThrottledRegionException

                self.endpoint_statistics_table.increment_region_count(region_request.model_name)

            try:
                # Start region request processing
                self.region_request_table.start_region_request(region_request_item)
                logger.debug(f"Enhanced handler starting region request: region id: {region_request_item.region_id}")

                # Set up async worker pool
                worker_setup = self._setup_async_worker_pool(region_request, sensor_model, self.config.elevation_model)

                # Process tiles using appropriate method
                total_tile_count, failed_tile_count = self._process_tiles_with_async_pool(
                    worker_setup,
                    self.tiling_strategy,
                    region_request_item,
                    raster_dataset,
                    sensor_model,
                    metrics
                )

                # Update table with tile counts
                region_request_item.total_tiles = total_tile_count
                region_request_item.succeeded_tile_count = total_tile_count - failed_tile_count
                region_request_item.failed_tile_count = failed_tile_count
                region_request_item = self.region_request_table.update_region_request(region_request_item)

                # Complete region processing
                image_request_item = self.job_table.complete_region_request(
                    region_request.image_id, bool(failed_tile_count)
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
                    
                    # Add async-specific metrics if async pool was used
                    if isinstance(worker_setup, AsyncTileWorkerPool):
                        worker_stats = worker_setup.get_worker_stats()
                        metrics.put_metric("AsyncWorkerPool.SubmissionWorkers", worker_stats["submission_workers"]["workers"], str(Unit.COUNT.value))
                        metrics.put_metric("AsyncWorkerPool.PollingWorkers", worker_stats["polling_workers"]["workers"], str(Unit.COUNT.value))
                        metrics.put_metric("AsyncWorkerPool.JobsCompleted", worker_stats["polling_workers"]["total_completed"], str(Unit.COUNT.value))

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
                    self.endpoint_statistics_table.decrement_region_count(region_request.model_name)

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
    
    def _process_tiles_with_async_pool(
        self,
        async_pool: AsyncTileWorkerPool,
        tiling_strategy: TilingStrategy,
        region_request_item: RegionRequestItem,
        raster_dataset,
        sensor_model: Optional[SensorModel],
        metrics: Optional[MetricsLogger] = None
    ) -> Tuple[int, int]:
        """
        Process tiles using the async worker pool optimization.
        
        :param async_pool: AsyncTileWorkerPool instance
        :param tiling_strategy: The tiling strategy to use
        :param region_request_item: The region request item
        :param raster_dataset: The raster dataset
        :param sensor_model: Optional sensor model
        :param metrics: Optional metrics logger
        :return: Tuple of (total_tile_count, failed_tile_count)
        """
        logger.debug("Processing tiles with async worker pool optimization")
        
        try:
            # Import required modules
            from queue import Queue
            from pathlib import Path
            from secrets import token_hex
            import tempfile
            from aws.osml.gdal import GDALConfigEnv
            from aws.osml.image_processing.gdal_tile_factory import GDALTileFactory
            from aws.osml.model_runner.common import get_credentials_for_assumed_role
            
            # Calculate tile bounds
            region_bounds = (
                (region_request_item.region_bounds[0][0], region_request_item.region_bounds[0][1]),
                (region_request_item.region_bounds[1][0], region_request_item.region_bounds[1][1]),
            )
            tile_size = (region_request_item.tile_size[0], region_request_item.tile_size[1])
            tile_overlap = (region_request_item.tile_overlap[0], region_request_item.tile_overlap[1])
            
            tile_array = tiling_strategy.compute_tiles(region_bounds, tile_size, tile_overlap)
            
            # Filter out already processed tiles
            if region_request_item.succeeded_tiles is not None:
                filtered_regions = [
                    region
                    for region in tile_array
                    if [[region[0][0], region[0][1]], [region[1][0], region[1][1]]] not in region_request_item.succeeded_tiles
                ]
                tile_array = filtered_regions
            
            total_tile_count = len(tile_array)
            
            if total_tile_count == 0:
                logger.debug("No tiles to process")
                return 0, 0
            
            # Set up credentials for image reading
            image_read_credentials = None
            if region_request_item.image_read_role:
                image_read_credentials = get_credentials_for_assumed_role(region_request_item.image_read_role)
            
            # Create tile queue
            tile_queue = Queue()
            
            with GDALConfigEnv().with_aws_credentials(image_read_credentials):
                # Create GDAL tile factory
                gdal_tile_factory = GDALTileFactory(
                    raster_dataset=raster_dataset,
                    tile_format=region_request_item.tile_format,
                    tile_compression=region_request_item.tile_compression,
                    sensor_model=sensor_model,
                )
                
                # Create tiles and add to queue
                with tempfile.TemporaryDirectory() as tmp:
                    for tile_bounds in tile_array:
                        # Create temp file name
                        region_image_filename = (
                            f"{token_hex(16)}-region-{tile_bounds[0][0]}-{tile_bounds[0][1]}-"
                            f"{tile_bounds[1][0]}-{tile_bounds[1][1]}.{region_request_item.tile_format}"
                        )
                        
                        tmp_image_path = Path(tmp, region_image_filename)
                        
                        # Create tile
                        absolute_tile_path = _create_tile(gdal_tile_factory, tile_bounds, tmp_image_path, metrics)
                        
                        if not absolute_tile_path:
                            continue
                        
                        # Create image info
                        image_info = {
                            "image_path": tmp_image_path,
                            "region": tile_bounds,
                            "image_id": region_request_item.image_id,
                            "job_id": region_request_item.job_id,
                            "region_id": region_request_item.region_id,
                        }
                        
                        tile_queue.put(image_info)
                    
                    # Add shutdown signals for workers
                    for _ in range(async_pool.config.submission_workers):
                        tile_queue.put(None)
                    
                    # Process tiles with async pool
                    processed_count, failed_count = async_pool.process_tiles_async(tile_queue)
                    
                    # Log worker pool statistics
                    worker_stats = async_pool.get_worker_stats()
                    logger.info(f"Async worker pool stats: {worker_stats}")
                    
                    # Add async-specific metrics
                    if isinstance(metrics, MetricsLogger):
                        metrics.put_metric("AsyncWorkerPool.TotalProcessed", processed_count, str(Unit.COUNT.value))
                        metrics.put_metric("AsyncWorkerPool.TotalFailed", failed_count, str(Unit.COUNT.value))
                        
                        # Add worker utilization metrics
                        submission_stats = worker_stats["submission_workers"]
                        polling_stats = worker_stats["polling_workers"]
                        
                        if submission_stats["workers"] > 0:
                            avg_tiles_per_worker = submission_stats["total_processed"] / submission_stats["workers"]
                            metrics.put_metric("AsyncWorkerPool.AvgTilesPerSubmissionWorker", avg_tiles_per_worker, str(Unit.COUNT.value))
                        
                        if polling_stats["workers"] > 0:
                            avg_jobs_per_worker = polling_stats["total_completed"] / polling_stats["workers"]
                            metrics.put_metric("AsyncWorkerPool.AvgJobsPerPollingWorker", avg_jobs_per_worker, str(Unit.COUNT.value))
            
            logger.info(f"Async worker pool processed {processed_count} tiles, {failed_count} failed")
            return processed_count, failed_count
            
        except Exception as e:
            logger.error(f"Error processing tiles with async worker pool: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            if isinstance(metrics, MetricsLogger):
                metrics.put_metric("AsyncWorkerPool.ProcessingErrors", 1, str(Unit.COUNT.value))
            
            raise

    def _setup_async_worker_pool(
        self,
        region_request: RegionRequest,
        sensor_model: Optional[SensorModel] = None,
        elevation_model = None,
    ) -> AsyncTileWorkerPool:
        """
        Set up async worker pool for optimized async endpoint processing.
        
        :param region_request: The region request being processed
        :param sensor_model: Optional sensor model for geolocating features
        :param elevation_model: Optional elevation model
        :return: AsyncTileWorkerPool instance
        """
        logger.debug("Setting up async worker pool for optimized processing")
        
        try:
            # Get model invocation credentials
            model_invocation_credentials = None
            if region_request.model_invocation_role:
                from aws.osml.model_runner.common import get_credentials_for_assumed_role
                model_invocation_credentials = get_credentials_for_assumed_role(region_request.model_invocation_role)
            
            # Create and return async worker pool
            return AsyncTileWorkerPool(region_request, sensor_model, elevation_model, model_invocation_credentials)
            
        except Exception as e:
            logger.error(f"Failed to setup async worker pool: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise
