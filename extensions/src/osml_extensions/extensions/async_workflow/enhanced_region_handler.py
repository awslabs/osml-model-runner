#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
import traceback
import uuid
from typing import Optional, Tuple

import tempfile
from pathlib import Path
from secrets import token_hex

from aws.osml.model_runner.queue import RequestQueue
from aws.osml.model_runner.database import JobTable
from aws.osml.model_runner.database import EndpointStatisticsTable
from aws.osml.model_runner.common import EndpointUtils
from aws.osml.model_runner.status import RegionStatusMonitor
from aws.osml.model_runner.database import RegionRequestTable

from aws_embedded_metrics.logger.metrics_logger import MetricsLogger
from aws_embedded_metrics.metric_scope import metric_scope
from aws_embedded_metrics.unit import Unit

from osgeo import gdal

from aws.osml.gdal import GDALConfigEnv
from aws.osml.image_processing.gdal_tile_factory import GDALTileFactory
from aws.osml.model_runner.common import get_credentials_for_assumed_role
from aws.osml.model_runner.api import RegionRequest
from aws.osml.model_runner.database import JobItem, RegionRequestItem
from aws.osml.model_runner.region_request_handler import RegionRequestHandler
from aws.osml.model_runner.tile_worker import TilingStrategy
from aws.osml.model_runner.tile_worker.tile_worker_utils import _create_tile
from aws.osml.photogrammetry import SensorModel

from osml_extensions.registry import HandlerType, register_handler

from .async_app_config import AsyncServiceConfig
from .errors import ExtensionRuntimeError
from .database import TileRequestTable, TileRequestItem
from .api import TileRequest
from .workers import setup_submission_tile_workers

logger = logging.getLogger(__name__)


@register_handler(
    request_type="async_sm_endpoint",
    handler_type=HandlerType.REGION_REQUEST_HANDLER,
    name="enhanced_region_request_handler",
    description="Enhanced region request handler with async processing capabilities",
)
class EnhancedRegionRequestHandler(RegionRequestHandler):
    """
    Enhanced region request handler with additional monitoring and processing capabilities.

    This class maintains full compatibility with the base RegionRequestHandler while adding
    enhanced features for improved performance and monitoring.
    """

    def __init__(
        self,
        tile_request_table: TileRequestTable,
        tile_request_queue: RequestQueue,
        region_request_table: RegionRequestTable,
        job_table: JobTable,
        region_status_monitor: RegionStatusMonitor,
        endpoint_statistics_table: EndpointStatisticsTable,
        tiling_strategy: TilingStrategy,
        endpoint_utils: EndpointUtils,
        config: AsyncServiceConfig,
    ) -> None:
        """Initialize the enhanced region request handler with tile tracking capabilities."""
        super().__init__(
            region_request_table,
            job_table,
            region_status_monitor,
            endpoint_statistics_table,
            tiling_strategy,
            endpoint_utils,
            config,
        )

        self.tile_request_queue = tile_request_queue
        self.tile_request_table = tile_request_table

        logger.info("Initialized EnhancedRegionRequestHandler")

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
        logger.info(f"EnhancedRegionRequestHandler processing region: {region_request.region_id}")

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
                        "HandlerType": "EnhancedRegionRequestHandler",
                    }
                )

            # Handle self-throttling with enhanced monitoring
            if AsyncServiceConfig.self_throttling:
                max_regions = self.endpoint_utils.calculate_max_regions(
                    region_request.model_name, region_request.model_invocation_role
                )
                self.endpoint_statistics_table.upsert_endpoint(region_request.model_name, max_regions)
                in_progress = self.endpoint_statistics_table.current_in_progress_regions(region_request.model_name)

                if in_progress >= max_regions:
                    if isinstance(metrics, MetricsLogger):
                        metrics.put_metric("EnhancedRegionRequestHandler.Throttles", 1, str(Unit.COUNT.value))
                    logger.warning(
                        f"Enhanced handler throttling region request. (Max: {max_regions} In-progress: {in_progress})"
                    )
                    from aws.osml.model_runner.exceptions import SelfThrottledRegionException

                    raise SelfThrottledRegionException

                self.endpoint_statistics_table.increment_region_count(region_request.model_name)

            try:
                # Start region request processing
                self.region_request_table.start_region_request(region_request_item)
                logger.debug(f"Enhanced handler starting region request: region id: {region_request_item.region_id}")

                # # Set up async worker pool
                # worker_setup = self._setup_async_worker_pool(
                #     region_request, sensor_model, AsyncServiceConfig.elevation_model, metrics
                # )

                # Set up our threaded tile worker pool
                tile_queue, tile_workers = setup_submission_tile_workers(
                    region_request, sensor_model, AsyncServiceConfig.elevation_model
                )

                # Process tiles using appropriate method
                self.queue_tile_request(
                    tile_queue,
                    tile_workers,
                    self.tiling_strategy,
                    region_request,
                    region_request_item,
                    raster_dataset,
                    sensor_model,
                    metrics=metrics,
                )

                image_request_item = self.job_table.get_image_request(region_request.image_id)

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
                if AsyncServiceConfig.self_throttling:
                    self.endpoint_statistics_table.decrement_region_count(region_request.model_name)

        except Exception as e:
            failed_msg = f"EnhancedRegionRequestHandler error: {e}"
            logger.error(failed_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
            if isinstance(metrics, MetricsLogger):
                metrics.put_metric("EnhancedRegionRequestHandler.Errors", 1, str(Unit.COUNT.value))

            # Check if we should fallback or re-raise
            # Check if we should fallback or re-raise
            if isinstance(AsyncServiceConfig, AsyncServiceConfig) and not AsyncServiceConfig.extension_fallback_enabled:
                raise ExtensionRuntimeError(f"Enhanced region processing failed: {e}") from e

            region_request_item.message = failed_msg
            return self.fail_region_request(region_request_item)

    def load_region_request(
        self,
        tiling_strategy: TilingStrategy,
        region_request_item: RegionRequestItem,
    ):

        try:
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
                    if [[region[0][0], region[0][1]], [region[1][0], region[1][1]]]
                    not in region_request_item.succeeded_tiles
                ]
                tile_array = filtered_regions

            total_tile_count = len(tile_array)

            if total_tile_count == 0:
                logger.debug("No tiles to process")
                return None
            return tile_array
        except Exception as err:
            logger.error(f"Error loading region request: {err}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    def queue_tile_request(
        self,
        tile_queue,
        tile_workers,
        tiling_strategy,
        region_request: RegionRequest,
        region_request_item: RegionRequestItem,
        raster_dataset,
        sensor_model: Optional[SensorModel],
        metrics: MetricsLogger,
    ) -> Tuple[int, int]:
        """
        Process tiles using the async worker pool optimization.

        :param region_request_item: The region request item
        :param raster_dataset: The raster dataset
        :param sensor_model: Optional sensor model
        :param metrics: Optional metrics logger
        :return: Tuple of (total_tile_count, failed_tile_count)
        """
        logger.debug("Processing tiles with async worker pool optimization")

        try:

            tile_array = self.load_region_request(self.tiling_strategy, region_request_item)

            # Set up credentials for image reading
            image_read_credentials = None
            if region_request_item.image_read_role:
                image_read_credentials = get_credentials_for_assumed_role(region_request_item.image_read_role)

            tiles_submitted = 0
            logger.debug(f"Processing tiles with creds: {image_read_credentials}")
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
                        absolute_tile_path = _create_tile(gdal_tile_factory, tile_bounds, tmp_image_path)

                        if not absolute_tile_path:
                            continue

                        # Create image info
                        tile_request = TileRequest(
                            tile_id=str(uuid.uuid4()),
                            region_id=region_request_item.region_id,
                            image_id=region_request_item.image_id,
                            job_id=region_request_item.job_id,
                            image_path=str(tmp_image_path.absolute()),
                            image_url=region_request.image_url,
                            tile_bounds=tile_bounds,
                            model_invocation_role=region_request.model_invocation_role,
                            tile_size=region_request.tile_size,
                            tile_overlap=region_request.tile_overlap,
                            model_invoke_mode=str(region_request.model_invoke_mode),
                            model_name=region_request.model_name,
                            image_read_role=region_request.image_read_role,
                        )

                        # Add tile to tracking database
                        tile_request_item = TileRequestItem.from_tile_request(tile_request)
                        self.tile_request_table.start_tile_request(tile_request_item)
                        tile_queue.put(tile_request.__dict__)
                        tiles_submitted += 1

                    # Put enough empty messages on the queue to shut down the workers
                    for i in range(len(tile_workers)):
                        tile_queue.put(None)

            logger.info(f"Region handler submittedd {tiles_submitted} tiles to workers")

        except Exception as e:
            logger.error(f"Error processing tiles with async worker pool: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")

            if isinstance(metrics, MetricsLogger):
                metrics.put_metric("AsyncWorkerPool.ProcessingErrors", 1, str(Unit.COUNT.value))
            raise
