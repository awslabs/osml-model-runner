import logging
from typing import Optional

from aws_embedded_metrics.logger.metrics_logger import MetricsLogger
from aws_embedded_metrics.metric_scope import metric_scope
from aws_embedded_metrics.unit import Unit

from aws.osml.gdal import load_gdal_dataset
from aws.osml.model_runner.database import JobTable
from aws.osml.model_runner.app_config import MetricLabels
from aws.osml.model_runner.common import RequestStatus

from .database import TileRequestTable, TileRequestItem
from .async_app_config import EnhancedServiceConfig
from .api import TileRequest
from .status import TileStatusMonitor
from .workers import setup_result_tile_workers
from .errors import ProcessTileException

# Set up logging configuration
logger = logging.getLogger(__name__)


class TileRequestHandler:
    def __init__(
        self,
        tile_request_table: TileRequestTable,
        job_table: JobTable,
        tile_status_monitor: TileStatusMonitor,
        config: EnhancedServiceConfig,
    ):
        self.tile_request_table = tile_request_table
        self.config = config
        self.job_table = job_table

    @metric_scope
    def process_tile_request(
        self,
        tile_request: TileRequest,
        tile_request_item: TileRequestItem,
        metrics: MetricsLogger = None,
    ):

        if isinstance(metrics, MetricsLogger):
            metrics.set_dimensions()

        if not tile_request.is_valid():
            logger.error(f"Invalid Tile Request! {tile_request.__dict__}")
            raise ValueError("Invalid Tile Request")

        try:

            # Set up our threaded tile worker pool
            raster_dataset, sensor_model = load_gdal_dataset(tile_request.image_path)
            region_request = self.tile_request_table.get_region_request(tile_request.tile_id)
            tile_queue, tile_workers = setup_result_tile_workers(region_request, sensor_model, self.config.elevation_model)

            # Process all our tiles
            _ = self.process_tiles(tile_request_item, tile_queue)

            region_request_item = self.job_table.complete_tile_request(tile_request.tile_id)
            tile_status = self.tile_status_monitor.get_status(tile_request.item)
            tile_request_item = self.tile_request_table.complete_tile_request(tile_request_item, tile_status)

            self.tile_status_monitor.process_event(tile_request_item, tile_status, "Completed tile processing")

            # Return the updated item
            return region_request_item

        except Exception as err:
            failed_msg = f"Failed to process image tile: {err}"
            logger.error(failed_msg)
            # Update the table to record the failure
            tile_request_item.message = failed_msg
            return self.fail_tile_request(tile_request_item)

    @metric_scope
    def fail_tile_request(self, tile_request_item: TileRequestItem, metrics: Optional[MetricsLogger] = None):
        if isinstance(metrics, MetricsLogger):
            metrics.put_metric(MetricLabels.ERRORS, 1, str(Unit.COUNT.value))
        try:
            tile_status = RequestStatus.FAILED
            tile_request_item = self.tile_request_table.complete_tile_request(tile_request_item, tile_status)
            self.tile_status_monitor.process_event(tile_request_item, tile_status, "Completed tile processing")
            return self.job_table.complete_tile_request(tile_request_item.image_id, error=True)
        except Exception as status_error:
            logger.error("Unable to update tile status in job table")
            logger.exception(status_error)
            raise ProcessTileException("Failed to process image tile!")

    def process_tiles(self, tile_request_item, tile_queue):
        tile_queue.put(tile_request_item.__dict__)
