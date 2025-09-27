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
from .async_app_config import AsyncServiceConfig
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
    ):
        self.tile_request_table = tile_request_table
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
            raster_dataset, sensor_model = load_gdal_dataset(tile_request.image_url)
            region_request = self.tile_request_table.get_region_request(tile_request.tile_id)
            # using a subclass of TileWorker to reuse the code already there.
            tile_queue, tile_workers = setup_result_tile_workers(
                region_request, sensor_model, AsyncServiceConfig.elevation_model
            )

            # submit to worker queue.
            # Using 'process_tiles" to maintain original TileWorker function naming.
            # self.process_tiles(tile_request_item, tile_queue)

            tile_queue.put(tile_request_item.__dict__)

            # Put enough empty messages on the queue to shut down the workers
            for i in range(len(tile_workers)):
                tile_queue.put(None)

                # Ensure the wait for tile workers happens within the context where we create
                # the temp directory. If the context is exited before all workers return then
                # the directory will be deleted, and we will potentially lose tiles.
                # Wait for all the workers to finish gracefully before we clean up the temp directory
                tile_error_count = 0
                for worker in tile_workers:
                    worker.join()
                    tile_error_count += worker.failed_tile_count

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
            return self.tile_request_table.complete_tile_request(tile_request_item.image_id, error=True)
        except Exception as status_error:
            logger.error("Unable to update tile status in job table")
            logger.exception(status_error)
            raise ProcessTileException("Failed to process image tile!")

    # def process_tiles(self, tile_request_item, tile_queue):
    #     tile_queue.put(tile_request_item.__dict__)
