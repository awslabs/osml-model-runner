import logging
import time
import uuid
from typing import Optional
from queue import Queue

from aws_embedded_metrics.logger.metrics_logger import MetricsLogger
from aws_embedded_metrics.metric_scope import metric_scope
from aws_embedded_metrics.unit import Unit

from aws.osml.gdal import load_gdal_dataset
from aws.osml.model_runner.database import JobTable
from aws.osml.model_runner.app_config import MetricLabels
from aws.osml.model_runner.common import RequestStatus
from aws.osml.model_runner.api import get_image_path

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
        self.tile_status_monitor = tile_status_monitor

        # Add persistent worker pool components
        self._worker_pool = None
        self._work_queue = None
        self._completion_queue = Queue()

    def _ensure_worker_pool(self, tile_request):
        """Lazy initialization of persistent worker pool (no sensor/elevation model)"""
        if self._worker_pool is None:
            self._work_queue, self._worker_pool = setup_result_tile_workers(
                tile_request,
                sensor_model=None,  # Don't pass sensor model at setup
                elevation_model=None,  # Don't pass elevation model at setup
                completion_queue=self._completion_queue,
            )

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
            # Load models for this specific request
            image_path = get_image_path(tile_request.image_url, tile_request.image_read_role)
            raster_dataset, sensor_model = load_gdal_dataset(image_path)

            self._ensure_worker_pool(tile_request)

            # Submit to persistent worker queue with models and image_id for this request
            request_data = {
                "request_id": str(uuid.uuid4()),
                "tile_request_item": tile_request_item.__dict__,
                "image_id": tile_request_item.image_id,  # Add image_id for caching
                "sensor_model": sensor_model,
                "elevation_model": AsyncServiceConfig.elevation_model,
                "timestamp": time.time(),
            }
            self._work_queue.put(request_data)

            # Wait for completion
            result = self._completion_queue.get()  # Blocks until worker completes

            if result["status"] == "failed":
                raise Exception(result.get("error", "Worker processing failed"))

            logger.info(f"Tile request {result['request_id']} completed successfully")

        except Exception as err:
            failed_msg = f"Failed to process image tile: {err}"
            logger.error(failed_msg)
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

    def shutdown(self):
        """Gracefully shutdown worker pool"""
        if self._worker_pool:
            for worker in self._worker_pool:
                self._work_queue.put(None)  # Shutdown signal
            for worker in self._worker_pool:
                worker.join()
