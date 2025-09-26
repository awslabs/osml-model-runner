import boto3
from typing import Dict, Optional, Any
import logging
from queue import Queue
import time

from aws.osml.features import Geolocator
from aws.osml.model_runner.database import FeatureTable, RegionRequestTable
from aws.osml.model_runner.app_config import MetricLabels
from aws.osml.model_runner.app_config import BotoConfig
from aws.osml.model_runner.tile_worker import TileWorker
from aws.osml.model_runner.common import TileState

from aws_embedded_metrics.metric_scope import metric_scope
from aws_embedded_metrics.unit import Unit
from aws_embedded_metrics.logger.metrics_logger import MetricsLogger

from ..s3 import S3Manager
from ..database import TileRequestTable
from ..detectors import AsyncSMDetector
from ..async_app_config import AsyncServiceConfig
from ..metrics import AsyncMetricsTracker

# Set up logging configuration
logger = logging.getLogger(__name__)

S3_MANAGER = S3Manager()


class AsyncResultsWorker(TileWorker):
    """
    Worker thread for async inference completion and processes results.

    This worker monitors submitted jobs, waits for their completion, downloads results,
    and processes them when ready. It operates independently of submission workers.
    """

    def __init__(
        self,
        worker_id: int,
        feature_table: FeatureTable,
        geolocator: Optional[Geolocator],
        region_request_table: RegionRequestTable,
        tile_queue: Queue,
        feature_detector: AsyncSMDetector,  # TODO: Is this needed here?
        metrics_tracker: Optional[AsyncMetricsTracker] = None,
        assumed_credentials: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize AsyncResultsWorker.

        :param worker_id: Unique identifier for this worker
        :param feature_table: FeatureTable for storing detected features
        :param geolocator: Optional geolocator for feature positioning
        :param region_request_table: RegionRequestTable for tracking tile processing
        :param tile_queue: Queue containing submitted jobs
        :param feature_detector: AsyncSMDetector instance
        :param metrics_tracker: Optional metrics tracker
        """

        super().__init__(tile_queue, feature_detector, geolocator, feature_table, region_request_table)

        self.name = f"AsyncResultsWorker-{worker_id}"
        self.worker_id = worker_id
        self.metrics_tracker = metrics_tracker
        self.running = True

        self.tile_request_table = TileRequestTable(AsyncServiceConfig.tile_table_name)

        # Initialize async configuration

        if assumed_credentials is not None:
            # Use the provided credentials to invoke SageMaker endpoints in another AWS account.
            self.sm_client = boto3.client(
                "sagemaker-runtime",
                config=BotoConfig.sagemaker,
                aws_access_key_id=assumed_credentials.get("AccessKeyId"),
                aws_secret_access_key=assumed_credentials.get("SecretAccessKey"),
                aws_session_token=assumed_credentials.get("SessionToken"),
            )
        else:
            # Use the default role for this container if no specific credentials are provided.
            self.sm_client = boto3.client("sagemaker-runtime", config=BotoConfig.sagemaker)

        self.async_config = AsyncServiceConfig.async_endpoint_config

        logger.info(f"AsyncResultsWorker-{worker_id} initialized")

    @metric_scope
    def process_tile(self, image_info: Dict, metrics: MetricsLogger = None) -> None:
        """
        This method handles the processing of a single tile by invoking the ML model, geolocating the detections to
        create features and finally storing the features in the database.

        :param image_info: description of the tile to be processed
        :param metrics: the current metric scope
        """
        if isinstance(metrics, MetricsLogger):
            metrics.set_dimensions()
            metrics.put_dimensions(
                {
                    MetricLabels.OPERATION_DIMENSION: MetricLabels.TILE_PROCESSING_OPERATION,
                    MetricLabels.MODEL_NAME_DIMENSION: self.feature_detector.endpoint,
                }
            )
            metrics.put_metric(MetricLabels.INVOCATIONS, 1, str(Unit.COUNT.value))

        try:

            # Get image_info status from tile table
            tile_id = image_info.get("tile_id")
            job_id = image_info.get("job_id")

            current_time = time.time()

            tile_item = self.tile_request_table.get_tile_request(tile_id, job_id)
            if not tile_item:
                logger.warning(f"Could not find tile item for {tile_id}, {job_id}")
                return

            job_status = tile_item.status

            logger.debug(f"AsyncResultsWorker-{self.worker_id} image_info: {tile_item}")

            if job_status == "COMPLETED":
                # Get output location from image_info's output_s3_uri
                output_location = getattr(image_info, "output_s3_uri", None)
                if output_location:
                    self._process_completed_job(image_info, output_location)
                else:
                    logger.error(f"Job {image_info} completed but no output location")
                    self._handle_failed_job(image_info, "No output location")

            elif job_status == "FAILED":
                error_message = tile_item.error_message or "Job failed"
                self._handle_failed_job(image_info, error_message)

            # Check for timeout
            elif current_time - image_info["submitted_time"] > AsyncServiceConfig.async_endpoint_config.max_wait_time:
                self._handle_failed_job(image_info, "Job timed out")

        except Exception as e:
            logger.error(f"AsyncResultsWorker-{self.worker_id} error image_info {image_info}: {e}")
            self._handle_failed_job(image_info, f"error: {e}")

    def _process_completed_job(self, image_info: Dict[str, Any], output_location: str) -> None:
        """
        Process a completed image_info by downloading results and storing them.

        :param image_info: Completed
        :param output_location: S3 URI of the output data
        """
        try:
            logger.info(f"AsyncResultsWorker-{self.worker_id} processing completed image_info: {image_info}")

            # Download and parse results
            feature_collection = AsyncServiceConfig._download_from_s3(output_location)

            features = self._refine_features(feature_collection, image_info.tile_info)

            if len(features) > 0:
                self.feature_table.add_features(features)

            self.region_request_table.add_tile(
                image_info.tile_info.get("image_id"),
                image_info.tile_info.get("region_id"),
                image_info.tile_info.get("region"),
                TileState.SUCCEEDED,
            )

            if self.metrics_tracker:
                self.metrics_tracker.increment_counter("JobCompletions")
                processing_time = time.time() - image_info.submitted_time
                self.metrics_tracker.set_counter("JobProcessingTime", int(processing_time))

            # Update tile status to COMPLETED
            if self.tile_request_table and image_info.tile_info.get("tile_id") and image_info.tile_info.get("job_id"):
                try:
                    self.tile_request_table.update_tile_status(
                        image_info.tile_info["tile_id"], image_info.tile_info["job_id"], "COMPLETED"
                    )
                except Exception as e:
                    logger.warning(f"Failed to update tile status to COMPLETED: {e}")

            logger.debug(f"AsyncResultsWorker-{self.worker_id} completed image_info: {image_info}")

        except Exception as e:
            logger.error(f"AsyncResultsWorker-{self.worker_id} error processing completed image_info {image_info}: {e}")
            self._handle_failed_job(image_info, f"Result processing error: {e}")

    def _handle_failed_job(self, image_info: Dict[str, Any], reason: str) -> None:
        """
        Handle a failure by logging and cleaning up resources.
        """
        logger.error(f"AsyncResultsWorker-{self.worker_id} image_info {image_info} failed: {reason}")

        # Update tile status to FAILED
        if self.tile_request_table and image_info.tile_info.get("tile_id") and image_info.tile_info.get("job_id"):
            try:
                self.tile_request_table.update_tile_status(
                    image_info.tile_info["tile_id"], image_info.tile_info["job_id"], "FAILED", reason
                )
            except Exception as e:
                logger.warning(f"Failed to update tile status to FAILED: {e}")

        assert isinstance(self.feature_detector, AsyncSMDetector)

        if self.metrics_tracker:
            self.metrics_tracker.increment_counter("JobFailures")
