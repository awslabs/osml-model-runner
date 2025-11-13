import asyncio
import logging
import time
import traceback
from queue import Empty, Queue
from typing import Any, Dict, Optional

import boto3
from aws_embedded_metrics.logger.metrics_logger import MetricsLogger
from aws_embedded_metrics.metric_scope import metric_scope
from aws_embedded_metrics.unit import Unit

from aws.osml.features import Geolocator, ImagedFeaturePropertyAccessor
from aws.osml.model_runner.app_config import BotoConfig, MetricLabels, ServiceConfig
from aws.osml.model_runner.common import TileState, RequestStatus
from aws.osml.model_runner.database import FeatureTable, ImageRequestTable, RegionRequestTable, TileRequestTable
from aws.osml.model_runner.inference.async_sm_detector import AsyncSMDetector
from aws.osml.model_runner.utilities import S3Manager
from aws.osml.photogrammetry import ElevationModel, SensorModel

from .tile_worker import TileWorker

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
        in_queue: Queue,
        feature_detector: AsyncSMDetector,
        assumed_credentials: Optional[Dict[str, str]] = None,
        completion_queue: Optional[Queue] = None,
    ):
        """
        Initialize AsyncResultsWorker.

        :param worker_id: Unique identifier for this worker
        :param feature_table: FeatureTable for storing detected features
        :param geolocator: Optional geolocator for feature positioning (will be None for persistent workers)
        :param region_request_table: RegionRequestTable for tracking tile processing
        :param in_queue: Queue containing submitted jobs
        :param feature_detector: AsyncSMDetector instance
        :param completion_queue: Optional queue for completion notifications
        """

        # Initialize without geolocator - will create per request
        super().__init__(in_queue, feature_detector, None, feature_table, region_request_table)

        self.name = f"AsyncResultsWorker-{worker_id}"
        self.worker_id = worker_id
        self.completion_queue = completion_queue

        # Geolocator caching by image_id
        self._cached_geolocator = None
        self._cached_image_id = None

        self.tile_request_table = TileRequestTable(ServiceConfig.tile_request_table)
        self.image_request_table = ImageRequestTable(self.config.image_request_table)

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

        self.async_config = ServiceConfig.async_endpoint_config

        logger.info(f"AsyncResultsWorker-{worker_id} initialized")

    def _get_or_create_geolocator(
        self, image_id: str, sensor_model: Optional[SensorModel], elevation_model: Optional[ElevationModel]
    ) -> Optional[Geolocator]:
        """Get cached geolocator or create new one if image_id changed"""

        # Check if we can reuse cached geolocator
        if self._cached_geolocator is not None and self._cached_image_id == image_id:
            logger.debug(f"AsyncResultsWorker-{self.worker_id} reusing cached geolocator for image_id: {image_id}")
            return self._cached_geolocator

        # Create new geolocator
        if sensor_model is not None:
            logger.debug(f"AsyncResultsWorker-{self.worker_id} creating new geolocator for image_id: {image_id}")
            new_geolocator = Geolocator(ImagedFeaturePropertyAccessor(), sensor_model, elevation_model=elevation_model)

            # Cache the new geolocator
            self._cached_geolocator = new_geolocator
            self._cached_image_id = image_id

            return new_geolocator
        else:
            # No sensor model, clear cache
            self._cached_geolocator = None
            self._cached_image_id = None
            return None

    def run(self) -> None:
        """Modified run method with geolocator caching"""
        thread_event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(thread_event_loop)
        while True:
            try:
                work_item = self.in_queue.get(timeout=1.0)

                if work_item is None:  # Shutdown signal
                    break

                try:
                    request_id = work_item["request_id"]
                    image_info = work_item["tile_request_item"]
                    image_id = work_item.get("image_id")
                    sensor_model = work_item.get("sensor_model")
                    elevation_model = work_item.get("elevation_model")

                    # Get or create geolocator for this image_id
                    request_geolocator = self._get_or_create_geolocator(image_id, sensor_model, elevation_model)

                    # Process using existing method with request-specific geolocator
                    self.process_tile_with_geolocator(image_info, request_geolocator)

                    # Signal completion
                    if self.completion_queue:
                        self.completion_queue.put(
                            {
                                "request_id": request_id,
                                "status": "completed",
                                "timestamp": time.time(),
                                "worker_id": self.worker_id,
                            }
                        )

                except Exception as e:
                    logger.error(f"AsyncResultsWorker-{self.worker_id} error processing: {e}")

                    # Signal failure
                    if self.completion_queue:
                        self.completion_queue.put(
                            {
                                "request_id": request_id,
                                "status": "failed",
                                "error": str(e),
                                "timestamp": time.time(),
                                "worker_id": self.worker_id,
                            }
                        )

                finally:
                    self.in_queue.task_done()

            except Empty:
                continue
            except Exception as e:
                logger.error(f"Unexpected error in AsyncResultsWorker-{self.worker_id}: {e}")

        try:
            thread_event_loop.stop()
            thread_event_loop.close()
        except Exception as e:
            logger.warning("Failed to stop and close the thread event loop")
            logging.exception(e)

    def process_tile_with_geolocator(self, image_info: Dict, geolocator: Optional[Geolocator]) -> None:
        """Process tile with a specific geolocator (temporarily override self.geolocator)"""
        # Temporarily set the geolocator for this request
        original_geolocator = self.geolocator
        self.geolocator = geolocator

        try:
            # Use existing process_tile method
            self.process_tile(image_info)
        finally:
            # Restore original geolocator
            self.geolocator = original_geolocator

    def clear_geolocator_cache(self):
        """Clear cached geolocator (useful for testing or memory management)"""
        logger.debug(f"AsyncResultsWorker-{self.worker_id} clearing geolocator cache")
        self._cached_geolocator = None
        self._cached_image_id = None

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

            tile_item = self.tile_request_table.get_tile_request(tile_id, image_info.get("region_id"))
            if not tile_item:
                logger.warning(f"Could not find tile item for {tile_id}, {job_id}")
                return

            job_status = tile_item.tile_status

            logger.debug(f"AsyncResultsWorker-{self.worker_id} image_info: {tile_item}")

            if job_status == RequestStatus.IN_PROGRESS:
                # Get output location from image_info's output_s3_uri
                output_location = image_info.get("output_location")
                if output_location:
                    self._process_completed_job(image_info, output_location)
                else:
                    logger.error(f"Job [{type(image_info)}]{image_info} completed but no output location")
                    self._handle_failed_job(image_info, "No output location")

            elif job_status == RequestStatus.FAILED:
                error_message = tile_item.error_message or "Job failed"
                self._handle_failed_job(image_info, error_message)

            # # Check for timeout
            # elif time.time() - image_info["submitted_time"] > ServiceConfig.async_endpoint_config.max_wait_time:
            #     self._handle_failed_job(image_info, "Job timed out")

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
            feature_collection = S3_MANAGER._download_from_s3(output_location)

            # image_info = {
            #             "image_path": tmp_image_path,
            #             "region": tile_bounds,
            #             "image_id": region_request_item.image_id,
            #             "job_id": region_request_item.job_id,
            #             "region_id": region_request_item.region_id,
            #         }
            features = self._refine_features(feature_collection, image_info)

            if len(features) > 0:
                self.feature_table.add_features(features)

            self.region_request_table.add_tile(
                image_info.get("image_id"),
                image_info.get("region_id"),
                image_info.get("region"),
                TileState.SUCCEEDED,
            )

            # Update tile status to RequestStatus.SUCCESS
            if self.tile_request_table and image_info.get("tile_id") and image_info.get("region_id"):
                try:
                    self.tile_request_table.update_tile_status(image_info["tile_id"], image_info["region_id"], RequestStatus.SUCCESS)
                except Exception as e:
                    logger.warning(f"Failed to update tile status to {RequestStatus.SUCCESS}: {e}")

            logger.debug(f"AsyncResultsWorker-{self.worker_id} completed image_info: {image_info}")

        except Exception as e:
            logger.error(f"AsyncResultsWorker-{self.worker_id} error processing completed image_info {image_info}: {e}")
            logger.error(f"traceback: {traceback.format_exc()}")
            self._handle_failed_job(image_info, f"Result processing error: {e}")

    def _handle_failed_job(self, image_info: Dict[str, Any], reason: str) -> None:
        """
        Handle a failure by logging and cleaning up resources.
        """
        logger.error(f"AsyncResultsWorker-{self.worker_id} image_info {image_info} failed: {reason}")

        # Update tile status to FAILED
        if self.tile_request_table and image_info.get("tile_id") and image_info.get("region_id"):
            try:
                self.tile_request_table.update_tile_status(image_info["tile_id"], image_info["region_id"], RequestStatus.FAILED, reason)
            except Exception as e:
                logger.warning(f"Failed to update tile status to FAILED: {e}")

        assert isinstance(self.feature_detector, AsyncSMDetector)
