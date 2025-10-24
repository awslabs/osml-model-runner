import asyncio
import logging
from threading import Thread
from typing import Any, Dict, Optional
from queue import Empty, Queue

from aws_embedded_metrics.metric_scope import metric_scope
from aws.osml.model_runner.app_config import ServiceConfig

from aws.osml.model_runner.utilities import S3Manager
from aws.osml.model_runner.database import TileRequestTable
from aws.osml.model_runner.inference.async_sm_detector import AsyncSMDetector

# Set up logging configuration
logger = logging.getLogger(__name__)

S3_MANAGER = S3Manager()


class AsyncSubmissionWorker(Thread):
    """
    Worker thread that submits tiles to async endpoints without waiting for completion.

    This worker processes tiles from the input queue, uploads them to S3, submits them
    to the async endpoint, and immediately moves on to the next tile. Completed jobs
    are tracked by separate polling workers.
    """

    def __init__(
        self,
        worker_id: int,
        tile_queue: Queue,
        feature_detector: AsyncSMDetector,
        tile_request_table: Optional[TileRequestTable] = None,
    ):
        """
        Initialize AsyncSubmissionWorker.

        :param worker_id: Unique identifier for this worker
        :param tile_queue: Queue containing tiles to process
        :param feature_detector: AsyncSMDetector instance for submissions
        :param tile_request_table: Optional TileRequestTable for tracking tile status
        """
        super().__init__(name=f"AsyncSubmissionWorker-{worker_id}")
        self.worker_id = worker_id
        self.tile_queue = tile_queue
        self.feature_detector = feature_detector
        self.failed_tile_count = 0
        self.processed_tile_count = 0
        self.running = True

        self.tile_request_table = TileRequestTable(ServiceConfig.tile_request_table)

        logger.debug(f"AsyncSubmissionWorker-{worker_id} initialized")

    def run(self) -> None:
        """Main worker loop for processing tile submissions."""
        logger.debug(f"AsyncSubmissionWorker-{self.worker_id} started")

        try:
            thread_event_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(thread_event_loop)

            logger.debug(f"Worker: {self.worker_id} staring while loop")
            while self.running:
                try:
                    # Get tile from queue with timeout
                    tile_info = self.tile_queue.get(timeout=1.0)

                    logger.debug(
                        f"Got tile in submission worker from region handler: {tile_info}, on worker: {self.worker_id}"
                    )

                    # Check for shutdown signal
                    if tile_info is None:
                        logger.info(f"AsyncSubmissionWorker-{self.worker_id} received shutdown signal")
                        break

                    # Process tile submission
                    success = self.process_tile_submission(tile_info)

                    if success:
                        self.processed_tile_count += 1
                    else:
                        self.failed_tile_count += 1

                    # Mark task as done
                    self.tile_queue.task_done()
                    logger.info(f"Completing task on submission worker: {self.worker_id} for {tile_info.get('tile_id')}")

                except Empty:
                    # Timeout waiting for tile, continue loop
                    continue

                except Exception as e:
                    logger.error(f"AsyncSubmissionWorker-{self.worker_id} error: {e}")
                    self.failed_tile_count += 1

                    # Mark task as done if we got a tile
                    try:
                        logger.info(f"Error on submission worker: {self.worker_id} on error")
                        self.tile_queue.task_done()
                    except ValueError:
                        pass  # task_done() called more times than get()

            try:
                thread_event_loop.stop()
                thread_event_loop.close()
            except Exception as e:
                logger.warning("Failed to stop and close the thread event loop")
                logging.exception(e)

        finally:
            logger.info(
                f"AsyncSubmissionWorker-{self.worker_id} finished. "
                f"Processed: {self.processed_tile_count}, Failed: {self.failed_tile_count}"
            )

    @metric_scope
    def process_tile_submission(self, tile_info: Dict[str, Any], metrics) -> bool:
        """
        Process a single tile submission to async endpoint.

        :param tile_info: Tile information dictionary
        :return: True if submission successful, False otherwise
        """
        try:
            logger.info(f"AsyncSubmissionWorker-{self.worker_id} processing tile: {tile_info.get('region')}")

            # Generate unique key for S3 input
            input_key = S3_MANAGER.generate_unique_key("input")

            # Upload tile to S3
            with open(tile_info["image_path"], "rb") as payload:
                input_s3_uri = S3_MANAGER._upload_to_s3(payload, input_key)

            # Submit to async endpoint
            # The use of custom attributes does not work because it depends on the model to
            # parse and pass through the required information
            inference_id, output_location = self.feature_detector._invoke_async_endpoint(
                input_s3_uri, metrics
            )  # , custom_attributes=tile_info

            logger.info(f"Async inference job submitted with {inference_id=}, {output_location=}")

            # Update tile status to PROCESSING and store inference info
            if self.tile_request_table and tile_info.get("tile_id") and tile_info.get("region_id"):
                try:
                    # Update status to PROCESSING
                    self.tile_request_table.update_tile_status(tile_info["tile_id"], tile_info["region_id"], "PROCESSING")

                    # Update inference_id and output_location
                    self.tile_request_table.update_tile_inference_info(
                        tile_info["tile_id"], tile_info["region_id"], inference_id, output_location
                    )
                except Exception as e:
                    logger.warning(f"Failed to update tile status and inference info: {e}")

            return True

        except Exception as e:
            logger.error(f"AsyncSubmissionWorker-{self.worker_id} failed to submit tile: {e}")

            # Update tile status to FAILED due to submission error
            if self.tile_request_table and tile_info.get("tile_id") and tile_info.get("region_id"):
                try:
                    logger.info(f"Updating status for {tile_info=}")
                    self.tile_request_table.update_tile_status(
                        tile_info["tile_id"], tile_info["region_id"], "FAILED", f"Submission error: {str(e)}"
                    )
                except Exception as update_e:
                    logger.warning(f"Failed to update tile status to FAILED: {update_e}")

            return False

    def stop(self) -> None:
        """Signal the worker to stop processing."""
        self.running = False
