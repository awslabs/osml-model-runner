#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import asyncio
import boto3
import logging
import time
from queue import Empty, Full, Queue
from threading import Thread
from typing import Any, Dict, List, Optional, Tuple

from aws_embedded_metrics.logger.metrics_logger import MetricsLogger
from aws_embedded_metrics.metric_scope import metric_scope
from osml_extensions.enhanced_app_config import EnhancedServiceConfig

from aws.osml.features import Geolocator, ImagedFeaturePropertyAccessor
from aws.osml.model_runner.common import TileState
from aws.osml.model_runner.database import FeatureTable, RegionRequestTable
from aws.osml.model_runner.tile_worker import TileWorker
from aws.osml.model_runner.app_config import BotoConfig

from ..async_app_config import AsyncEndpointConfig, AsyncServiceConfig
from ..detectors.async_sm_detector import AsyncSMDetector
from ..factory import EnhancedFeatureDetectorFactory
from ..metrics import AsyncMetricsTracker
from ..s3 import S3Manager
from ..tile_request_table import TileRequestTable
from ..utils import ResourceManager, ResourceType
from ..polling import AsyncInferencePoller, AsyncInferenceTimeoutError

logger = logging.getLogger(__name__)


S3_MANAGER = S3Manager()


# TODO: Convert to dataclass
class AsyncInferenceJob:
    """
    Represents an async inference job with metadata.
    """

    def __init__(self, inference_id: str, tile_info: Dict[str, Any], input_s3_uri: str, output_s3_uri: str, submitted_time: float):
        """
        Initialize AsyncInferenceJob.

        :param inference_id: SageMaker inference job ID
        :param tile_info: Original tile information
        :param input_s3_uri: S3 URI of input data
        :param output_s3_uri: S3 URI of output data
        :param submitted_time: Timestamp when job was submitted
        """
        self.inference_id = inference_id
        self.tile_info = tile_info
        self.input_s3_uri = input_s3_uri
        self.output_s3_uri = output_s3_uri
        self.submitted_time = submitted_time
        self.poll_count = 0
        self.last_poll_time = submitted_time


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
        job_queue: Queue,
        feature_detector: AsyncSMDetector,
        config: AsyncEndpointConfig,
        metrics_tracker: Optional[AsyncMetricsTracker] = None,
        tile_table: Optional[TileRequestTable] = None,
    ):
        """
        Initialize AsyncSubmissionWorker.

        :param worker_id: Unique identifier for this worker
        :param tile_queue: Queue containing tiles to process
        :param job_queue: Queue to place submitted jobs for polling
        :param feature_detector: AsyncSMDetector instance for submissions
        :param config: AsyncEndpointConfig for settings
        :param metrics_tracker: Optional metrics tracker
        :param tile_table: Optional TileRequestTable for tracking tile status
        """
        super().__init__(name=f"AsyncSubmissionWorker-{worker_id}")
        self.worker_id = worker_id
        self.tile_queue = tile_queue
        self.job_queue = job_queue
        self.feature_detector = feature_detector
        self.config = config
        self.metrics_tracker = metrics_tracker
        self.tile_table = tile_table
        self.failed_tile_count = 0
        self.processed_tile_count = 0
        self.running = True

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

                    logger.info(f"Got tile: {tile_info}, on worker: {self.worker_id}")

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
                    logger.info(f"Completing task on worker: {self.worker_id}")
                    self.tile_queue.task_done()

                except Empty:
                    # Timeout waiting for tile, continue loop
                    continue

                except Exception as e:
                    logger.error(f"AsyncSubmissionWorker-{self.worker_id} error: {e}")
                    self.failed_tile_count += 1

                    # Mark task as done if we got a tile
                    try:
                        logger.info(f"Completing task on worker: {self.worker_id} on error")
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

            # Update tile status to PROCESSING
            if self.tile_table and tile_info.get("tile_id") and tile_info.get("job_id"):
                try:
                    self.tile_table.update_tile_status(
                        tile_info["tile_id"], 
                        tile_info["job_id"], 
                        "PROCESSING"
                    )
                except Exception as e:
                    logger.warning(f"Failed to update tile status to PROCESSING: {e}")

            # Track submission timing
            if self.metrics_tracker:
                self.metrics_tracker.start_timer("AsyncSubmissionTime")

            # Generate unique key for S3 input
            input_key = S3_MANAGER.generate_unique_key("input")

            # Upload tile to S3
            with open(tile_info["image_path"], "rb") as payload:
                input_s3_uri = S3_MANAGER._upload_to_s3(payload, input_key)

            # Submit to async endpoint
            inference_id, output_location = self.feature_detector._invoke_async_endpoint(input_s3_uri, metrics)

            # Create job object and add to polling queue
            job = AsyncInferenceJob(
                inference_id=inference_id,
                tile_info=tile_info,
                input_s3_uri=input_s3_uri,
                output_s3_uri=output_location,
                submitted_time=time.time(),
            )

            logger.info(f"job info: {job}")

            # Add job to polling queue with timeout
            try:
                self.job_queue.put(job, timeout=self.config.job_queue_timeout)

                if self.metrics_tracker:
                    self.metrics_tracker.stop_timer("AsyncSubmissionTime")
                    self.metrics_tracker.increment_counter("TileSubmissions")

                logger.debug(f"AsyncSubmissionWorker-{self.worker_id} submitted job: {inference_id}")
                return True

            except Full:
                logger.error(f"AsyncSubmissionWorker-{self.worker_id} job queue full, dropping tile")
                
                # Update tile status to FAILED due to queue full
                if self.tile_table and tile_info.get("tile_id") and tile_info.get("job_id"):
                    try:
                        self.tile_table.update_tile_status(
                            tile_info["tile_id"], 
                            tile_info["job_id"], 
                            "FAILED",
                            "Job queue full"
                        )
                    except Exception as e:
                        logger.warning(f"Failed to update tile status to FAILED: {e}")
                
                return False

        except Exception as e:
            logger.error(f"AsyncSubmissionWorker-{self.worker_id} failed to submit tile: {e}")

            # Update tile status to FAILED due to submission error
            if self.tile_table and tile_info.get("tile_id") and tile_info.get("job_id"):
                try:
                    self.tile_table.update_tile_status(
                        tile_info["tile_id"], 
                        tile_info["job_id"], 
                        "FAILED",
                        f"Submission error: {str(e)}"
                    )
                except Exception as update_e:
                    logger.warning(f"Failed to update tile status to FAILED: {update_e}")

            if self.metrics_tracker:
                self.metrics_tracker.increment_counter("TileSubmissionFailures")

            return False

    def stop(self) -> None:
        """Signal the worker to stop processing."""
        self.running = False


class AsyncPollingWorker(TileWorker):
    """
    Worker thread that polls for async inference completion and processes results.

    This worker monitors submitted jobs, polls for their completion, downloads results,
    and processes them when ready. It operates independently of submission workers.
    """

    def __init__(
        self,
        worker_id: int,
        feature_table: FeatureTable,
        geolocator: Optional[Geolocator],
        region_request_table: RegionRequestTable,
        in_queue: Queue,
        result_queue: Queue,
        feature_detector: AsyncSMDetector,
        config: AsyncEndpointConfig,
        metrics_tracker: Optional[AsyncMetricsTracker] = None,
        assumed_credentials: Optional[Dict[str, str]] = None,
        tile_table: Optional[TileRequestTable] = None,
    ):
        """
        Initialize AsyncPollingWorker.

        :param worker_id: Unique identifier for this worker
        :param feature_table: FeatureTable for storing detected features
        :param geolocator: Optional geolocator for feature positioning
        :param region_request_table: RegionRequestTable for tracking tile processing
        :param in_queue: Queue containing submitted jobs to poll
        :param result_queue: Queue to place completed results
        :param feature_detector: AsyncSMDetector instance for polling
        :param config: AsyncEndpointConfig for settings
        :param metrics_tracker: Optional metrics tracker
        :param tile_table: Optional TileRequestTable for tracking tile status
        """

        super().__init__(in_queue, feature_detector, geolocator, feature_table, region_request_table)

        self.name = f"AsyncPollingWorker-{worker_id}"
        self.worker_id = worker_id
        self.result_queue = result_queue
        self.config = config
        self.metrics_tracker = metrics_tracker
        self.tile_table = tile_table
        self.active_jobs: Dict[str, AsyncInferenceJob] = {}
        self.completed_job_count = 0
        self.running = True

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
        self.poller = AsyncInferencePoller(self.sm_client, self.async_config)

        logger.info(f"AsyncPollingWorker-{worker_id} initialized")

    def run(self) -> None:
        """Main worker loop for polling job completion."""
        logger.info(f"AsyncPollingWorker-{self.worker_id} started")

        try:
            thread_event_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(thread_event_loop)
            while self.running:
                try:
                    # Check for new jobs to monitor
                    self._collect_new_jobs()

                    # Poll active jobs for completion
                    self._poll_active_jobs()

                    # Sleep briefly to avoid overwhelming the API
                    time.sleep(1.0)

                except Exception as e:
                    logger.error(f"AsyncPollingWorker-{self.worker_id} error: {e}")

            try:
                thread_event_loop.stop()
                thread_event_loop.close()
            except Exception as e:
                logger.warning("Failed to stop and close the thread event loop")
                logging.exception(e)

        finally:
            logger.info(
                f"AsyncPollingWorker-{self.worker_id} finished. "
                f"Completed: {self.completed_job_count}, Failed: {self.failed_tile_count}"
            )

    def _collect_new_jobs(self) -> None:
        """Collect new jobs from the job queue."""
        # logger.info("Began job collection")
        while len(self.active_jobs) < self.config.max_concurrent_jobs:
            try:
                job = self.in_queue.get_nowait()
                logger.info(f"Poller got job: {job}")
                self.active_jobs[job.inference_id] = job
                logger.info(f"AsyncPollingWorker-{self.worker_id} monitoring job: {job.inference_id}")

            except Empty:
                break  # No more jobs available


    def _poll_active_jobs(self) -> None:
        """Poll all active jobs for completion."""
        completed_jobs = []

        for inference_id, job in self.active_jobs.items():
            try:
                logger.info(f"poller checking for {inference_id}, {job}")
                # Check if enough time has passed since last poll
                current_time = time.time()
                time_since_last_poll = current_time - job.last_poll_time

                # Calculate appropriate polling interval based on job age and attempts
                polling_interval = self._calculate_polling_interval(job)

                if time_since_last_poll < polling_interval:
                    continue  # Not time to poll this job yet

                # Get job status from tile table instead of polling SageMaker
                tile_id = job.tile_info.get("tile_id")
                job_id = job.tile_info.get("job_id")
                
                if not tile_id or not job_id or not self.tile_table:
                    logger.warning(f"Missing tile_id, job_id, or tile_table for job {inference_id}")
                    continue

                tile_item = self.tile_table.get_tile_request(tile_id, job_id)
                if not tile_item:
                    logger.warning(f"Could not find tile item for {tile_id}, {job_id}")
                    continue

                job_status = tile_item.status
                job.poll_count += 1
                job.last_poll_time = current_time

                if self.metrics_tracker:
                    self.metrics_tracker.increment_counter("JobPolls")

                logger.info(f"AsyncPollingWorker-{self.worker_id} polled job {inference_id}: {job_status}")

                if job_status == "COMPLETED":
                    # Get output location from job's output_s3_uri
                    output_location = getattr(job, 'output_s3_uri', None)
                    if output_location:
                        self._process_completed_job(job, output_location)
                        completed_jobs.append(inference_id)
                        self.completed_job_count += 1
                    else:
                        logger.error(f"Job {inference_id} completed but no output location")
                        self._handle_failed_job(job, "No output location")
                        completed_jobs.append(inference_id)
                        self.failed_tile_count += 1

                elif job_status == "FAILED":
                    error_message = tile_item.error_message or "Job failed"
                    self._handle_failed_job(job, error_message)
                    completed_jobs.append(inference_id)
                    self.failed_tile_count += 1

                # Check for timeout
                elif current_time - job.submitted_time > self.config.max_wait_time:
                    self._handle_failed_job(job, "Job timed out")
                    completed_jobs.append(inference_id)
                    self.failed_tile_count += 1

            except Exception as e:
                logger.error(f"AsyncPollingWorker-{self.worker_id} error polling job {inference_id}: {e}")
                self._handle_failed_job(job, f"Polling error: {e}")
                completed_jobs.append(inference_id)
                self.failed_tile_count += 1

        # Remove completed jobs from active list
        for inference_id in completed_jobs:
            del self.active_jobs[inference_id]

    def _calculate_polling_interval(self, job: AsyncInferenceJob) -> float:
        """
        Calculate appropriate polling interval based on job age and poll count.

        :param job: AsyncInferenceJob to calculate interval for
        :return: Polling interval in seconds
        """
        base_interval = self.config.polling_interval
        multiplier = self.config.exponential_backoff_multiplier

        # Apply exponential backoff based on poll count
        interval = base_interval * (multiplier**job.poll_count)

        # Cap at maximum interval
        return min(interval, self.config.max_polling_interval)

    def _process_completed_job(self, job: AsyncInferenceJob, output_location: str) -> None:
        """
        Process a completed job by downloading results and storing them.

        :param job: Completed AsyncInferenceJob
        :param output_location: S3 URI of the output data
        """
        try:
            logger.info(f"AsyncPollingWorker-{self.worker_id} processing completed job: {job.inference_id}")

            # Download and parse results
            feature_collection = AsyncServiceConfig._download_from_s3(output_location)

            features = self._refine_features(feature_collection, job.tile_info)

            if len(features) > 0:
                self.feature_table.add_features(features)

            self.region_request_table.add_tile(
                job.tile_info.get("image_id"),
                job.tile_info.get("region_id"),
                job.tile_info.get("region"),
                TileState.SUCCEEDED,
            )

            # Create result object
            result = {
                "tile_info": job.tile_info,
                "feature_collection": feature_collection,
                "inference_id": job.inference_id,
                "processing_time": time.time() - job.submitted_time,
                "poll_count": job.poll_count,
            }

            # Add to result queue
            self.result_queue.put(result)

            # Cleanup S3 objects if configured
            if self.config.cleanup_enabled:
                S3_MANAGER.cleanup_s3_objects([job.input_s3_uri, output_location])

            if self.metrics_tracker:
                self.metrics_tracker.increment_counter("JobCompletions")
                processing_time = time.time() - job.submitted_time
                self.metrics_tracker.set_counter("JobProcessingTime", int(processing_time))

            # Update tile status to COMPLETED
            if self.tile_table and job.tile_info.get("tile_id") and job.tile_info.get("job_id"):
                try:
                    self.tile_table.update_tile_status(
                        job.tile_info["tile_id"], 
                        job.tile_info["job_id"], 
                        "COMPLETED"
                    )
                except Exception as e:
                    logger.warning(f"Failed to update tile status to COMPLETED: {e}")

            logger.debug(f"AsyncPollingWorker-{self.worker_id} completed job: {job.inference_id}")

        except Exception as e:
            logger.error(f"AsyncPollingWorker-{self.worker_id} error processing completed job {job.inference_id}: {e}")
            self._handle_failed_job(job, f"Result processing error: {e}")

    def _handle_failed_job(self, job: AsyncInferenceJob, reason: str) -> None:
        """
        Handle a failed job by logging and cleaning up resources.

        :param job: Failed AsyncInferenceJob
        :param reason: Reason for failure
        """
        logger.error(f"AsyncPollingWorker-{self.worker_id} job {job.inference_id} failed: {reason}")

        # Update tile status to FAILED
        if self.tile_table and job.tile_info.get("tile_id") and job.tile_info.get("job_id"):
            try:
                self.tile_table.update_tile_status(
                    job.tile_info["tile_id"], 
                    job.tile_info["job_id"], 
                    "FAILED",
                    reason
                )
            except Exception as e:
                logger.warning(f"Failed to update tile status to FAILED: {e}")

        assert isinstance(self.feature_detector, AsyncSMDetector)

        # Cleanup S3 objects if configured
        if self.config.cleanup_enabled:
            try:
                S3_MANAGER.cleanup_s3_objects([job.input_s3_uri])
            except Exception as cleanup_error:
                logger.warning(f"Failed to cleanup S3 objects for failed job {job.inference_id}: {cleanup_error}")

        if self.metrics_tracker:
            self.metrics_tracker.increment_counter("JobFailures")

    def stop(self) -> None:
        """Signal the worker to stop processing."""
        self.running = False


class AsyncTileWorkerPool:
    """
    Optimized worker pool for async tile processing with separate submission and polling workers.

    This pool coordinates submission workers that quickly submit tiles to async endpoints
    and polling workers that independently monitor job completion, maximizing throughput
    and resource utilization.
    """

    @metric_scope
    def __init__(self, region_request, sensor_model, elevation_model, model_invocation_credentials, metrics: MetricsLogger, tile_table: Optional[TileRequestTable] = None):
        """
        Initialize AsyncTileWorkerPool.

        :param region_request: Region request configuration
        :param sensor_model: Sensor model for geolocating features
        :param elevation_model: Elevation model for geolocating features
        :param model_invocation_credentials: Credentials for model invocation
        :param metrics: MetricsLogger for tracking performance
        :param tile_table: Optional TileRequestTable for tracking tile status
        """

        self.region_request = region_request
        self.sensor_model = sensor_model
        self.elevation_model = elevation_model
        self.model_invocation_credentials = model_invocation_credentials
        self.tile_table = tile_table

        # Create async endpoint configuration
        self.config = AsyncServiceConfig.async_endpoint_config

        self.metrics_tracker = AsyncMetricsTracker(metrics_logger=metrics)

        # Worker queues
        self.job_queue = Queue(maxsize=self.config.max_concurrent_jobs)  # tile_queue
        self.result_queue = Queue()

        # Worker lists
        self.submission_workers: List[AsyncSubmissionWorker] = []
        self.polling_workers: List[AsyncPollingWorker] = []

        # Resource management for worker threads
        # self.resource_manager = ResourceManager(self.config)

        logger.debug(
            f"AsyncTileWorkerPool initialized with {self.config.submission_workers} submission workers "
            f"and {self.config.polling_workers} polling workers"
        )

    def process_tiles_async(self, tile_queue: Queue) -> Tuple[int, int]:
        """
        Process tiles using optimized async worker pool.

        :param tile_queue: Queue containing tiles to process
        :return: Tuple of (total_tiles_processed, failed_tiles)
        """
        logger.info("Starting async tile processing with optimized worker pool")

        try:
            # Start workers
            self._start_workers(tile_queue)

            # Wait for all tiles to be submitted
            tile_queue.join()
            logger.debug("All tiles submitted to async endpoints")

            # Wait for all jobs to complete
            total_tiles, failed_tiles = self._wait_for_completion()

            logger.info(f"Async tile processing completed. Total: {total_tiles}, Failed: {failed_tiles}")
            return total_tiles, failed_tiles

        finally:
            # Ensure workers are stopped
            self._stop_workers()

    def _start_workers(self, tile_queue: Queue) -> None:
        """
        Start submission and polling workers.

        :param tile_queue: Queue containing tiles to process
        """
        logger.debug("Starting async worker pool")

        # Start submission workers
        for i in range(self.config.submission_workers):
            # Ignoring mypy error - if model_name was None the call to validate the region
            # request at the start of this function would have failed
            feature_detector = EnhancedFeatureDetectorFactory(
                endpoint=self.region_request.model_name,
                endpoint_mode=self.region_request.model_invoke_mode,
                assumed_credentials=self.model_invocation_credentials,
            ).build()

            logger.info(f"Got feature detector: {feature_detector}")

            if feature_detector is None:
                logger.error("Failed to create feature detector")
                return None

            worker = AsyncSubmissionWorker(
                worker_id=i,
                tile_queue=tile_queue,
                job_queue=self.job_queue,
                feature_detector=feature_detector,
                config=self.config,
                metrics_tracker=self.metrics_tracker,
                tile_table=self.tile_table,
            )
            logger.debug(f"Starting worker thread: {worker}")
            worker.start()
            logger.debug(f"Started worker thread: {worker}")
            self.submission_workers.append(worker)

            # Register worker thread for managed cleanup
            # logger.debug(f"Registering worker thread: {worker}")
            # self.resource_manager.register_worker_thread(worker)

        # Start polling workers
        for i in range(self.config.polling_workers):

            # Set up our feature table to work with the region quest
            feature_table = FeatureTable(
                EnhancedServiceConfig.feature_table,
                self.region_request.tile_size,
                self.region_request.tile_overlap,
            )

            # Set up our feature table to work with the region quest
            region_request_table = RegionRequestTable(EnhancedServiceConfig.region_request_table)

            # Ignoring mypy error - if model_name was None the call to validate the region
            # request at the start of this function would have failed
            feature_detector = EnhancedFeatureDetectorFactory(
                endpoint=self.region_request.model_name,
                endpoint_mode=self.region_request.model_invoke_mode,
                assumed_credentials=self.model_invocation_credentials,
            ).build()

            if feature_detector is None:
                logger.error("Failed to create feature detector")
                return None

            # Set up geolocator
            geolocator = None
            if self.sensor_model is not None:
                geolocator = Geolocator(
                    ImagedFeaturePropertyAccessor(), self.sensor_model, elevation_model=self.elevation_model
                )
            worker = AsyncPollingWorker(
                worker_id=i,
                feature_table=feature_table,
                geolocator=geolocator,
                region_request_table=region_request_table,
                in_queue=self.job_queue,
                result_queue=self.result_queue,
                feature_detector=feature_detector,
                config=self.config,
                metrics_tracker=self.metrics_tracker,
                tile_table=self.tile_table,
            )
            logger.info("Created poller worker")
            worker.start()
            logger.info("poller worker started")
            self.polling_workers.append(worker)

            # # Register worker thread for managed cleanup
            # self.resource_manager.register_worker_thread(worker)

        logger.info(
            f"Started {len(self.submission_workers)} submission workers " f"and {len(self.polling_workers)} polling workers"
        )

    def _wait_for_completion(self) -> Tuple[int, int]:
        """
        Wait for all jobs to complete and collect results.

        :return: Tuple of (total_tiles_processed, failed_tiles)
        """
        logger.debug("Waiting for async job completion")

        # Calculate expected number of jobs
        total_submitted = sum(worker.processed_tile_count for worker in self.submission_workers)
        total_failed_submissions = sum(worker.failed_tile_count for worker in self.submission_workers)

        logger.debug(f"Waiting for {total_submitted} submitted jobs to complete")

        # Wait for all jobs to be processed
        completed_jobs = 0
        failed_jobs = 0

        # Wait for results with timeout
        timeout_time = time.time() + self.config.max_wait_time

        while completed_jobs + failed_jobs < total_submitted and time.time() < timeout_time:
            try:
                # Check for completed results
                result = self.result_queue.get(timeout=5.0)
                completed_jobs += 1

                # Process result (store features, update database, etc.)
                self._process_result(result)

            except Empty:
                # Check if polling workers are still active
                active_polling_workers = sum(1 for worker in self.polling_workers if worker.is_alive())
                if active_polling_workers == 0:
                    logger.warning("No active polling workers remaining")
                    break
                continue

        # Count any remaining failed jobs from polling workers
        failed_jobs += sum(worker.failed_tile_count for worker in self.polling_workers)

        total_processed = completed_jobs + failed_jobs + total_failed_submissions
        total_failed = failed_jobs + total_failed_submissions

        logger.debug(f"Job completion summary: Processed={total_processed}, Failed={total_failed}")

        return total_processed, total_failed

    def _process_result(self, result: Dict[str, Any]) -> None:
        """
        Process a completed result.

        :param result: Result dictionary from polling worker
        """
        # This would integrate with the existing tile processing pipeline
        # For now, just log the result
        tile_info = result["tile_info"]
        feature_count = len(result["feature_collection"].get("features", []))
        processing_time = result["processing_time"]

        logger.debug(
            f"Processed tile {tile_info.get('region')} with {feature_count} features "
            f"in {processing_time:.2f}s after {result['poll_count']} polls"
        )

        if self.metrics_tracker:
            self.metrics_tracker.increment_counter("TileCompletions")
            self.metrics_tracker.set_counter("FeaturesDetected", feature_count)

    def _stop_workers(self) -> None:
        """Stop all workers gracefully."""
        logger.debug("Stopping async worker pool")

        # Signal workers to stop
        for worker in self.submission_workers:
            worker.stop()

        for worker in self.polling_workers:
            worker.stop()

        # Use resource manager to clean up worker threads
        # try:
        #     self.resource_manager.cleanup_all_resources(ResourceType.WORKER_THREAD, force=True)
        # except Exception as e:
        #     logger.warning(f"Error during worker thread cleanup: {e}")

        # Wait for workers to finish as backup
        for worker in self.submission_workers:
            worker.join(timeout=5.0)
            if worker.is_alive():
                logger.warning(f"Submission worker {worker.worker_id} did not stop gracefully")

        for worker in self.polling_workers:
            worker.join(timeout=5.0)
            if worker.is_alive():
                logger.warning(f"Polling worker {worker.worker_id} did not stop gracefully")

        # # Stop resource manager cleanup worker
        # self.resource_manager.stop_cleanup_worker()

        logger.debug("Async worker pool stopped")

    def get_worker_stats(self) -> Dict[str, Any]:
        """
        Get statistics about worker performance.

        :return: Dictionary of worker statistics
        """
        submission_stats = {
            "total_processed": sum(worker.processed_tile_count for worker in self.submission_workers),
            "total_failed": sum(worker.failed_tile_count for worker in self.submission_workers),
            "workers": len(self.submission_workers),
        }

        polling_stats = {
            "total_completed": sum(worker.completed_job_count for worker in self.polling_workers),
            "total_failed": sum(worker.failed_tile_count for worker in self.polling_workers),
            "active_jobs": sum(len(worker.active_jobs) for worker in self.polling_workers),
            "workers": len(self.polling_workers),
        }

        return {
            "submission_workers": submission_stats,
            "polling_workers": polling_stats,
            "job_queue_size": self.job_queue.qsize(),
            "result_queue_size": self.result_queue.qsize(),
        }
