
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
        # self.poller = AsyncInferencePoller(self.sm_client, self.async_config)

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
                logger.debug(f"Poller got job: {job}")
                self.active_jobs[job.inference_id] = job
                logger.debug(f"AsyncPollingWorker-{self.worker_id} monitoring job: {job.inference_id}")

            except Empty:
                break  # No more jobs available

    def _poll_active_jobs(self) -> None:
        """Poll all active jobs for completion."""
        completed_jobs = []

        for inference_id, job in self.active_jobs.items():
            try:
                logger.debug(f"poller checking for {inference_id}, {job}")
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

                logger.debug(f"AsyncPollingWorker-{self.worker_id} polled job {inference_id}: {job_status}")

                if job_status == "COMPLETED":
                    # Get output location from job's output_s3_uri
                    logger.info(f"AsyncPollingWorker-{self.worker_id} polled job {inference_id}: {job_status}. job: {job}")

                    output_location = getattr(job, "output_s3_uri", None)
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
                    self.tile_table.update_tile_status(job.tile_info["tile_id"], job.tile_info["job_id"], "COMPLETED")
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
                self.tile_table.update_tile_status(job.tile_info["tile_id"], job.tile_info["job_id"], "FAILED", reason)
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

