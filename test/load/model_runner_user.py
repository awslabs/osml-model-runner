# Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

"""
Model Runner Locust user base classes.

This file uses flat imports (no package structure) so the directory can be passed
directly to Locust via `-f ./test/load`.
"""

import json
import logging
import time
from enum import Enum
from secrets import token_hex
from typing import Any, Dict, Optional, Tuple

import boto3
from _load_utils import split_s3_path
from botocore.exceptions import ClientError, ParamValidationError
from job_tracker import get_job_tracker
from locust import User
from locust_setup import get_shared_status_monitor

logger = logging.getLogger(__name__)


class ImageRequestStatus(str, Enum):
    """
    Job status values emitted by the Model Runner pipeline.
    """

    STARTED = "STARTED"
    IN_PROGRESS = "IN_PROGRESS"
    SUCCESS = "SUCCESS"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"

    def __str__(self) -> str:
        return self.value


class ModelRunnerUser(User):
    """
    Base class for Locust users that interact with Model Runner.

    This class provides synchronous semantics (submit + wait).
    """

    abstract = True
    # Locust UI expects a host value for some workflows; our tests don't use HTTP,
    # so set a harmless default to avoid the UI warning/prompt.
    host = "http://localhost"

    DEFAULT_TILE_SIZE = 512
    DEFAULT_TILE_OVERLAP = 128
    DEFAULT_TILE_FORMAT = "GTIFF"
    DEFAULT_TILE_COMPRESSION = "NONE"
    DEFAULT_POST_PROCESSING = (
        '[{"step": "FEATURE_DISTILLATION", "algorithm": {"algorithmType": "NMS", "iouThreshold":  0.1}}]'
    )

    def __init__(self, environment):
        super().__init__(environment)
        self.client = ModelRunnerClient(environment=environment)

    def _build_image_processing_request(
        self,
        endpoint: str,
        endpoint_type: str,
        image_url: str,
        result_url: str,
        tile_size: int = DEFAULT_TILE_SIZE,
        tile_overlap: int = DEFAULT_TILE_OVERLAP,
        tile_format: str = DEFAULT_TILE_FORMAT,
        tile_compression: str = DEFAULT_TILE_COMPRESSION,
        post_processing: str = DEFAULT_POST_PROCESSING,
        region_of_interest: Optional[str] = None,
        feature_properties: Optional[list[dict[str, Any]]] = None,
    ) -> dict[str, Any]:
        """
        Build a Model Runner `ProcessImage` request payload.

        :param endpoint: Image processor identifier (e.g. SageMaker endpoint name).
        :param endpoint_type: Image processor type (e.g. `SM_ENDPOINT`).
        :param image_url: Input image URL.
        :param result_url: Base S3 URL where results should be written.
        :param tile_size: Tile size to use for processing.
        :param tile_overlap: Tile overlap to use for processing.
        :param tile_format: Tile output format.
        :param tile_compression: Tile compression setting.
        :param post_processing: JSON string for post-processing pipeline configuration.
        :param region_of_interest: Optional region-of-interest configuration.
        :param feature_properties: Optional list of feature property configurations.
        :returns: Request payload dictionary.
        """
        job_id = token_hex(16)
        job_name = f"test-{job_id}"

        result_bucket, result_prefix = split_s3_path(result_url)
        if result_prefix and not result_prefix.endswith("/"):
            result_prefix += "/"
        result_prefix += f"{job_name}/"

        if feature_properties is None:
            feature_properties = []

        return {
            "jobName": job_name,
            "jobId": job_id,
            "imageUrls": [image_url],
            "outputs": [{"type": "S3", "bucket": result_bucket, "prefix": result_prefix}],
            "imageProcessor": {"name": endpoint, "type": endpoint_type},
            "imageProcessorTileSize": tile_size,
            "imageProcessorTileOverlap": tile_overlap,
            "imageProcessorTileFormat": tile_format,
            "imageProcessorTileCompression": tile_compression,
            "postProcessing": json.loads(post_processing),
            "regionOfInterest": region_of_interest,
            "featureProperties": feature_properties,
        }


class ModelRunnerClientException(Exception):
    """
    Exception raised for unsuccessful Model Runner jobs.
    """

    def __init__(
        self,
        message: str,
        status: Optional[str] = None,
        response_body: Optional[Dict[str, Any]] = None,
        job_id: Optional[str] = None,
    ) -> None:
        self.message = message
        self.status = status
        self.response_body = response_body
        self.job_id = job_id

        detailed_message = [message]
        if status:
            detailed_message.append(f"Status: {status}")
        if job_id:
            detailed_message.append(f"Request ID: {job_id}")
        if response_body:
            detailed_message.append(f"Response: {json.dumps(response_body, indent=2)}")

        super().__init__("\n".join(detailed_message))


class ModelRunnerClient:
    """
    Client for interacting with Model Runner via SQS + status polling.
    """

    def __init__(self, environment):
        self.environment = environment
        self.sqs_resource = boto3.resource("sqs")
        self.max_retry_attempts = 3

    def process_image(self, image_processing_request: Dict[str, Any]) -> None:
        """
        Submit a processing job and wait for a terminal status.

        This emits a Locust request event named `Process Image` once the job is terminal.

        :param image_processing_request: Request payload to submit.
        :returns: None
        """
        job_id = image_processing_request["jobId"]
        image_url = ""
        try:
            image_url = image_processing_request.get("imageUrls", [""])[0]
        except Exception:
            image_url = ""

        # Register job for later job_status/job_summary output.
        tracker = getattr(self.environment, "osml_job_tracker", None) or get_job_tracker()
        tracker.register_job(job_id=job_id, image_url=image_url)

        start_perf_counter = time.perf_counter()

        final_job_status = None
        processing_time = None

        for attempt_number in range(1, self.max_retry_attempts + 1):
            logger.info("Starting: %s attempt %s", job_id, attempt_number)
            self.queue_image_processing_job(image_processing_request)
            final_job_status, processing_time = self.wait_for_image_complete(job_id)
            if final_job_status != ImageRequestStatus.PARTIAL:
                break

        response_time_ms = (time.perf_counter() - start_perf_counter) * 1000

        exception = None
        if final_job_status != ImageRequestStatus.SUCCESS:
            exception = ModelRunnerClientException(
                "Job Unsuccessful",
                status=str(final_job_status) if final_job_status else None,
                response_body={"request": image_processing_request, "processing_time": processing_time},
                job_id=job_id,
            )
        # Update tracker with final status (even if unsuccessful).
        tracker.complete_job(
            job_id=job_id,
            status=str(final_job_status) if final_job_status else "UNKNOWN",
            processing_duration_s=processing_time,
        )

        self.environment.events.request.fire(
            request_type="Process Image",
            name=self._build_event_name(image_processing_request),
            exception=exception,
            response_time=response_time_ms,
            response_length=0,
        )

    def _build_event_name(self, image_processing_request: Dict[str, Any]) -> str:
        """
        Build a human-readable Locust event name for the request.

        :param image_processing_request: Request payload.
        :returns: Event name string.
        """
        try:
            image_url = image_processing_request["imageUrls"][0]
            filename = image_url.split("/")[-1]
            model_name = image_processing_request["imageProcessor"]["name"]
            return f"{filename}:{model_name}"
        except (KeyError, IndexError):
            return "invalid_request"

    def queue_image_processing_job(self, image_processing_request: Dict[str, Any]) -> Optional[str]:
        """
        Send the job request to the configured Model Runner input queue.

        :param image_processing_request: Request payload to submit.
        :returns: SQS `MessageId` if available.
        """
        try:
            queue = self.sqs_resource.get_queue_by_name(
                QueueName=self.environment.parsed_options.mr_input_queue,
                QueueOwnerAWSAccountId=self.environment.parsed_options.aws_account,
            )
            response = queue.send_message(MessageBody=json.dumps(image_processing_request))
            return response.get("MessageId")
        except ClientError as error:
            logger.error(
                "Unable to send job request to SQS queue: %s",
                self.environment.parsed_options.mr_input_queue,
            )
            logger.error("%s", error)
            raise
        except ParamValidationError as error:
            logger.error("Invalid SQS API request; validation failed")
            logger.error("%s", error)
            raise

    def check_image_status(self, job_id: str) -> Tuple[Optional[str], Optional[int]]:
        """
        Check the current status for a job.

        :param job_id: Job identifier.
        :returns: Tuple of `(status, processing_duration)`; values may be `None`.
        """
        # Prefer environment-attached monitor (robust even if locust_setup is imported multiple times).
        monitor = getattr(self.environment, "osml_status_monitor", None) or get_shared_status_monitor()
        if monitor is None:
            return None, None
        return monitor.check_job_status(job_id)

    def wait_for_image_complete(
        self,
        job_id: str,
        retry_interval: int = 5,
        timeout: int = 15 * 60 * 60,
    ) -> Tuple[Optional[str], Optional[int]]:
        """
        Wait until a job reaches a terminal status or times out.

        :param job_id: Job identifier.
        :param retry_interval: Poll interval in seconds.
        :param timeout: Maximum time to wait in seconds.
        :returns: Tuple of `(status, processing_duration)` as last observed.
        """
        job_status, processing_duration = (None, None)
        total_wait_time = 0
        while total_wait_time < timeout:
            job_status, processing_duration = self.check_image_status(job_id)
            if job_status in [ImageRequestStatus.SUCCESS, ImageRequestStatus.FAILED, ImageRequestStatus.PARTIAL]:
                return job_status, processing_duration
            time.sleep(retry_interval)
            total_wait_time += retry_interval

        return job_status, processing_duration
