#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
Base Locust user class for Model Runner load tests.

This module provides a base class for Locust users that interact with the model runner,
including functionality for making image processing requests and checking status.
"""

import json
import logging
import time
from enum import Enum
from secrets import token_hex
from test.load.locust_job_tracker import get_job_tracker
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError, ParamValidationError
from locust import User

logger = logging.getLogger(__name__)


class ImageRequestStatus(str, Enum):
    """Enumeration of possible image request processing statuses."""

    STARTED = "STARTED"
    IN_PROGRESS = "IN_PROGRESS"
    SUCCESS = "SUCCESS"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"

    def __str__(self) -> str:
        """
        Convert enum value to string.

        :return: String representation of the status
        """
        return self.value


class ModelRunnerUser(User):
    """
    Base class for Locust users that interact with the model runner.

    Provides common functionality for making image processing requests and checking status.
    This class waits for job completion, unlike ModelRunnerLoadTestUser which only submits jobs.
    """

    abstract = True  # This prevents Locust from creating this user directly

    DEFAULT_TILE_SIZE = 4096
    DEFAULT_TILE_OVERLAP = 50
    DEFAULT_TILE_FORMAT = "NITF"
    DEFAULT_TILE_COMPRESSION = "NONE"
    DEFAULT_POST_PROCESSING = (
        '[{"step": "FEATURE_DISTILLATION", "algorithm": {"algorithmType": "NMS", "iouThreshold":  0.75}}]'
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
        Build an image processing request.

        :param endpoint: Name of the processing endpoint
        :param endpoint_type: Type of endpoint (e.g., 'SM_ENDPOINT', 'DETECT', 'CLASSIFY')
        :param image_url: S3 URL of the input image
        :param result_url: S3 URL for storing results
        :param tile_size: Size of image tiles in pixels
        :param tile_overlap: Overlap between tiles in pixels
        :param tile_format: Format for image tiles (e.g., 'NITF')
        :param tile_compression: Compression type for tiles
        :param post_processing: JSON string defining post-processing steps
        :param region_of_interest: Optional GeoJSON defining region to process
        :param feature_properties: Optional list of feature property specifications
        :return: Dictionary containing the complete image processing request
        """

        job_id = token_hex(16)
        job_name = f"test-{job_id}"

        # Parse S3 URL: s3://bucket/key -> (bucket, key)
        parsed = urlparse(result_url)
        result_bucket = parsed.netloc or result_url.replace("s3://", "").split("/")[0]
        result_prefix = parsed.path.lstrip("/")
        if result_prefix and not result_prefix.endswith("/"):
            result_prefix += "/"
        result_prefix += f"{job_name}/"

        if feature_properties is None:
            feature_properties = []

        image_processing_request: Dict[str, Any] = {
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

        return image_processing_request


class ModelRunnerClientException(Exception):
    """
    Exception raised for errors that occur during model runner client operations.

    Contains details about the specific request that failed.

    :param message: Error message
    :param status: ModelRunner job status if available
    :param response_body: Response body if available
    :param job_id: ModelRunner job ID if available
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

        # Build detailed error message
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
    Client for interacting with the model runner service.

    This implementation follows the design pattern setup by the Users implemented in
    the Locust package. The abstract base User class contains a reference to the client
    and any behavioral utility methods while the client itself encapsulates the protocol
    used to communicate with the service.
    """

    def __init__(self, environment):
        """
        Initialize the model runner client.

        :param environment: Locust environment containing test configuration
        """
        self.environment = environment
        self.sqs_service_resource = boto3.resource("sqs")
        self.max_retry_attempts = 3
        self.job_tracker = get_job_tracker()

    def process_image(self, image_processing_request: Dict[str, Any]) -> None:
        """
        Submit an image processing request and wait for completion.

        This is a convenience method that provides a synchronous call pattern for the
        ModelRunner API. It wraps the asynchronous calls to queue_image_processing_job
        and wait_for_image_complete that are non-blocking.

        :param image_processing_request: Dictionary containing the request parameters
        :raises ModelRunnerClientException: If request submission or processing fails
        """

        logger.debug(f"Starting ModelRunner image job for: {image_processing_request['imageUrls']}")
        logger.debug(
            f"Model: {image_processing_request['imageProcessor']['name']}, "
            f"Type:{image_processing_request['imageProcessor']['type']}"
        )

        job_id = image_processing_request["jobId"]
        image_url = image_processing_request["imageUrls"][0]
        image_id = f"{job_id}:{image_url}"

        # Register job with tracker (if not already registered)
        from datetime import datetime

        from osgeo import gdal

        try:
            gdal_info = gdal.Open(image_url.replace("s3:/", "/vsis3", 1))
            pixels = gdal_info.RasterXSize * gdal_info.RasterYSize if gdal_info else 0
        except Exception:
            pixels = 0

        # Try to get image size from S3
        try:
            s3_client = boto3.client("s3")
            # Parse S3 URL: s3://bucket/key -> (bucket, key)
            parsed = urlparse(image_url)
            bucket = parsed.netloc or image_url.replace("s3://", "").split("/")[0]
            key = parsed.path.lstrip("/")
            response = s3_client.head_object(Bucket=bucket, Key=key)
            image_size = response.get("ContentLength", 0)
        except Exception:
            image_size = 0

        start_time_str = datetime.now().strftime("%m/%d/%Y/%H:%M:%S")
        start_perf_counter = time.perf_counter()
        final_job_status = None
        processing_time = -1
        attempt_number = 0
        while attempt_number < self.max_retry_attempts:
            attempt_number += 1
            logger.info(f"Starting: {job_id} attempt {attempt_number}")
            queued_message_id = self.queue_image_processing_job(image_processing_request)
            logger.debug(f"SQS Message ID: {queued_message_id}")

            # Register job with tracker
            self.job_tracker.register_job(
                image_id=image_id,
                job_id=job_id,
                image_url=image_url,
                message_id=queued_message_id or "",
                size=image_size,
                pixels=pixels,
                start_time=start_time_str,
            )

            final_job_status, processing_time = self.wait_for_image_complete(job_id, image_id)
            if final_job_status != ImageRequestStatus.PARTIAL:
                break

        response_time = (time.perf_counter() - start_perf_counter) * 1000
        logger.info(f"Complete: {job_id} {final_job_status} - {processing_time}")

        exception = None
        if final_job_status != ImageRequestStatus.SUCCESS:
            exception = ModelRunnerClientException(
                "Job Unsuccessful",
                status=str(final_job_status) if final_job_status else None,
                response_body={"request": image_processing_request, "processing_time": processing_time},
                job_id=job_id,
            )

        self.environment.events.request.fire(
            request_type="Process Image",
            name=self._build_event_name(image_processing_request),
            exception=exception,
            response_time=response_time,
            response_length=0,
        )

    def _build_event_name(self, image_processing_request: Dict[str, Any]) -> str:
        """
        Build a locust event name for tracking request metrics.

        :param image_processing_request: Dictionary containing the request parameters
        :return: Event name string incorporating endpoint and request type
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
        Submit job to the processing queue.

        :param image_processing_request: Dictionary containing the request parameters
        :return: Message ID if successfully queued, None if failed
        :raises ModelRunnerClientException: If request submission fails
        """
        logger.debug(f"Sending request: jobId={image_processing_request['jobId']}")
        try:
            from test.config import LoadTestConfig

            config = LoadTestConfig()
            queue_name = getattr(self.environment.parsed_options, "mr_input_queue", None) or config.IMAGE_QUEUE_NAME
            account_id = getattr(self.environment.parsed_options, "aws_account", None) or config.ACCOUNT

            queue = self.sqs_service_resource.get_queue_by_name(
                QueueName=queue_name,
                QueueOwnerAWSAccountId=account_id,
            )
            response = queue.send_message(MessageBody=json.dumps(image_processing_request))

            message_id = response.get("MessageId")
            logger.debug(f"Message queued to SQS with messageId={message_id}")

            return message_id
        except ClientError as error:
            logger.error(f"Unable to send job request to SQS queue: {queue_name}")
            logger.error(f"{error}")
            raise error

        except ParamValidationError as error:
            logger.error("Invalid SQS API request; validation failed")
            logger.error(f"{error}")
            raise error

    def check_image_status(self, job_id: str, image_id: str) -> Tuple[Optional[str], Optional[int]]:
        """
        Check the current status of a processing job.

        Uses the job tracker to look up job status by image_id.

        :param job_id: ID of the job to check (for logging)
        :param image_id: Image ID (job_id:image_url) used as key in tracker
        :return: Tuple of (status, processing_duration) if found, (None, None) if not found
        """
        with self.job_tracker.lock:
            job_status = self.job_tracker.job_status_dict.get(image_id)
            if job_status:
                status_str = str(job_status.status) if job_status.status else None
                duration = int(job_status.processing_duration) if job_status.processing_duration else None
                return status_str, duration
        return None, None

    def wait_for_image_complete(
        self, job_id: str, image_id: str, retry_interval: int = 5, timeout: int = 15 * 60 * 60
    ) -> Tuple[Optional[str], Optional[int]]:
        """
        Wait for an image processing request to complete.

        Polls the job status until it reaches a terminal state or times out.

        :param job_id: ID of the job to monitor (for logging)
        :param image_id: Image ID (job_id:image_url) used as key in tracker
        :param retry_interval: Seconds to wait between status checks
        :param timeout: Maximum total seconds to wait
        :return: Tuple of (final_status, processing_duration)
        :raises RuntimeError: If job times out or enters invalid state
        """

        job_status, processing_duration = (None, None)
        total_wait_time = 0
        while total_wait_time < timeout:
            job_status, processing_duration = self.check_image_status(job_id, image_id)
            if job_status in [
                ImageRequestStatus.SUCCESS,
                ImageRequestStatus.FAILED,
                ImageRequestStatus.PARTIAL,
            ]:
                return job_status, processing_duration
            time.sleep(retry_interval)
            total_wait_time += retry_interval

        return job_status, processing_duration
