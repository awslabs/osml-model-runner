#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
Locust user class for OSML Model Runner load tests.

This module provides a Locust User implementation that submits image processing
requests to the Model Runner service.
"""

import json
import logging
import random
import time
from datetime import datetime
from secrets import token_hex
from test.config import LoadTestConfig
from test.load.locust_job_tracker import get_job_tracker
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError, ParamValidationError
from locust import User, task
from osgeo import gdal

logger = logging.getLogger(__name__)


class ModelRunnerLoadTestUser(User):
    """
    Locust user that submits image processing requests to Model Runner.

    Each user instance cycles through available images and submits them
    for processing.
    """

    def __init__(self, environment):
        """
        Initialize the load test user.

        :param environment: Locust environment containing test configuration
        """
        super().__init__(environment)
        self.config = LoadTestConfig()
        self.sqs_resource = boto3.resource("sqs")
        self.s3_client = boto3.client("s3")
        self.images: List[Dict[str, Any]] = []
        self.image_index = 0
        self.job_tracker = get_job_tracker()
        # Get configuration from parsed options or environment
        result_bucket = (
            getattr(self.environment.parsed_options, "result_bucket", None) or self.config.S3_LOAD_TEST_RESULT_BUCKET
        )
        self.result_bucket = self._normalize_bucket_name(result_bucket) if result_bucket else ""

        self.model_name = getattr(self.environment.parsed_options, "model_name", None) or self.config.SM_LOAD_TEST_MODEL

        source_bucket = (
            getattr(self.environment.parsed_options, "source_bucket", None) or self.config.S3_LOAD_TEST_SOURCE_IMAGE_BUCKET
        )
        self.source_bucket = self._normalize_bucket_name(source_bucket) if source_bucket else ""

    def on_start(self):
        """Initialize user by loading available images."""
        self.images = self._get_s3_images(self.source_bucket)
        if not self.images:
            logger.error(f"No images found in bucket: {self.source_bucket}")
            self.environment.runner.quit()
        logger.info(f"User initialized with {len(self.images)} images")

    def _normalize_bucket_name(self, bucket_name: str) -> str:
        """Normalize S3 bucket name by removing s3:// prefix and trailing slashes."""
        if bucket_name.startswith("s3://"):
            bucket_name = bucket_name[5:]
        return bucket_name.rstrip("/")

    def _get_s3_images(self, bucket_name: str) -> List[Dict[str, Any]]:
        """
        Get all S3 images within the bucket.

        :param bucket_name: Name of source bucket
        :return: List of image dictionaries with 'image_name' and 'image_size'
        """
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket_name)
            images_list = []
            images_suffixes = (".ntf", ".nitf", ".tif", ".tiff", ".png", ".jpg", ".jpeg")

            for image in response.get("Contents", []):
                image_name = f"s3://{bucket_name}/{image['Key']}"
                image_size = image["Size"]

                if image_name.lower().endswith(images_suffixes):
                    images_list.append({"image_name": image_name, "image_size": image_size})

            return images_list

        except ClientError as error:
            logger.error(f"Error accessing bucket: {bucket_name}: {error}")
            return []

    def wait_time(self):
        """
        Wait time between tasks.

        :return: Random wait time between 1-3 seconds
        """
        return random.uniform(1, 3)

    @task
    def submit_image_request(self):
        """
        Submit an image processing request.

        This is the main task that Locust users will execute repeatedly.
        """

        if not self.images:
            logger.warning("No images available")
            return

        # Select next image (cycle through list)
        image_info = self.images[self.image_index % len(self.images)]
        self.image_index += 1

        image_url = image_info["image_name"]
        image_size = image_info["image_size"]

        # Build request
        image_processing_request = self._build_image_processing_request(
            endpoint=self.model_name,
            image_url=image_url,
            result_bucket=self.result_bucket,
        )

        # Submit request and track metrics
        start_time = time.perf_counter()
        try:
            message_id = self._queue_image_processing_job(image_processing_request)
            job_id = image_processing_request["jobId"]

            # Get image metadata for tracking
            try:
                gdal_info = gdal.Open(image_url.replace("s3:/", "/vsis3", 1))
                pixels = gdal_info.RasterXSize * gdal_info.RasterYSize if gdal_info else 0
            except Exception:
                pixels = 0

            # Register job with tracker
            image_id = f"{job_id}:{image_url}"
            start_time_str = datetime.now().strftime("%m/%d/%Y/%H:%M:%S")
            self.job_tracker.register_job(
                image_id=image_id,
                job_id=job_id,
                image_url=image_url,
                message_id=message_id or "",
                size=image_size,
                pixels=pixels,
                start_time=start_time_str,
            )

            # Fire Locust event for metrics
            response_time = (time.perf_counter() - start_time) * 1000
            self.environment.events.request.fire(
                request_type="Submit Image",
                name=f"{image_url.split('/')[-1]}:{self.model_name}",
                response_time=response_time,
                response_length=image_size,
                exception=None,
            )

            logger.debug(f"Submitted job {job_id[:16]}... for {image_url}")

        except Exception as e:
            response_time = (time.perf_counter() - start_time) * 1000
            self.environment.events.request.fire(
                request_type="Submit Image",
                name=f"{image_url.split('/')[-1]}:{self.model_name}",
                response_time=response_time,
                response_length=0,
                exception=e,
            )
            logger.error(f"Failed to submit image request: {e}")

    def _build_image_processing_request(self, endpoint: str, image_url: str, result_bucket: str) -> Dict[str, Any]:
        """
        Build an image processing request for submission to ModelRunner.

        :param endpoint: Model endpoint name
        :param image_url: URL of image to process
        :param result_bucket: S3 bucket name for results
        :returns: Complete image processing request dictionary
        """
        job_id = token_hex(16)
        job_name = f"load-test-{job_id}"

        result_stream = f"{self.config.KINESIS_RESULTS_STREAM_PREFIX}-{self.config.ACCOUNT}"

        request: Dict[str, Any] = {
            "jobName": job_name,
            "jobId": job_id,
            "imageUrls": [image_url],
            "outputs": [
                {"type": "S3", "bucket": result_bucket, "prefix": f"{job_name}/"},
                {"type": "Kinesis", "stream": result_stream, "batchSize": 1000},
            ],
            "imageProcessor": {"name": endpoint, "type": "SM_ENDPOINT"},
            "imageProcessorTileSize": 512,
            "imageProcessorTileOverlap": 128,
            "imageProcessorTileFormat": "GTIFF",
            "imageProcessorTileCompression": "NONE",
            "postProcessing": [
                {"step": "FEATURE_DISTILLATION", "algorithm": {"algorithmType": "NMS", "iouThreshold": 0.75}}
            ],
        }

        return request

    def _queue_image_processing_job(self, image_processing_request: Dict[str, Any]) -> Optional[str]:
        """
        Submit an image processing request to the SQS queue.

        :param image_processing_request: The request to submit
        :returns: Message ID of the queued message
        :raises ClientError: If SQS operation fails
        :raises ParamValidationError: If request validation fails
        """
        try:
            queue = self.sqs_resource.get_queue_by_name(
                QueueName=self.config.IMAGE_QUEUE_NAME,
                QueueOwnerAWSAccountId=self.config.ACCOUNT,
            )
            response = queue.send_message(MessageBody=json.dumps(image_processing_request))
            return response.get("MessageId")
        except ClientError as error:
            logger.error(f"Unable to send job request to SQS queue: {self.config.IMAGE_QUEUE_NAME}")
            logger.error(f"{error}")
            raise
        except ParamValidationError as error:
            logger.error("Invalid SQS API request; validation failed")
            logger.error(f"{error}")
            raise
