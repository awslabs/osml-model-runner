# Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

"""
Locust user that makes random image processing requests.
"""

import logging
import random

import boto3
from _load_utils import split_s3_path
from botocore.exceptions import ClientError
from load_context import get_load_test_context
from locust import task
from locust.exception import StopUser
from model_runner_user import ModelRunnerUser

logger = logging.getLogger(__name__)


class RandomRequestUser(ModelRunnerUser):
    """
    Selects random images and SageMaker endpoints and submits requests.
    """

    ALLOWED_IMAGE_TYPES = ["ntf", "nitf", "tiff", "tif"]

    def __init__(self, environment):
        super().__init__(environment)
        self.sm_client = boto3.client("sagemaker")

    def on_start(self) -> None:
        """
        Initialize shared discovery data and validate that this user can run.

        This caches the discovered images/endpoints on the shared load-test context so
        we don't perform expensive AWS discovery once per simulated user.

        :returns: None
        :raises StopUser: If no endpoints or no images can be discovered.
        """
        ctx = get_load_test_context(self.environment)
        with ctx.lock:
            if ctx.random_request_endpoints is None:
                ctx.random_request_endpoints = self._find_test_endpoints()
            if ctx.random_request_images is None:
                ctx.random_request_images = self._find_test_images()

        self.endpoint_names = ctx.random_request_endpoints or []
        self.image_urls = ctx.random_request_images or []
        if not self.endpoint_names:
            logger.error("No SageMaker endpoints found (cannot run RandomRequestUser).")
            raise StopUser()
        if not self.image_urls:
            logger.error(
                "No images found under %s (extensions: %s).",
                self.environment.parsed_options.test_imagery_location,
                ", ".join(f".{ext}" for ext in self.ALLOWED_IMAGE_TYPES),
            )
            raise StopUser()

    @task
    def process_random_image(self):
        """
        Submit a job for a randomly selected image and endpoint.

        :returns: None
        """
        selected_endpoint = random.choice(self.endpoint_names)
        selected_image = random.choice(self.image_urls)

        image_processing_request = self._build_image_processing_request(
            endpoint=selected_endpoint,
            endpoint_type="SM_ENDPOINT",
            image_url=selected_image,
            result_url=self.environment.parsed_options.test_results_location,
        )
        self.client.process_image(image_processing_request)

    def _find_test_images(self) -> list[str]:
        """
        List candidate imagery under `--test-imagery-location`.

        :returns: List of S3 URLs for images matching allowed extensions.
        """
        bucket, prefix = split_s3_path(self.environment.parsed_options.test_imagery_location)

        s3_client = boto3.client("s3")
        image_urls: list[str] = []

        try:
            paginator = s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if any(key.lower().endswith(f".{ext}") for ext in self.ALLOWED_IMAGE_TYPES):
                        image_urls.append(f"s3://{bucket}/{key}")
            logger.info("Found %s test images in %s/%s", len(image_urls), bucket, prefix)
            return image_urls
        except ClientError as e:
            logger.error("Error listing images from S3 bucket %s: %s", bucket, e)
            return []

    def _find_test_endpoints(self) -> list[str]:
        """
        Discover SageMaker endpoint names.

        :returns: List of endpoint names (may be empty on error).
        """
        try:
            paginator = self.sm_client.get_paginator("list_endpoints")
            endpoint_names = [
                endpoint["EndpointName"] for page in paginator.paginate() for endpoint in page.get("Endpoints", [])
            ]
            logger.info("Found %s SageMaker endpoints", len(endpoint_names))
            return endpoint_names
        except ClientError as e:
            logger.error("Error listing SageMaker endpoints: %s", e)
            return []
        except Exception as e:
            logger.error("Unexpected error listing endpoints: %s", e, exc_info=True)
            return []
