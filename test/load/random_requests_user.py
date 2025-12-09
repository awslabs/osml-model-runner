#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
Locust user that makes random image processing requests.

Selects random images and endpoints from configured S3 buckets.
"""

import logging
import random
from test.load.model_runner_user import ModelRunnerUser
from urllib.parse import urlparse

import boto3
from locust import task

logger = logging.getLogger(__name__)


class RandomRequestUser(ModelRunnerUser):
    """
    Locust user that makes random image processing requests.

    Selects random images and endpoints from configured S3 buckets.
    """

    ALLOWED_IMAGE_TYPES = ["ntf", "nitf", "tiff", "tif"]

    def __init__(self, environment):
        """
        Initialize the random request user.

        :param environment: Locust environment containing test configuration
        """
        super().__init__(environment)
        self.sm_client = boto3.client("sagemaker")

    def on_start(self) -> None:
        """
        Set up the test user before starting requests.

        Loads available test images and endpoints from S3.
        """
        self.endpoint_names = self._find_test_endpoints()
        self.image_urls = self._find_test_images()

    @task
    def process_random_image(self):
        """
        Process a random image with a random endpoint.

        Selects a random image and endpoint combination and submits for processing.
        """
        if not self.endpoint_names:
            logger.error("No endpoints found")
            return

        if not self.image_urls:
            logger.error("No images found")
            return

        selected_endpoint = random.choice(self.endpoint_names)
        selected_image = random.choice(self.image_urls)

        test_results_location = getattr(
            self.environment.parsed_options, "test_results_location", "s3://mr-bucket-sink-<account>"
        )

        image_processing_request = self._build_image_processing_request(
            endpoint=selected_endpoint,
            endpoint_type="SM_ENDPOINT",
            image_url=selected_image,
            result_url=test_results_location,
        )
        self.client.process_image(image_processing_request)

    def _find_test_images(self) -> list[str]:
        """
        Find all test images in the configured S3 bucket.

        Searches for files with allowed image extensions in the test imagery location.

        :return: List of S3 URLs for available test images
        """

        # These attributes are defined as custom configuration attributes on the user. See
        # the json() classmethod.
        test_imagery_location = getattr(
            self.environment.parsed_options, "test_imagery_location", "s3://osml-test-images-<account>"
        )
        # Parse S3 URL: s3://bucket/key -> (bucket, key)
        parsed = urlparse(test_imagery_location)
        bucket = parsed.netloc or test_imagery_location.replace("s3://", "").split("/")[0]
        prefix = parsed.path.lstrip("/")

        s3_client = boto3.client("s3")
        result = []
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        while True:
            resp = s3_client.list_objects_v2(**kwargs)
            if "Contents" not in resp:
                break
            for obj in resp["Contents"]:
                key = obj["Key"]
                lower_key = key.lower()
                if any(lower_key.endswith(f".{ext}") for ext in self.ALLOWED_IMAGE_TYPES):
                    result.append(f"s3://{bucket}/{key}")
            try:
                kwargs["ContinuationToken"] = resp["NextContinuationToken"]
            except KeyError:
                break

        return result

    def _find_test_endpoints(self) -> list[str]:
        """
        Find all available SageMaker endpoints.

        Lists all endpoints in the current AWS account and region.

        :return: List of endpoint names
        """
        endpoint_names = []

        try:
            # Get all endpoints, handling pagination
            paginator = self.sm_client.get_paginator("list_endpoints")
            for page in paginator.paginate():
                for endpoint in page["Endpoints"]:
                    endpoint_names.append(endpoint["EndpointName"])

            return endpoint_names

        except Exception as e:
            logger.error(f"Error listing endpoints: {str(e)}", exc_info=True)
            return []
