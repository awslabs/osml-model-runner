#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
Configuration management for integration tests.

This module provides centralized configuration management for integration tests,
including environment variable handling imported from ECS task definitions.
"""

import logging
import os
from dataclasses import dataclass
from typing import Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


@dataclass
class Config:
    """
    Configuration class for OSML integration tests.

    This class manages infrastructure configuration parameters for integration tests,
    with values imported directly from the ECS task definition.

    Configuration values are extracted from the task definition and stored
    directly in this class. Only infrastructure-level configuration (queues, tables,
    default result destinations) are stored here. Test-specific parameters (image,
    model, region of interest) should be specified per-test via ImageRequest.
    """

    # Core AWS infrastructure (computed/derived)
    ACCOUNT: Optional[str] = None
    REGION: Optional[str] = None

    # SQS queues (extracted from task definition URLs)
    IMAGE_QUEUE_NAME: Optional[str] = None  # Extracted from IMAGE_QUEUE URL
    IMAGE_STATUS_QUEUE_NAME: Optional[str] = None  # Derived from IMAGE_QUEUE_NAME

    # Result destinations (with defaults if not provided)
    # These serve as defaults but can be overridden per-test via ImageRequest
    KINESIS_RESULTS_STREAM_PREFIX: str = os.getenv("KINESIS_RESULTS_STREAM_PREFIX", "mr-stream-sink")
    S3_RESULTS_BUCKET_PREFIX: str = os.getenv("S3_RESULTS_BUCKET_PREFIX", "mr-bucket-sink")

    # Values from task definition (set when built from task definition)
    FEATURE_TABLE: Optional[str] = None
    REGION_REQUEST_TABLE: Optional[str] = None

    @staticmethod
    def _extract_queue_name_from_url(url: str) -> str:
        """
        Extract queue name from SQS URL format.

        :param url: SQS queue URL or name.
        :returns: Queue name extracted from URL, or the original value if already a name.

        Examples:
            https://sqs.us-west-2.amazonaws.com/${ACCOUNT}/ImageRequestQueue -> ImageRequestQueue
            ImageRequestQueue -> ImageRequestQueue (already a name)
        """
        if not url:
            return ""
        # If it's already just a name (no URL), return as-is
        if not url.startswith("http"):
            return url
        # Extract queue name from URL (last part after final slash)
        return url.split("/")[-1]

    def __post_init__(self) -> None:
        """
        Initialize configuration by auto-detecting AWS account and region.

        Automatically loads configuration from task definition if required values are missing.
        """
        self.ACCOUNT = self.ACCOUNT or self._get_aws_account()
        self.REGION = self.REGION or self._get_aws_region()

        # Automatically load from task definition if we don't have task definition values yet
        if not self.FEATURE_TABLE or not self.IMAGE_QUEUE_NAME:
            try:
                self._load_from_task_definition()
            except Exception as e:
                # If building fails, log but continue with empty config
                logger.debug(f"Could not automatically load config from task definition: {e}")

    def _load_from_task_definition(self, pattern: Optional[str] = None, region: Optional[str] = None) -> None:
        """
        Load configuration values from ECS task definition.

        :param pattern: Pattern to match task definition names. Defaults to "ModelRunnerDataplane" or from env.
        :param region: AWS region to search in. Defaults to auto-detected region or "us-west-2".
        :raises RuntimeError: If task definition cannot be found or accessed.
        """
        task_def_pattern = pattern or os.environ.get("TASK_DEFINITION_PATTERN", "ModelRunnerDataplane")
        region = region or self._get_aws_region() or "us-west-2"

        ecs_client = boto3.client("ecs", region_name=region)

        # Find the latest matching task definition
        logger.info(f"Searching for task definition: {task_def_pattern}")
        paginator = ecs_client.get_paginator("list_task_definitions")
        matching_arns = [
            arn
            for page in paginator.paginate(sort="DESC")
            for arn in page.get("taskDefinitionArns", [])
            if task_def_pattern in arn
        ]

        if not matching_arns:
            raise RuntimeError(
                f"No task definition found with pattern: {task_def_pattern} in region: {region}. "
                f"Integration tests require a deployed Model Runner task definition."
            )

        task_def_arn = matching_arns[0]
        task_def_name = task_def_arn.split("/")[-1]
        logger.info(f"Found task definition: {task_def_name}")

        # Extract environment variables from task definition
        response = ecs_client.describe_task_definition(taskDefinition=task_def_arn)
        container_defs = response.get("taskDefinition", {}).get("containerDefinitions", [])
        if not container_defs:
            raise RuntimeError(f"No container definitions found in task definition: {task_def_arn}")

        env_dict = {
            env_var["name"]: env_var["value"]
            for env_var in container_defs[0].get("environment", [])
            if env_var.get("name") and env_var.get("value")
        }

        logger.info(f"Imported {len(env_dict)} environment variables")

        # Set defaults for test-specific variables
        env_dict.setdefault("KINESIS_RESULTS_STREAM_PREFIX", "mr-stream-sink")
        env_dict.setdefault("S3_RESULTS_BUCKET_PREFIX", "mr-bucket-sink")

        # Extract queue names
        image_queue = env_dict.get("IMAGE_QUEUE")
        self.IMAGE_QUEUE_NAME = self._extract_queue_name_from_url(image_queue) if image_queue else None

        status_queue = env_dict.get("SQS_IMAGE_STATUS_QUEUE") or env_dict.get("IMAGE_STATUS_QUEUE")
        if status_queue:
            self.IMAGE_STATUS_QUEUE_NAME = self._extract_queue_name_from_url(status_queue)
        elif self.IMAGE_QUEUE_NAME:
            # Derive status queue name from request queue name
            request_queue = self.IMAGE_QUEUE_NAME
            if "Request" in request_queue:
                self.IMAGE_STATUS_QUEUE_NAME = request_queue.replace("Request", "Status")
            elif request_queue.endswith("Queue"):
                self.IMAGE_STATUS_QUEUE_NAME = request_queue.replace("Queue", "StatusQueue")
            else:
                self.IMAGE_STATUS_QUEUE_NAME = f"{request_queue}Status"

        # Populate instance with values
        self.KINESIS_RESULTS_STREAM_PREFIX = env_dict.get("KINESIS_RESULTS_STREAM_PREFIX", "mr-stream-sink")
        self.S3_RESULTS_BUCKET_PREFIX = env_dict.get("S3_RESULTS_BUCKET_PREFIX", "mr-bucket-sink")
        self.FEATURE_TABLE = env_dict.get("FEATURE_TABLE")
        self.REGION_REQUEST_TABLE = env_dict.get("REGION_REQUEST_TABLE")
        self.REGION = self.REGION or region
        self.ACCOUNT = self.ACCOUNT or self._get_aws_account()

    def _get_aws_account(self) -> Optional[str]:
        """
        Get AWS account ID from STS or environment variable.

        :returns: AWS account ID if found, None otherwise.
        """
        try:
            return boto3.client("sts").get_caller_identity().get("Account")
        except Exception as e:
            logger.debug(f"Failed to get AWS account from STS: {e}")
            return os.environ.get("ACCOUNT")

    def _get_aws_region(self) -> Optional[str]:
        """
        Get AWS region from various sources.

        Tries in order: environment variables, boto3 session, EC2 metadata.

        :returns: AWS region if found, None otherwise.
        """
        # Try environment variables first
        region = os.environ.get("REGION") or os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION")
        if region:
            return region

        # Try boto3 session
        try:
            region = boto3.Session().region_name
            if region:
                return region
        except Exception:
            pass

        # Try EC2 metadata (if running on EC2)
        try:
            import requests

            response = requests.get("http://169.254.169.254/latest/meta-data/placement/region", timeout=1)
            if response.status_code == 200:
                return response.text
        except Exception:
            pass

        return None

    @staticmethod
    def from_task_definition(pattern: str = "ModelRunnerDataplane", region: Optional[str] = None) -> "Config":
        """
        Build a Config instance from ECS task definition environment variables.

        :param pattern: Pattern to match task definition names. Defaults to "ModelRunnerDataplane".
        :param region: AWS region to search in. Defaults to auto-detected.
        :returns: Config instance configured with values from the task definition.
        :raises RuntimeError: If task definition cannot be found or accessed.
        """
        config = Config()
        try:
            config._load_from_task_definition(pattern=pattern, region=region)
        except ClientError as e:
            raise RuntimeError(
                f"Failed to import environment variables from task definition: {e}. "
                f"Ensure AWS credentials are configured and you have access to ECS."
            ) from e
        except RuntimeError:
            raise
        except Exception as e:
            raise RuntimeError(f"Unexpected error importing environment variables from task definition: {e}") from e

        return config

    def validate(self) -> None:
        """
        Validate that required configuration parameters are set.

        :raises ValueError: If required parameters (ACCOUNT, REGION) are missing.
        """
        required_params = ["ACCOUNT", "REGION"]
        missing_params = [param for param in required_params if not getattr(self, param)]

        if missing_params:
            raise ValueError(f"Missing required configuration parameters: {', '.join(missing_params)}")


# Global configuration instance
config = Config()
