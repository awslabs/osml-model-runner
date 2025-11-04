#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
Configuration management for integration tests.

This module provides centralized configuration management for integration tests,
including environment variable handling, default values, and validation.
"""

import logging
import os
from dataclasses import dataclass
from typing import Optional

import boto3

logger = logging.getLogger(__name__)


@dataclass
class IntegConfig:
    """
    Configuration class for OSML integration tests.

    This class manages configuration parameters for integration tests,
    with sensible defaults and environment variable support.
    """

    # Core AWS infrastructure
    ACCOUNT: Optional[str] = None
    REGION: Optional[str] = None

    # SQS queues (only the ones actually used)
    SQS_IMAGE_REQUEST_QUEUE: str = os.getenv("SQS_IMAGE_REQUEST_QUEUE", "ImageRequestQueue")
    SQS_IMAGE_STATUS_QUEUE: str = os.getenv("SQS_IMAGE_STATUS_QUEUE", "ImageStatusQueue")

    # DynamoDB tables
    DDB_FEATURES_TABLE: str = os.getenv("DDB_FEATURES_TABLE", "ImageProcessingFeatures")
    DDB_REGION_REQUEST_TABLE: str = os.getenv("REGION_REQUEST_TABLE", "RegionRequestTable")

    # Result destinations
    KINESIS_RESULTS_STREAM_PREFIX: str = os.getenv("KINESIS_RESULTS_STREAM_PREFIX", "mr-stream-sink")
    S3_RESULTS_BUCKET_PREFIX: str = os.getenv("S3_RESULTS_BUCKET_PREFIX", "mr-bucket-sink")

    # Testing configuration
    TARGET_IMAGE: Optional[str] = os.environ.get("TARGET_IMAGE")
    TARGET_MODEL: Optional[str] = os.environ.get("TARGET_MODEL")
    REGION_OF_INTEREST: Optional[str] = os.environ.get("REGION_OF_INTEREST")

    def __post_init__(self):
        """Auto-detect AWS account and region if not provided."""
        if not self.ACCOUNT:
            self.ACCOUNT = self._get_aws_account()
        if not self.REGION:
            self.REGION = self._get_aws_region()

    def _get_aws_account(self) -> Optional[str]:
        """Get AWS account ID from STS."""
        try:
            sts_client = boto3.client("sts")
            response = sts_client.get_caller_identity()
            return response.get("Account")
        except Exception as e:
            logger.debug(f"Failed to get AWS account from STS: {e}. Falling back to environment variable.")
            # Fall back to environment variable
            return os.environ.get("ACCOUNT")

    def _get_aws_region(self) -> Optional[str]:
        """Get AWS region from various sources."""
        # Try environment variable first
        region = os.environ.get("REGION") or os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION")
        if region:
            return region

        # Try to get from boto3 session
        try:
            session = boto3.Session()
            return session.region_name
        except Exception as e:
            logger.debug(f"Failed to get AWS region from boto3 session: {e}. Trying EC2 metadata next.")

        # Try to get from EC2 metadata (if running on EC2)
        try:
            import requests

            response = requests.get("http://169.254.169.254/latest/meta-data/placement/region", timeout=1)
            if response.status_code == 200:
                return response.text
        except Exception as e:
            logger.debug(f"Failed to get AWS region from EC2 metadata: {e}. Region detection exhausted.")

        return None

    def validate(self) -> None:
        """
        Validate that required configuration parameters are set.

        Raises:
            ValueError: If required parameters are missing
        """
        required_params = ["ACCOUNT", "REGION"]
        missing_params = [param for param in required_params if not getattr(self, param)]

        if missing_params:
            raise ValueError(f"Missing required configuration parameters: {', '.join(missing_params)}")

        # Check for test-specific parameters and provide helpful guidance
        missing_test_params = []
        if not self.TARGET_IMAGE:
            missing_test_params.append("TARGET_IMAGE (S3 URI to test image)")
        if not self.TARGET_MODEL:
            missing_test_params.append("TARGET_MODEL (model name to test)")

        if missing_test_params:
            raise ValueError(
                f"Integration tests require test parameters: {', '.join(missing_test_params)}. "
                f"Set these environment variables to run specific tests."
            )


# Global configuration instance
config = IntegConfig()
