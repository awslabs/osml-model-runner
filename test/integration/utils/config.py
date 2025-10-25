#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
Configuration management for integration tests.

This module provides centralized configuration management for integration tests,
including environment variable handling, default values, and validation.
"""

import os
from dataclasses import dataclass
from typing import Optional

import boto3


@dataclass
class OSMLConfig:
    """
    Configuration class for OSML integration tests.

    This class manages all configuration parameters for integration tests,
    with sensible defaults and environment variable support.
    """

    # SNS topic names
    SNS_IMAGE_STATUS_TOPIC: str = os.getenv("SNS_IMAGE_STATUS_TOPIC", "ImageStatusTopic")
    SNS_REGION_STATUS_TOPIC: str = os.getenv("SNS_REGION_STATUS_TOPIC", "RegionStatusTopic")

    # SQS queue names
    SQS_IMAGE_REQUEST_QUEUE: str = os.getenv("SQS_IMAGE_REQUEST_QUEUE", "ImageRequestQueue")
    SQS_REGION_REQUEST_QUEUE: str = os.getenv("SQS_REGION_REQUEST_QUEUE", "RegionRequestQueue")
    SQS_IMAGE_STATUS_QUEUE: str = os.getenv("SQS_IMAGE_STATUS_QUEUE", "ImageStatusQueue")
    SQS_REGION_STATUS_QUEUE: str = os.getenv("SQS_REGION_STATUS_QUEUE", "RegionStatusQueue")

    # DynamoDB table names
    DDB_JOB_STATUS_TABLE: str = os.getenv("DDB_JOB_STATUS_TABLE", "ImageProcessingJobStatus")
    DDB_FEATURES_TABLE: str = os.getenv("DDB_FEATURES_TABLE", "ImageProcessingFeatures")
    DDB_ENDPOINT_PROCESSING_TABLE: str = os.getenv("DDB_ENDPOINT_PROCESSING_TABLE", "EndpointProcessingStatistics")
    DDB_REGION_REQUEST_TABLE: str = os.getenv("DDB_REGION_REQUEST_TABLE", "RegionProcessingJobStatus")

    # SageMaker model names
    SM_CENTERPOINT_MODEL: str = os.getenv("SM_CENTERPOINT_MODEL", "centerpoint")
    SM_FLOOD_MODEL: str = os.getenv("SM_FLOOD_MODEL", "flood")
    SM_AIRCRAFT_MODEL: str = os.getenv("SM_AIRCRAFT_MODEL", "aircraft")
    SM_MULTI_CONTAINER_ENDPOINT: str = os.getenv("SM_MULTI_CONTAINER_ENDPOINT", "multi-container")

    # HTTP model configuration
    HTTP_CENTERPOINT_MODEL_URL: Optional[str] = os.getenv("HTTP_CENTERPOINT_MODEL_URL")
    HTTP_CENTERPOINT_MODEL_ELB_NAME: str = os.getenv("HTTP_CENTERPOINT_MODEL_ELB_NAME", "test-http-model-endpoint")
    HTTP_CENTERPOINT_MODEL_INFERENCE_PATH: str = os.getenv("HTTP_CENTERPOINT_MODEL_INFERENCE_PATH", "/invocations")

    # S3 bucket configuration
    S3_RESULTS_BUCKET: Optional[str] = os.getenv("S3_RESULTS_BUCKET")
    S3_RESULTS_BUCKET_PREFIX: str = os.getenv("S3_RESULTS_BUCKET_PREFIX", "mr-bucket-sink")
    S3_IMAGE_BUCKET_PREFIX: str = os.getenv("S3_IMAGE_BUCKET_PREFIX", "mr-test-imagery")

    # Kinesis stream configuration
    KINESIS_RESULTS_STREAM: Optional[str] = os.getenv("KINESIS_RESULTS_STREAM")
    KINESIS_RESULTS_STREAM_PREFIX: str = os.getenv("KINESIS_RESULTS_STREAM_PREFIX", "mr-stream-sink")

    # Deployment information
    ACCOUNT: Optional[str] = None
    REGION: Optional[str] = None

    # Testing configuration
    TARGET_IMAGE: Optional[str] = os.environ.get("TARGET_IMAGE")
    TARGET_MODEL: Optional[str] = os.environ.get("TARGET_MODEL")
    TILE_FORMAT: str = os.environ.get("TILE_FORMAT", "GTIFF")
    TILE_COMPRESSION: str = os.environ.get("TILE_COMPRESSION", "NONE")
    TILE_SIZE: int = int(os.environ.get("TILE_SIZE", "512"))
    TILE_OVERLAP: int = int(os.environ.get("TILE_OVERLAP", "128"))
    POST_PROCESSING: str = os.environ.get(
        "POST_PROCESSING", '[{"step": "FEATURE_DISTILLATION", "algorithm": {"algorithmType": "NMS", "iouThreshold": 0.75}}]'
    )
    REGION_OF_INTEREST: Optional[str] = os.environ.get("REGION_OF_INTEREST")

    def __post_init__(self):
        """Auto-detect AWS account and region if not provided."""
        if not self.ACCOUNT:
            self.ACCOUNT = self._get_aws_account()
        if not self.REGION:
            self.REGION = self._get_aws_region()

        # Update resource names that depend on account/region
        self._update_resource_names()

    def _get_aws_account(self) -> Optional[str]:
        """Get AWS account ID from STS."""
        try:
            sts_client = boto3.client("sts")
            response = sts_client.get_caller_identity()
            return response.get("Account")
        except Exception:
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
        except Exception:
            pass

        # Try to get from EC2 metadata (if running on EC2)
        try:
            import requests

            response = requests.get("http://169.254.169.254/latest/meta-data/placement/region", timeout=1)
            if response.status_code == 200:
                return response.text
        except Exception:
            pass

        return None

    def _update_resource_names(self):
        """Update resource names that depend on account/region."""
        # Update Kinesis stream name if not explicitly set
        if not self.KINESIS_RESULTS_STREAM and self.ACCOUNT:
            self.KINESIS_RESULTS_STREAM = f"{self.KINESIS_RESULTS_STREAM_PREFIX}-{self.ACCOUNT}"

        # Update S3 bucket names if not explicitly set
        if not self.S3_RESULTS_BUCKET and self.ACCOUNT:
            self.S3_RESULTS_BUCKET = f"{self.S3_RESULTS_BUCKET_PREFIX}-{self.ACCOUNT}"

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


@dataclass
class OSMLLoadTestConfig:
    """
    Configuration class for OSML load testing.

    This class manages configuration parameters specific to load testing scenarios.
    """

    # SageMaker model configuration for load testing
    SM_LOAD_TEST_MODEL: str = os.getenv("SM_LOAD_TEST_MODEL", "aircraft")

    # S3 bucket configuration for load testing
    S3_LOAD_TEST_SOURCE_IMAGE_BUCKET: Optional[str] = os.getenv("S3_LOAD_TEST_SOURCE_IMAGE_BUCKET")
    S3_LOAD_TEST_RESULT_BUCKET: Optional[str] = os.getenv("S3_LOAD_TEST_RESULT_BUCKET")

    # Load testing workflow configuration
    PERIODIC_SLEEP_SECS: str = os.getenv("PERIODIC_SLEEP_SECS", "60")
    PROCESSING_WINDOW_MIN: str = os.getenv("PROCESSING_WINDOW_MIN", "1")

    def validate(self) -> None:
        """
        Validate that required load test configuration parameters are set.

        Raises:
            ValueError: If required parameters are missing
        """
        required_params = ["S3_LOAD_TEST_SOURCE_IMAGE_BUCKET", "S3_LOAD_TEST_RESULT_BUCKET"]
        missing_params = [param for param in required_params if not getattr(self, param)]

        if missing_params:
            raise ValueError(f"Missing required load test configuration parameters: {', '.join(missing_params)}")


# Global configuration instances
config = OSMLConfig()
load_test_config = OSMLLoadTestConfig()
