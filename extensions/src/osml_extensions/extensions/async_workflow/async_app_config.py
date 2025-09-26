#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.
import os
from dataclasses import dataclass, field

import logging
from typing import Optional

from aws.osml.model_runner.app_config import ServiceConfig

logger = logging.getLogger(__name__)


def var_to_bool(var_name):
    return os.getenv(var_name, "false").lower() in ("true", "1")


@dataclass
class AsyncEndpointConfig:
    """
    Configuration class for async SageMaker endpoint settings.

    This class provides comprehensive configuration options for async endpoint operations
    including S3 bucket settings, polling parameters, and worker pool optimization.
    """

    # Load other environment variables with current values as defaults
    input_bucket = os.getenv("ARTIFACT_BUCKET")
    input_prefix = os.getenv("ASYNC_SM_INPUT_PREFIX", "async-inference/input/")
    max_wait_time = int(os.getenv("ASYNC_SM_MAX_WAIT_TIME", 3600))  # Maximum wait time in seconds
    polling_interval = int(os.getenv("ASYNC_SM_POLLING_INTERVAL", 30))  # Initial polling interval in seconds
    max_polling_interval = int(os.getenv("ASYNC_SM_MAX_POLLING_INTERVAL", 300))  # Maximum polling interval
    exponential_backoff_multiplier = float(os.getenv("ASYNC_SM_BACKOFF_MULTIPLIER", 1.5))
    max_retries = int(os.getenv("ASYNC_SM_MAX_RETRIES", 3))  # For S3 operations
    cleanup_enabled = os.getenv("ASYNC_SM_CLEANUP_ENABLED", "true").lower() == "true"  # Whether to cleanup temp files
    enable_worker_optimization = (
        os.getenv("ASYNC_SM_WORKER_OPTIMIZATION", "true").lower() == "true"  # Enable separate submission/polling workers
    )
    submission_workers = int(os.getenv("ASYNC_SM_SUBMISSION_WORKERS", 4))  # Number of workers for submitting async requests
    polling_workers = int(os.getenv("ASYNC_SM_POLLING_WORKERS", 2))  # Number of workers for polling results
    max_concurrent_jobs = int(os.getenv("ASYNC_SM_MAX_CONCURRENT_JOBS", 100))  # Maximum concurrent async jobs
    job_queue_timeout = int(os.getenv("ASYNC_SM_JOB_QUEUE_TIMEOUT", 300))  # Timeout for job queue operations
    cleanup_policy = os.getenv("ASYNC_SM_CLEANUP_POLICY", "immediate")  # immediate, delayed, disabled
    cleanup_delay_seconds = int(
        os.getenv("ASYNC_SM_CLEANUP_DELAY_SECONDS", 300)
    )  # Delay for delayed cleanup policy (5 minutes)

    @staticmethod
    def get_input_s3_uri(input_bucket: str, input_prefix: str, key: str) -> str:
        """Generate input S3 URI for the given key."""
        return f"s3://{input_bucket}/{input_prefix}{key}"


@dataclass
class AsyncServiceConfig(ServiceConfig):
    """
    Async ServiceConfig

    This class extends the base ServiceConfig with extension settings
    while maintaining full compatibility with the base model runner.
    """

    # Extension configuration
    use_extensions: bool = var_to_bool("USE_EXTENSIONS")
    async_detector_enabled: bool = var_to_bool("ASYNC_DETECTOR_ENABLED")
    enhanced_monitoring_enabled: bool = var_to_bool("ENHANCED_MONITORING_ENABLED")
    extension_fallback_enabled: bool = var_to_bool("EXTENSION_FALLBACK_ENABLED")
    enable_async_processing: bool = var_to_bool("ENABLE_ASYNC_PROCESSING")
    max_concurrent_regions: int = int(os.getenv("MAX_CONCURRENT_REGIONS", "100"))

    # Request type configuration
    request_type: str = os.getenv("REQUEST_TYPE", "async_sm_endpoint")

    # tile tracking table
    tile_request_table: str = os.environ["TILE_REQUEST_TABLE"]
    tile_queue: str = os.environ.get("TILE_QUEUE")

    # async endpoint config
    async_endpoint_config: AsyncEndpointConfig = field(default=AsyncEndpointConfig)

    # Optional
    region_status_topic: Optional[str] = os.getenv("TILE_STATUS_TOPIC")
    tile_status_topic: Optional[str] = os.getenv("TILE_STATUS_TOPIC")
