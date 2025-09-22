import os
from dataclasses import dataclass
from typing import Optional

from .errors import ExtensionConfigurationError
from osml_extensions.enhanced_app_config import EnhancedServiceConfig


@dataclass
class AsyncEndpointConfig:
    """
    Configuration class for async SageMaker endpoint settings.

    This class provides comprehensive configuration options for async endpoint operations
    including S3 bucket settings, polling parameters, and worker pool optimization.
    """

    # S3 Configuration
    input_bucket: Optional[str] = None  # S3 bucket for input data
    output_bucket: Optional[str] = None  # S3 bucket for output data
    input_prefix: str = "async-inference/input/"
    output_prefix: str = "async-inference/output/"

    # Polling Configuration
    max_wait_time: int = 3600  # Maximum wait time in seconds
    polling_interval: int = 30  # Initial polling interval in seconds
    max_polling_interval: int = 300  # Maximum polling interval
    exponential_backoff_multiplier: float = 1.5

    # S3 Operation Configuration
    max_retries: int = 3  # For S3 operations
    cleanup_enabled: bool = True  # Whether to cleanup temp files

    # Cleanup Configuration
    cleanup_policy: str = "immediate"  # immediate, delayed, disabled
    cleanup_delay_seconds: int = 300  # Delay for delayed cleanup policy (5 minutes)

    # Worker pool optimization settings
    enable_worker_optimization: bool = True  # Enable separate submission/polling workers
    submission_workers: int = 4  # Number of workers for submitting async requests
    polling_workers: int = 2  # Number of workers for polling results
    max_concurrent_jobs: int = 100  # Maximum concurrent async jobs
    job_queue_timeout: int = 300  # Timeout for job queue operations

    def __post_init__(self):
        """Post-initialization validation and environment variable loading."""
        self._load_from_environment()
        self._validate_configuration()

    def _load_from_environment(self) -> None:
        """Load configuration from environment variables if not set."""
        if self.input_bucket is None:
            self.input_bucket = os.getenv("ASYNC_SM_INPUT_BUCKET")

        if self.output_bucket is None:
            self.output_bucket = os.getenv("ASYNC_SM_OUTPUT_BUCKET")

        # Load other environment variables with current values as defaults
        self.input_prefix = os.getenv("ASYNC_SM_INPUT_PREFIX", self.input_prefix)
        self.output_prefix = os.getenv("ASYNC_SM_OUTPUT_PREFIX", self.output_prefix)
        self.max_wait_time = int(os.getenv("ASYNC_SM_MAX_WAIT_TIME", str(self.max_wait_time)))
        self.polling_interval = int(os.getenv("ASYNC_SM_POLLING_INTERVAL", str(self.polling_interval)))
        self.max_polling_interval = int(os.getenv("ASYNC_SM_MAX_POLLING_INTERVAL", str(self.max_polling_interval)))
        self.exponential_backoff_multiplier = float(
            os.getenv("ASYNC_SM_BACKOFF_MULTIPLIER", str(self.exponential_backoff_multiplier))
        )
        self.max_retries = int(os.getenv("ASYNC_SM_MAX_RETRIES", str(self.max_retries)))
        self.cleanup_enabled = os.getenv("ASYNC_SM_CLEANUP_ENABLED", str(self.cleanup_enabled)).lower() == "true"
        self.enable_worker_optimization = (
            os.getenv("ASYNC_SM_WORKER_OPTIMIZATION", str(self.enable_worker_optimization)).lower() == "true"
        )
        self.submission_workers = int(os.getenv("ASYNC_SM_SUBMISSION_WORKERS", str(self.submission_workers)))
        self.polling_workers = int(os.getenv("ASYNC_SM_POLLING_WORKERS", str(self.polling_workers)))
        self.max_concurrent_jobs = int(os.getenv("ASYNC_SM_MAX_CONCURRENT_JOBS", str(self.max_concurrent_jobs)))
        self.job_queue_timeout = int(os.getenv("ASYNC_SM_JOB_QUEUE_TIMEOUT", str(self.job_queue_timeout)))
        self.cleanup_policy = os.getenv("ASYNC_SM_CLEANUP_POLICY", self.cleanup_policy)
        self.cleanup_delay_seconds = int(os.getenv("ASYNC_SM_CLEANUP_DELAY_SECONDS", str(self.cleanup_delay_seconds)))

    def _validate_configuration(self) -> None:
        """Validate configuration parameters."""
        if self.input_bucket is None:
            raise ExtensionConfigurationError("input_bucket is required for async endpoint configuration")

        if self.output_bucket is None:
            raise ExtensionConfigurationError("output_bucket is required for async endpoint configuration")

        if not isinstance(self.input_bucket, str) or not self.input_bucket.strip():
            raise ExtensionConfigurationError("input_bucket must be a non-empty string")

        if not isinstance(self.output_bucket, str) or not self.output_bucket.strip():
            raise ExtensionConfigurationError("output_bucket must be a non-empty string")

        if self.max_wait_time <= 0:
            raise ExtensionConfigurationError("max_wait_time must be positive")

        if self.polling_interval <= 0:
            raise ExtensionConfigurationError("polling_interval must be positive")

        if self.max_polling_interval < self.polling_interval:
            raise ExtensionConfigurationError("max_polling_interval must be >= polling_interval")

        if self.exponential_backoff_multiplier <= 1.0:
            raise ExtensionConfigurationError("exponential_backoff_multiplier must be > 1.0")

        if self.max_retries < 0:
            raise ExtensionConfigurationError("max_retries must be non-negative")

        if self.submission_workers <= 0:
            raise ExtensionConfigurationError("submission_workers must be positive")

        if self.polling_workers <= 0:
            raise ExtensionConfigurationError("polling_workers must be positive")

        if self.max_concurrent_jobs <= 0:
            raise ExtensionConfigurationError("max_concurrent_jobs must be positive")

        if self.job_queue_timeout <= 0:
            raise ExtensionConfigurationError("job_queue_timeout must be positive")

        if self.cleanup_policy not in ["immediate", "delayed", "disabled"]:
            raise ExtensionConfigurationError("cleanup_policy must be one of: immediate, delayed, disabled")

        if self.cleanup_delay_seconds <= 0:
            raise ExtensionConfigurationError("cleanup_delay_seconds must be positive")

    def get_input_s3_uri(self, key: str) -> str:
        """Generate input S3 URI for the given key."""
        return f"s3://{self.input_bucket}/{self.input_prefix}{key}"

    def get_output_s3_uri(self, key: str) -> str:
        """Generate output S3 URI for the given key."""
        return f"s3://{self.output_bucket}/{self.output_prefix}{key}"

@dataclass
class AsyncServiceConfig(EnhancedServiceConfig):
    """
    Async ServiceConfig

    This class extends the base ServiceConfig with extension settings
    while maintaining full compatibility with the base model runner.
    """

    async_endpoint_config: AsyncEndpointConfig = AsyncEndpointConfig()
