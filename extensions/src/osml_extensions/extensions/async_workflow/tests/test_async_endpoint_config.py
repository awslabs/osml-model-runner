#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import os
import unittest
from unittest.mock import patch

from ..src.osml_extensions.config import AsyncEndpointConfig
from ..src.osml_extensions.errors import ExtensionConfigurationError


class TestAsyncEndpointConfig(unittest.TestCase):
    """Test cases for AsyncEndpointConfig."""

    def setUp(self):
        """Set up test fixtures."""
        # Clear environment variables that might affect tests
        env_vars_to_clear = [
            "ARTIFACT_BUCKET",
            "ASYNC_SM_OUTPUT_BUCKET",
            "ASYNC_SM_INPUT_PREFIX",
            "ASYNC_SM_OUTPUT_PREFIX",
            "ASYNC_SM_MAX_WAIT_TIME",
            "ASYNC_SM_POLLING_INTERVAL",
            "ASYNC_SM_MAX_POLLING_INTERVAL",
            "ASYNC_SM_BACKOFF_MULTIPLIER",
            "ASYNC_SM_MAX_RETRIES",
            "ASYNC_SM_CLEANUP_ENABLED",
            "ASYNC_SM_WORKER_OPTIMIZATION",
            "ASYNC_SM_SUBMISSION_WORKERS",
            "ASYNC_SM_POLLING_WORKERS",
            "ASYNC_SM_MAX_CONCURRENT_JOBS",
            "ASYNC_SM_JOB_QUEUE_TIMEOUT",
            "ASYNC_SM_CLEANUP_POLICY",
            "ASYNC_SM_CLEANUP_DELAY_SECONDS",
        ]

        for var in env_vars_to_clear:
            if var in os.environ:
                del os.environ[var]

    def test_valid_configuration(self):
        """Test creating configuration with valid parameters."""
        config = AsyncEndpointConfig(input_bucket="test-input-bucket", output_bucket="test-output-bucket")

        self.assertEqual(config.input_bucket, "test-input-bucket")
        self.assertEqual(config.output_bucket, "test-output-bucket")
        self.assertEqual(config.input_prefix, "async-inference/input/")
        self.assertEqual(config.output_prefix, "async-inference/output/")
        self.assertEqual(config.max_wait_time, 3600)
        self.assertEqual(config.polling_interval, 30)
        self.assertEqual(config.max_polling_interval, 300)
        self.assertEqual(config.exponential_backoff_multiplier, 1.5)
        self.assertEqual(config.max_retries, 3)
        self.assertTrue(config.cleanup_enabled)
        self.assertTrue(config.enable_worker_optimization)
        self.assertEqual(config.submission_workers, 4)
        self.assertEqual(config.polling_workers, 2)
        self.assertEqual(config.max_concurrent_jobs, 100)
        self.assertEqual(config.job_queue_timeout, 300)

    def test_missing_input_bucket(self):
        """Test configuration validation with missing input bucket."""
        with self.assertRaises(ExtensionConfigurationError) as context:
            AsyncEndpointConfig(output_bucket="test-output-bucket")

        self.assertIn("input_bucket is required", str(context.exception))

    def test_missing_output_bucket(self):
        """Test configuration validation with missing output bucket."""
        with self.assertRaises(ExtensionConfigurationError) as context:
            AsyncEndpointConfig(input_bucket="test-input-bucket")

        self.assertIn("output_bucket is required", str(context.exception))

    def test_empty_bucket_names(self):
        """Test configuration validation with empty bucket names."""
        with self.assertRaises(ExtensionConfigurationError) as context:
            AsyncEndpointConfig(input_bucket="", output_bucket="test-output-bucket")

        self.assertIn("input_bucket must be a non-empty string", str(context.exception))

        with self.assertRaises(ExtensionConfigurationError) as context:
            AsyncEndpointConfig(input_bucket="test-input-bucket", output_bucket="")

        self.assertIn("output_bucket must be a non-empty string", str(context.exception))

    def test_invalid_timing_parameters(self):
        """Test configuration validation with invalid timing parameters."""
        # Negative max_wait_time
        with self.assertRaises(ExtensionConfigurationError) as context:
            AsyncEndpointConfig(input_bucket="test-input-bucket", output_bucket="test-output-bucket", max_wait_time=-1)
        self.assertIn("max_wait_time must be positive", str(context.exception))

        # Negative polling_interval
        with self.assertRaises(ExtensionConfigurationError) as context:
            AsyncEndpointConfig(input_bucket="test-input-bucket", output_bucket="test-output-bucket", polling_interval=-1)
        self.assertIn("polling_interval must be positive", str(context.exception))

        # max_polling_interval < polling_interval
        with self.assertRaises(ExtensionConfigurationError) as context:
            AsyncEndpointConfig(
                input_bucket="test-input-bucket",
                output_bucket="test-output-bucket",
                polling_interval=100,
                max_polling_interval=50,
            )
        self.assertIn("max_polling_interval must be >= polling_interval", str(context.exception))

    def test_invalid_backoff_multiplier(self):
        """Test configuration validation with invalid backoff multiplier."""
        with self.assertRaises(ExtensionConfigurationError) as context:
            AsyncEndpointConfig(
                input_bucket="test-input-bucket", output_bucket="test-output-bucket", exponential_backoff_multiplier=1.0
            )
        self.assertIn("exponential_backoff_multiplier must be > 1.0", str(context.exception))

    def test_invalid_worker_parameters(self):
        """Test configuration validation with invalid worker parameters."""
        # Zero submission workers
        with self.assertRaises(ExtensionConfigurationError) as context:
            AsyncEndpointConfig(input_bucket="test-input-bucket", output_bucket="test-output-bucket", submission_workers=0)
        self.assertIn("submission_workers must be positive", str(context.exception))

        # Zero polling workers
        with self.assertRaises(ExtensionConfigurationError) as context:
            AsyncEndpointConfig(input_bucket="test-input-bucket", output_bucket="test-output-bucket", polling_workers=0)
        self.assertIn("polling_workers must be positive", str(context.exception))

    @patch.dict(
        os.environ,
        {
            "ARTIFACT_BUCKET": "env-input-bucket",
            "ASYNC_SM_OUTPUT_BUCKET": "env-output-bucket",
            "ASYNC_SM_MAX_WAIT_TIME": "7200",
            "ASYNC_SM_POLLING_INTERVAL": "60",
            "ASYNC_SM_CLEANUP_ENABLED": "false",
            "ASYNC_SM_SUBMISSION_WORKERS": "8",
        },
    )
    def test_environment_variable_loading(self):
        """Test loading configuration from environment variables."""
        config = AsyncEndpointConfig()

        self.assertEqual(config.input_bucket, "env-input-bucket")
        self.assertEqual(config.output_bucket, "env-output-bucket")
        self.assertEqual(config.max_wait_time, 7200)
        self.assertEqual(config.polling_interval, 60)
        self.assertFalse(config.cleanup_enabled)
        self.assertEqual(config.submission_workers, 8)

    @patch.dict(os.environ, {"ARTIFACT_BUCKET": "env-input-bucket", "ASYNC_SM_OUTPUT_BUCKET": "env-output-bucket"})
    def test_constructor_overrides_environment(self):
        """Test that constructor parameters override environment variables."""
        config = AsyncEndpointConfig(input_bucket="constructor-input-bucket", output_bucket="constructor-output-bucket")

        # Constructor parameters should override environment
        self.assertEqual(config.input_bucket, "constructor-input-bucket")
        self.assertEqual(config.output_bucket, "constructor-output-bucket")

    @patch.dict(os.environ, {"ARTIFACT_BUCKET": "env-input-bucket", "ASYNC_SM_OUTPUT_BUCKET": "env-output-bucket"})
    def test_from_environment_class_method(self):
        """Test creating configuration from environment variables only."""
        config = AsyncEndpointConfig.from_environment()

        self.assertEqual(config.input_bucket, "env-input-bucket")
        self.assertEqual(config.output_bucket, "env-output-bucket")

    def test_s3_uri_generation(self):
        """Test S3 URI generation methods."""
        config = AsyncEndpointConfig(
            input_bucket="test-input-bucket",
            output_bucket="test-output-bucket",
            input_prefix="custom-input/",
            output_prefix="custom-output/",
        )

        input_uri = config.get_input_s3_uri("test-key")
        output_uri = config.get_output_s3_uri("test-key")

        self.assertEqual(input_uri, "s3://test-input-bucket/custom-input/test-key")
        self.assertEqual(output_uri, "s3://test-output-bucket/custom-output/test-key")

    def test_cleanup_policy_configuration(self):
        """Test cleanup policy configuration."""
        config = AsyncEndpointConfig(
            input_bucket="test-input-bucket",
            output_bucket="test-output-bucket",
            cleanup_policy="delayed",
            cleanup_delay_seconds=600,
        )

        self.assertEqual(config.cleanup_policy, "delayed")
        self.assertEqual(config.cleanup_delay_seconds, 600)

    def test_invalid_cleanup_policy(self):
        """Test invalid cleanup policy raises error."""
        with self.assertRaises(ExtensionConfigurationError):
            AsyncEndpointConfig(
                input_bucket="test-input-bucket", output_bucket="test-output-bucket", cleanup_policy="invalid_policy"
            )

    def test_invalid_cleanup_delay_seconds(self):
        """Test invalid cleanup delay seconds raises error."""
        with self.assertRaises(ExtensionConfigurationError):
            AsyncEndpointConfig(
                input_bucket="test-input-bucket", output_bucket="test-output-bucket", cleanup_delay_seconds=-1
            )

    def test_cleanup_policy_from_environment(self):
        """Test loading cleanup policy from environment variables."""
        with patch.dict(
            os.environ,
            {
                "ARTIFACT_BUCKET": "env-input-bucket",
                "ASYNC_SM_OUTPUT_BUCKET": "env-output-bucket",
                "ASYNC_SM_CLEANUP_POLICY": "delayed",
                "ASYNC_SM_CLEANUP_DELAY_SECONDS": "900",
            },
        ):
            config = AsyncEndpointConfig()

            self.assertEqual(config.cleanup_policy, "delayed")
            self.assertEqual(config.cleanup_delay_seconds, 900)

    def test_default_cleanup_configuration(self):
        """Test default cleanup configuration values."""
        config = AsyncEndpointConfig(input_bucket="test-input-bucket", output_bucket="test-output-bucket")

        self.assertEqual(config.cleanup_policy, "immediate")
        self.assertEqual(config.cleanup_delay_seconds, 300)


if __name__ == "__main__":
    unittest.main()
