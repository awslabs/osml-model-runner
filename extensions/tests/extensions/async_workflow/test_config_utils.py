#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import os
import unittest
from unittest.mock import Mock, patch

import boto3
from botocore.exceptions import ClientError
from moto import mock_s3, mock_sagemaker

from ..src.osml_extensions.config import ConfigurationManager, ConfigurationValidator, EnvironmentConfigLoader
from ..src.osml_extensions.errors import ExtensionConfigurationError


class TestConfigurationValidator(unittest.TestCase):
    """Test cases for ConfigurationValidator."""

    def setUp(self):
        """Set up test fixtures."""
        self.validator = ConfigurationValidator()

    @mock_s3
    def test_validate_s3_bucket_access_success(self):
        """Test successful S3 bucket validation."""
        # Create test bucket
        s3_client = boto3.client("s3", region_name="us-east-1")
        bucket_name = "test-bucket"
        s3_client.create_bucket(Bucket=bucket_name)

        # Test validation
        result = self.validator.validate_s3_bucket_access(bucket_name)
        self.assertTrue(result)

    @mock_s3
    def test_validate_s3_bucket_access_not_found(self):
        """Test S3 bucket validation with non-existent bucket."""
        bucket_name = "non-existent-bucket"

        with self.assertRaises(ExtensionConfigurationError) as context:
            self.validator.validate_s3_bucket_access(bucket_name)

        self.assertIn("does not exist", str(context.exception))

    def test_validate_s3_bucket_access_empty_name(self):
        """Test S3 bucket validation with empty bucket name."""
        with self.assertRaises(ExtensionConfigurationError) as context:
            self.validator.validate_s3_bucket_access("")

        self.assertIn("cannot be empty", str(context.exception))

    @mock_sagemaker
    def test_validate_sagemaker_endpoint_success(self):
        """Test successful SageMaker endpoint validation."""
        # Create mock SageMaker client
        sagemaker_client = boto3.client("sagemaker", region_name="us-east-1")

        # Mock endpoint response
        endpoint_name = "test-endpoint"

        with patch.object(sagemaker_client, "describe_endpoint") as mock_describe:
            mock_describe.return_value = {"EndpointStatus": "InService", "EndpointConfigName": "test-config"}

            with patch.object(sagemaker_client, "describe_endpoint_config") as mock_describe_config:
                mock_describe_config.return_value = {
                    "AsyncInferenceConfig": {"OutputConfig": {"S3OutputPath": "s3://test-bucket/output/"}}
                }

                # Set the mocked client
                self.validator.sagemaker_client = sagemaker_client

                result = self.validator.validate_sagemaker_endpoint(endpoint_name)
                self.assertTrue(result)

    def test_validate_sagemaker_endpoint_empty_name(self):
        """Test SageMaker endpoint validation with empty endpoint name."""
        with self.assertRaises(ExtensionConfigurationError) as context:
            self.validator.validate_sagemaker_endpoint("")

        self.assertIn("cannot be empty", str(context.exception))

    def test_validate_sagemaker_endpoint_not_in_service(self):
        """Test SageMaker endpoint validation with endpoint not in service."""
        sagemaker_client = Mock()
        sagemaker_client.describe_endpoint.return_value = {"EndpointStatus": "Creating"}

        self.validator.sagemaker_client = sagemaker_client

        with self.assertRaises(ExtensionConfigurationError) as context:
            self.validator.validate_sagemaker_endpoint("test-endpoint")

        self.assertIn("not in service", str(context.exception))

    def test_validate_iam_permissions(self):
        """Test IAM permissions validation."""
        required_actions = ["s3:GetObject", "s3:PutObject", "sagemaker:InvokeEndpointAsync"]

        permissions = self.validator.validate_iam_permissions(required_actions)

        # Should return True for all actions (simplified implementation)
        for action in required_actions:
            self.assertTrue(permissions[action])


class TestEnvironmentConfigLoader(unittest.TestCase):
    """Test cases for EnvironmentConfigLoader."""

    def setUp(self):
        """Set up test fixtures."""
        self.loader = EnvironmentConfigLoader()

        # Clear test environment variables
        test_vars = ["TEST_STRING", "TEST_INT", "TEST_FLOAT", "TEST_BOOL", "TEST_LIST"]
        for var in test_vars:
            if var in os.environ:
                del os.environ[var]

    def test_get_string_success(self):
        """Test successful string loading."""
        os.environ["TEST_STRING"] = "test_value"

        result = self.loader.get_string("TEST_STRING")
        self.assertEqual(result, "test_value")

    def test_get_string_default(self):
        """Test string loading with default value."""
        result = self.loader.get_string("TEST_STRING", default="default_value")
        self.assertEqual(result, "default_value")

    def test_get_string_required_missing(self):
        """Test string loading with required value missing."""
        with self.assertRaises(ExtensionConfigurationError) as context:
            self.loader.get_string("TEST_STRING", required=True)

        self.assertIn("Required environment variable", str(context.exception))

    def test_get_int_success(self):
        """Test successful integer loading."""
        os.environ["TEST_INT"] = "42"

        result = self.loader.get_int("TEST_INT")
        self.assertEqual(result, 42)

    def test_get_int_invalid(self):
        """Test integer loading with invalid value."""
        os.environ["TEST_INT"] = "not_a_number"

        with self.assertRaises(ExtensionConfigurationError) as context:
            self.loader.get_int("TEST_INT")

        self.assertIn("must be an integer", str(context.exception))

    def test_get_int_range_validation(self):
        """Test integer loading with range validation."""
        os.environ["TEST_INT"] = "5"

        # Test minimum value
        with self.assertRaises(ExtensionConfigurationError) as context:
            self.loader.get_int("TEST_INT", min_value=10)

        self.assertIn("must be >= 10", str(context.exception))

        # Test maximum value
        with self.assertRaises(ExtensionConfigurationError) as context:
            self.loader.get_int("TEST_INT", max_value=3)

        self.assertIn("must be <= 3", str(context.exception))

    def test_get_float_success(self):
        """Test successful float loading."""
        os.environ["TEST_FLOAT"] = "3.14"

        result = self.loader.get_float("TEST_FLOAT")
        self.assertEqual(result, 3.14)

    def test_get_float_invalid(self):
        """Test float loading with invalid value."""
        os.environ["TEST_FLOAT"] = "not_a_number"

        with self.assertRaises(ExtensionConfigurationError) as context:
            self.loader.get_float("TEST_FLOAT")

        self.assertIn("must be a number", str(context.exception))

    def test_get_bool_true_values(self):
        """Test boolean loading with true values."""
        true_values = ["true", "1", "yes", "on", "enabled", "TRUE", "YES"]

        for value in true_values:
            with self.subTest(value=value):
                os.environ["TEST_BOOL"] = value
                result = self.loader.get_bool("TEST_BOOL")
                self.assertTrue(result)

    def test_get_bool_false_values(self):
        """Test boolean loading with false values."""
        false_values = ["false", "0", "no", "off", "disabled", "FALSE", "NO"]

        for value in false_values:
            with self.subTest(value=value):
                os.environ["TEST_BOOL"] = value
                result = self.loader.get_bool("TEST_BOOL")
                self.assertFalse(result)

    def test_get_bool_invalid(self):
        """Test boolean loading with invalid value."""
        os.environ["TEST_BOOL"] = "maybe"

        with self.assertRaises(ExtensionConfigurationError) as context:
            self.loader.get_bool("TEST_BOOL")

        self.assertIn("must be a boolean value", str(context.exception))

    def test_get_list_success(self):
        """Test successful list loading."""
        os.environ["TEST_LIST"] = "item1,item2,item3"

        result = self.loader.get_list("TEST_LIST")
        self.assertEqual(result, ["item1", "item2", "item3"])

    def test_get_list_with_spaces(self):
        """Test list loading with spaces."""
        os.environ["TEST_LIST"] = " item1 , item2 , item3 "

        result = self.loader.get_list("TEST_LIST")
        self.assertEqual(result, ["item1", "item2", "item3"])

    def test_get_list_custom_separator(self):
        """Test list loading with custom separator."""
        os.environ["TEST_LIST"] = "item1;item2;item3"

        result = self.loader.get_list("TEST_LIST", separator=";")
        self.assertEqual(result, ["item1", "item2", "item3"])

    def test_load_config_dict(self):
        """Test loading configuration dictionary from environment."""
        os.environ["ASYNC_SM_INPUT_BUCKET"] = "test-input-bucket"
        os.environ["ASYNC_SM_OUTPUT_BUCKET"] = "test-output-bucket"
        os.environ["ASYNC_SM_MAX_WAIT_TIME"] = "3600"
        os.environ["OTHER_VAR"] = "should_not_be_included"

        result = self.loader.load_config_dict("ASYNC_SM_")

        expected = {"input_bucket": "test-input-bucket", "output_bucket": "test-output-bucket", "max_wait_time": "3600"}

        self.assertEqual(result, expected)
        self.assertNotIn("other_var", result)


class TestConfigurationManager(unittest.TestCase):
    """Test cases for ConfigurationManager."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_validator = Mock()
        self.manager = ConfigurationManager(validator=self.mock_validator)

    def test_load_and_validate_config_success(self):
        """Test successful configuration loading and validation."""
        config = {"input_bucket": "test-input-bucket", "output_bucket": "test-output-bucket", "max_wait_time": 3600}

        # Mock successful validation
        self.mock_validator.validate_s3_bucket_access.return_value = True

        result = self.manager.load_and_validate_config(config)

        self.assertEqual(result, config)

        # Verify validation was called
        self.mock_validator.validate_s3_bucket_access.assert_any_call("test-input-bucket", ["read", "write", "delete"])
        self.mock_validator.validate_s3_bucket_access.assert_any_call("test-output-bucket", ["read", "write", "delete"])

    def test_load_and_validate_config_validation_warning(self):
        """Test configuration loading with validation warnings."""
        config = {"input_bucket": "test-input-bucket", "output_bucket": "test-output-bucket"}

        # Mock validation failure (should warn, not fail)
        self.mock_validator.validate_s3_bucket_access.side_effect = ExtensionConfigurationError("Validation failed")

        with patch("osml_extensions.config.config_utils.logger") as mock_logger:
            result = self.manager.load_and_validate_config(config)

            # Should still return config despite validation failure
            self.assertEqual(result, config)

            # Should log warnings
            mock_logger.warning.assert_called()

    def test_get_default_config(self):
        """Test getting default configuration."""
        defaults = self.manager.get_default_config()

        # Verify expected default values
        self.assertEqual(defaults["input_prefix"], "async-inference/input/")
        self.assertEqual(defaults["output_prefix"], "async-inference/output/")
        self.assertEqual(defaults["max_wait_time"], 3600)
        self.assertEqual(defaults["polling_interval"], 30)
        self.assertTrue(defaults["cleanup_enabled"])
        self.assertTrue(defaults["enable_worker_optimization"])

    def test_merge_configs(self):
        """Test merging multiple configuration dictionaries."""
        config1 = {"a": 1, "b": 2, "c": 3}
        config2 = {"b": 20, "d": 4}
        config3 = {"c": 30, "e": 5}

        result = self.manager.merge_configs(config1, config2, config3)

        expected = {"a": 1, "b": 20, "c": 30, "d": 4, "e": 5}
        self.assertEqual(result, expected)

    def test_merge_configs_with_none(self):
        """Test merging configurations with None values."""
        config1 = {"a": 1, "b": 2}
        config2 = None
        config3 = {"b": 20, "c": 3}

        result = self.manager.merge_configs(config1, config2, config3)

        expected = {"a": 1, "b": 20, "c": 3}
        self.assertEqual(result, expected)


class TestConfigurationIntegration(unittest.TestCase):
    """Integration tests for configuration management."""

    @patch.dict(
        os.environ,
        {
            "ASYNC_SM_INPUT_BUCKET": "env-input-bucket",
            "ASYNC_SM_OUTPUT_BUCKET": "env-output-bucket",
            "ASYNC_SM_MAX_WAIT_TIME": "7200",
            "ASYNC_SM_POLLING_INTERVAL": "60",
            "ASYNC_SM_CLEANUP_ENABLED": "false",
        },
    )
    def test_complete_configuration_workflow(self):
        """Test complete configuration loading workflow."""
        manager = ConfigurationManager()

        # Load environment config
        env_config = manager.env_loader.load_config_dict("ASYNC_SM_")

        # Get defaults
        defaults = manager.get_default_config()

        # Merge configurations (env overrides defaults)
        merged_config = manager.merge_configs(defaults, env_config)

        # Verify merged configuration
        self.assertEqual(merged_config["input_bucket"], "env-input-bucket")
        self.assertEqual(merged_config["output_bucket"], "env-output-bucket")
        self.assertEqual(merged_config["max_wait_time"], "7200")  # From env (string)
        self.assertEqual(merged_config["polling_interval"], "60")  # From env (string)
        self.assertEqual(merged_config["cleanup_enabled"], "false")  # From env (string)

        # Should still have defaults for unspecified values
        self.assertEqual(merged_config["input_prefix"], "async-inference/input/")
        self.assertEqual(merged_config["submission_workers"], 4)


if __name__ == "__main__":
    unittest.main()
