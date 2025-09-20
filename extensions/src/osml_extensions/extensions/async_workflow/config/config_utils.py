#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
import os
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError

from ..errors import ExtensionConfigurationError

logger = logging.getLogger(__name__)


class ConfigurationValidator:
    """
    Utility class for validating async endpoint configurations.

    This class provides comprehensive validation for S3 buckets, SageMaker endpoints,
    and other AWS resources required for async endpoint operations.
    """

    def __init__(self, aws_session: Optional[boto3.Session] = None):
        """
        Initialize ConfigurationValidator.

        :param aws_session: Optional boto3 session for AWS operations
        """
        self.session = aws_session or boto3.Session()
        self.s3_client = None
        self.sagemaker_client = None

    def _get_s3_client(self):
        """Get or create S3 client."""
        if self.s3_client is None:
            self.s3_client = self.session.client("s3")
        return self.s3_client

    def _get_sagemaker_client(self):
        """Get or create SageMaker client."""
        if self.sagemaker_client is None:
            self.sagemaker_client = self.session.client("sagemaker")
        return self.sagemaker_client

    def validate_s3_bucket_access(self, bucket_name: str, required_permissions: Optional[list] = None) -> bool:
        """
        Validate S3 bucket exists and has required permissions.

        :param bucket_name: Name of the S3 bucket to validate
        :param required_permissions: List of required permissions (read, write, delete)
        :return: True if bucket is accessible with required permissions
        :raises ExtensionConfigurationError: If bucket validation fails
        """
        if not bucket_name:
            raise ExtensionConfigurationError("Bucket name cannot be empty")

        required_permissions = required_permissions or ["read", "write"]

        try:
            s3_client = self._get_s3_client()

            # Check if bucket exists
            try:
                s3_client.head_bucket(Bucket=bucket_name)
                logger.debug(f"S3 bucket {bucket_name} exists and is accessible")
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                if error_code == "404":
                    raise ExtensionConfigurationError(f"S3 bucket {bucket_name} does not exist")
                elif error_code == "403":
                    raise ExtensionConfigurationError(f"Access denied to S3 bucket {bucket_name}")
                else:
                    raise ExtensionConfigurationError(f"Error accessing S3 bucket {bucket_name}: {error_code}")

            # Test required permissions
            test_key = "async-endpoint-config-test"

            if "write" in required_permissions:
                try:
                    s3_client.put_object(
                        Bucket=bucket_name, Key=test_key, Body=b"test", Metadata={"test": "async-endpoint-validation"}
                    )
                    logger.debug(f"Write permission validated for bucket {bucket_name}")
                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code")
                    raise ExtensionConfigurationError(f"No write permission for S3 bucket {bucket_name}: {error_code}")

            if "read" in required_permissions:
                try:
                    s3_client.head_object(Bucket=bucket_name, Key=test_key)
                    logger.debug(f"Read permission validated for bucket {bucket_name}")
                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code")
                    if error_code != "NoSuchKey":  # NoSuchKey is expected if we didn't write
                        raise ExtensionConfigurationError(f"No read permission for S3 bucket {bucket_name}: {error_code}")

            if "delete" in required_permissions:
                try:
                    s3_client.delete_object(Bucket=bucket_name, Key=test_key)
                    logger.debug(f"Delete permission validated for bucket {bucket_name}")
                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code")
                    raise ExtensionConfigurationError(f"No delete permission for S3 bucket {bucket_name}: {error_code}")

            # Clean up test object if it was created
            if "write" in required_permissions:
                try:
                    s3_client.delete_object(Bucket=bucket_name, Key=test_key)
                except ClientError:
                    pass  # Ignore cleanup errors

            return True

        except ExtensionConfigurationError:
            raise
        except Exception as e:
            raise ExtensionConfigurationError(f"Unexpected error validating S3 bucket {bucket_name}: {str(e)}")

    def validate_sagemaker_endpoint(self, endpoint_name: str, check_async_support: bool = True) -> bool:
        """
        Validate SageMaker endpoint exists and supports async inference.

        :param endpoint_name: Name of the SageMaker endpoint
        :param check_async_support: Whether to check for async inference support
        :return: True if endpoint is valid
        :raises ExtensionConfigurationError: If endpoint validation fails
        """
        if not endpoint_name:
            raise ExtensionConfigurationError("Endpoint name cannot be empty")

        try:
            sagemaker_client = self._get_sagemaker_client()

            # Check if endpoint exists
            try:
                response = sagemaker_client.describe_endpoint(EndpointName=endpoint_name)
                endpoint_status = response.get("EndpointStatus")

                if endpoint_status != "InService":
                    raise ExtensionConfigurationError(
                        f"SageMaker endpoint {endpoint_name} is not in service (status: {endpoint_status})"
                    )

                logger.debug(f"SageMaker endpoint {endpoint_name} is in service")

                # Check async inference support if requested
                if check_async_support:
                    endpoint_config_name = response.get("EndpointConfigName")
                    if endpoint_config_name:
                        config_response = sagemaker_client.describe_endpoint_config(EndpointConfigName=endpoint_config_name)

                        # Check for async inference configuration
                        async_inference_config = config_response.get("AsyncInferenceConfig")
                        if not async_inference_config:
                            logger.warning(
                                f"SageMaker endpoint {endpoint_name} may not support async inference. "
                                f"No AsyncInferenceConfig found in endpoint configuration."
                            )
                        else:
                            logger.debug(f"SageMaker endpoint {endpoint_name} supports async inference")

                return True

            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                if error_code == "ValidationException":
                    raise ExtensionConfigurationError(f"SageMaker endpoint {endpoint_name} does not exist")
                else:
                    raise ExtensionConfigurationError(f"Error accessing SageMaker endpoint {endpoint_name}: {error_code}")

        except ExtensionConfigurationError:
            raise
        except Exception as e:
            raise ExtensionConfigurationError(f"Unexpected error validating SageMaker endpoint {endpoint_name}: {str(e)}")

    def validate_iam_permissions(self, required_actions: list) -> Dict[str, bool]:
        """
        Validate IAM permissions for async endpoint operations.

        :param required_actions: List of required IAM actions
        :return: Dictionary mapping actions to permission status
        """
        # This is a simplified validation - in practice, you might use
        # IAM policy simulation or other methods to check permissions
        permissions = {}

        for action in required_actions:
            # For now, assume permissions are available
            # In a real implementation, you would check actual permissions
            permissions[action] = True
            logger.debug(f"Assumed permission for action: {action}")

        return permissions


class EnvironmentConfigLoader:
    """
    Utility class for loading configuration from environment variables.

    This class provides standardized loading of configuration values from
    environment variables with type conversion and validation.
    """

    @staticmethod
    def get_string(key: str, default: Optional[str] = None, required: bool = False) -> Optional[str]:
        """
        Get string value from environment variable.

        :param key: Environment variable key
        :param default: Default value if not found
        :param required: Whether the value is required
        :return: String value or None
        :raises ExtensionConfigurationError: If required value is missing
        """
        value = os.getenv(key, default)

        if required and not value:
            raise ExtensionConfigurationError(f"Required environment variable {key} is not set")

        return value

    @staticmethod
    def get_int(
        key: str,
        default: Optional[int] = None,
        required: bool = False,
        min_value: Optional[int] = None,
        max_value: Optional[int] = None,
    ) -> Optional[int]:
        """
        Get integer value from environment variable.

        :param key: Environment variable key
        :param default: Default value if not found
        :param required: Whether the value is required
        :param min_value: Minimum allowed value
        :param max_value: Maximum allowed value
        :return: Integer value or None
        :raises ExtensionConfigurationError: If value is invalid
        """
        value_str = os.getenv(key)

        if not value_str:
            if required:
                raise ExtensionConfigurationError(f"Required environment variable {key} is not set")
            return default

        try:
            value = int(value_str)
        except ValueError:
            raise ExtensionConfigurationError(f"Environment variable {key} must be an integer, got: {value_str}")

        if min_value is not None and value < min_value:
            raise ExtensionConfigurationError(f"Environment variable {key} must be >= {min_value}, got: {value}")

        if max_value is not None and value > max_value:
            raise ExtensionConfigurationError(f"Environment variable {key} must be <= {max_value}, got: {value}")

        return value

    @staticmethod
    def get_float(
        key: str,
        default: Optional[float] = None,
        required: bool = False,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
    ) -> Optional[float]:
        """
        Get float value from environment variable.

        :param key: Environment variable key
        :param default: Default value if not found
        :param required: Whether the value is required
        :param min_value: Minimum allowed value
        :param max_value: Maximum allowed value
        :return: Float value or None
        :raises ExtensionConfigurationError: If value is invalid
        """
        value_str = os.getenv(key)

        if not value_str:
            if required:
                raise ExtensionConfigurationError(f"Required environment variable {key} is not set")
            return default

        try:
            value = float(value_str)
        except ValueError:
            raise ExtensionConfigurationError(f"Environment variable {key} must be a number, got: {value_str}")

        if min_value is not None and value < min_value:
            raise ExtensionConfigurationError(f"Environment variable {key} must be >= {min_value}, got: {value}")

        if max_value is not None and value > max_value:
            raise ExtensionConfigurationError(f"Environment variable {key} must be <= {max_value}, got: {value}")

        return value

    @staticmethod
    def get_bool(key: str, default: Optional[bool] = None, required: bool = False) -> Optional[bool]:
        """
        Get boolean value from environment variable.

        :param key: Environment variable key
        :param default: Default value if not found
        :param required: Whether the value is required
        :return: Boolean value or None
        :raises ExtensionConfigurationError: If value is invalid
        """
        value_str = os.getenv(key)

        if not value_str:
            if required:
                raise ExtensionConfigurationError(f"Required environment variable {key} is not set")
            return default

        value_lower = value_str.lower()

        if value_lower in ("true", "1", "yes", "on", "enabled"):
            return True
        elif value_lower in ("false", "0", "no", "off", "disabled"):
            return False
        else:
            raise ExtensionConfigurationError(
                f"Environment variable {key} must be a boolean value "
                f"(true/false, 1/0, yes/no, on/off, enabled/disabled), got: {value_str}"
            )

    @staticmethod
    def get_list(key: str, default: Optional[list] = None, required: bool = False, separator: str = ",") -> Optional[list]:
        """
        Get list value from environment variable.

        :param key: Environment variable key
        :param default: Default value if not found
        :param required: Whether the value is required
        :param separator: Separator for list items
        :return: List value or None
        :raises ExtensionConfigurationError: If required value is missing
        """
        value_str = os.getenv(key)

        if not value_str:
            if required:
                raise ExtensionConfigurationError(f"Required environment variable {key} is not set")
            return default

        # Split and strip whitespace
        items = [item.strip() for item in value_str.split(separator) if item.strip()]
        return items

    @staticmethod
    def load_config_dict(prefix: str = "ASYNC_SM_") -> Dict[str, Any]:
        """
        Load all environment variables with a given prefix into a dictionary.

        :param prefix: Prefix to filter environment variables
        :return: Dictionary of configuration values
        """
        config = {}

        for key, value in os.environ.items():
            if key.startswith(prefix):
                config_key = key[len(prefix) :].lower()
                config[config_key] = value

        return config


class ConfigurationManager:
    """
    Centralized configuration management for async endpoint operations.

    This class provides a unified interface for loading, validating, and managing
    configuration from multiple sources (environment variables, files, defaults).
    """

    def __init__(self, validator: Optional[ConfigurationValidator] = None):
        """
        Initialize ConfigurationManager.

        :param validator: Optional ConfigurationValidator instance
        """
        self.validator = validator or ConfigurationValidator()
        self.env_loader = EnvironmentConfigLoader()

    def load_and_validate_config(self, config_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Load and validate complete configuration.

        :param config_dict: Configuration dictionary to validate
        :return: Validated configuration dictionary
        :raises ExtensionConfigurationError: If validation fails
        """
        validated_config = config_dict.copy()

        # Validate S3 buckets if specified
        input_bucket = validated_config.get("input_bucket")
        output_bucket = validated_config.get("output_bucket")

        if input_bucket:
            try:
                self.validator.validate_s3_bucket_access(input_bucket, ["read", "write", "delete"])
                logger.info(f"Input bucket {input_bucket} validation successful")
            except ExtensionConfigurationError as e:
                logger.warning(f"Input bucket validation failed: {e}")
                # Don't fail configuration loading, just warn

        if output_bucket:
            try:
                self.validator.validate_s3_bucket_access(output_bucket, ["read", "write", "delete"])
                logger.info(f"Output bucket {output_bucket} validation successful")
            except ExtensionConfigurationError as e:
                logger.warning(f"Output bucket validation failed: {e}")
                # Don't fail configuration loading, just warn

        return validated_config

    def get_default_config(self) -> Dict[str, Any]:
        """
        Get default configuration values.

        :return: Dictionary of default configuration values
        """
        return {
            "input_prefix": "async-inference/input/",
            "output_prefix": "async-inference/output/",
            "max_wait_time": 3600,
            "polling_interval": 30,
            "max_polling_interval": 300,
            "exponential_backoff_multiplier": 1.5,
            "max_retries": 3,
            "cleanup_enabled": True,
            "enable_worker_optimization": True,
            "submission_workers": 4,
            "polling_workers": 2,
            "max_concurrent_jobs": 100,
            "job_queue_timeout": 300,
        }

    def merge_configs(self, *configs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge multiple configuration dictionaries with precedence.

        :param configs: Configuration dictionaries in order of precedence (later overrides earlier)
        :return: Merged configuration dictionary
        """
        merged = {}

        for config in configs:
            if config:
                merged.update(config)

        return merged
