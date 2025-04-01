#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
Unit tests for the SageMakerHelper utility class.
"""

import os
from unittest.mock import MagicMock, patch

import pytest

from aws.osml.model_runner_validation_tool.common.sagemaker_utils import SageMakerHelper


class TestSageMakerHelper:
    """Test cases for the SageMakerHelper class"""

    def test_init(self):
        """Test initialization of SageMakerHelper"""
        # Test with explicit role ARN
        helper = SageMakerHelper(ecs_image_uri="test-image-uri", execution_role_arn="test-role-arn")
        assert helper.execution_role_arn == "test-role-arn"
        assert helper.ecs_image_uri == "test-image-uri"

        # TODO Add a test for model_name

        # Test with environment variable
        with patch.dict(os.environ, {"SAGEMAKER_EXECUTION_ROLE_ARN": "env-role-arn"}):
            helper = SageMakerHelper()
            assert helper.execution_role_arn == "env-role-arn"

    def test_create_model(self):
        """Test creating a SageMaker model"""
        # Mock the SageMaker client
        mock_sagemaker = MagicMock()
        mock_sagemaker.create_model.return_value = {"ModelArn": "arn:aws:sagemaker:us-west-2:123456789012:model/test-model"}

        helper = SageMakerHelper(ecs_image_uri="test-image-uri", execution_role_arn="test-role-arn")
        helper.sagemaker_client = mock_sagemaker

        model_name = helper.create_model(model_data_uri="test-model-data-uri", environment_vars={"TEST_ENV": "test-value"})

        assert model_name == helper.model_name

        mock_sagemaker.create_model.assert_called_once_with(
            ModelName=helper.model_name,
            ExecutionRoleArn="test-role-arn",
            PrimaryContainer={
                "Image": "test-image-uri",
                "ModelDataUrl": "test-model-data-uri",
                "Environment": {"TEST_ENV": "test-value"},
            },
        )

    def test_create_endpoint_config(self):
        """Test creating a SageMaker endpoint configuration"""
        # Mock the SageMaker client
        mock_sagemaker = MagicMock()
        mock_sagemaker.create_endpoint_config.return_value = {
            "EndpointConfigArn": "arn:aws:sagemaker:us-west-2:123456789012:endpoint-config/test-endpoint-config"
        }

        helper = SageMakerHelper(ecs_image_uri="test-image-uri", execution_role_arn="test-role-arn")
        helper.sagemaker_client = mock_sagemaker
        helper.model_name = "test-model"  # Override the model name for testing

        helper.create_endpoint_config(instance_type="ml.m5.xlarge")

        mock_sagemaker.create_endpoint_config.assert_called_once()
        call_args = mock_sagemaker.create_endpoint_config.call_args[1]
        assert "EndpointConfigName" in call_args
        assert call_args["ProductionVariants"][0]["ModelName"] == "test-model"
        assert call_args["ProductionVariants"][0]["InstanceType"] == "ml.m5.xlarge"

    def test_create_endpoint(self):
        """Test creating a SageMaker endpoint"""
        # Mock the SageMaker client
        mock_sagemaker = MagicMock()
        mock_sagemaker.create_endpoint.return_value = {
            "EndpointArn": "arn:aws:sagemaker:us-west-2:123456789012:endpoint/test-endpoint"
        }

        # Mock the waiter
        mock_waiter = MagicMock()
        mock_sagemaker.get_waiter.return_value = mock_waiter

        helper = SageMakerHelper(ecs_image_uri="test-image-uri", execution_role_arn="test-role-arn")
        helper.sagemaker_client = mock_sagemaker
        helper.model_name = "test-model"  # Override the model name for testing

        endpoint_info = helper.create_endpoint(endpoint_config_name="test-endpoint-config", wait_for_completion=True)

        assert "endpointName" in endpoint_info
        assert endpoint_info["endpointConfigName"] == "test-endpoint-config"
        assert "creationStartTime" in endpoint_info

        # When wait_for_completion is True, these should be present
        assert "inServiceTime" in endpoint_info
        assert "timeToInService" in endpoint_info

        # Verify the client was called correctly
        mock_sagemaker.create_endpoint.assert_called_once()
        mock_sagemaker.get_waiter.assert_called_once_with("endpoint_in_service")
        mock_waiter.wait.assert_called_once()

    def test_delete_resources(self):
        """Test deleting SageMaker resources"""
        # Mock the SageMaker client
        mock_sagemaker = MagicMock()

        helper = SageMakerHelper(execution_role_arn="test-role-arn")
        helper.sagemaker_client = mock_sagemaker

        helper.delete_resources(
            model_name="test-model", endpoint_config_name="test-endpoint-config", endpoint_name="test-endpoint"
        )

        mock_sagemaker.delete_endpoint.assert_called_once_with(EndpointName="test-endpoint")
        mock_sagemaker.delete_endpoint_config.assert_called_once_with(EndpointConfigName="test-endpoint-config")
        mock_sagemaker.delete_model.assert_called_once_with(ModelName="test-model")

    def test_generate_unique_name(self):
        """Test generating a unique name"""
        # Create helper
        helper = SageMakerHelper(execution_role_arn="test-role-arn")

        # Mock time.time to return predictable values
        with patch("time.time", side_effect=[1000, 2000]):
            base_name = "test-base-name"

            # First call
            unique_name = helper.generate_unique_name(base_name)
            assert unique_name == f"{base_name}-1000"

            # Second call
            another_unique_name = helper.generate_unique_name(base_name)
            assert another_unique_name == f"{base_name}-2000"

            # Verify they're different
            assert unique_name != another_unique_name

    def test_generate_unique_name_truncation(self):
        """Test that generate_unique_name truncates base names longer than 24 characters"""
        # Create helper
        helper = SageMakerHelper(execution_role_arn="test-role-arn")

        # Mock time.time to return a predictable value
        with patch("time.time", return_value=1000):
            # Test with a short name (no truncation)
            short_base_name = "short-name"
            short_unique_name = helper.generate_unique_name(short_base_name)
            assert short_unique_name == f"{short_base_name}-1000"

            # Test with a name exactly 24 characters (no truncation)
            exact_base_name = "a" * 24
            exact_unique_name = helper.generate_unique_name(exact_base_name)
            assert exact_unique_name == f"{exact_base_name}-1000"
            assert len(exact_base_name) == 24

            # Test with a long name (should be truncated)
            long_base_name = "this-is-a-very-long-base-name-that-exceeds-24-characters"
            long_unique_name = helper.generate_unique_name(long_base_name)
            assert long_unique_name == f"{long_base_name[:24]}-1000"
            assert len(long_base_name[:24]) == 24

            # Verify the total length is correct (24 chars + hyphen + timestamp)
            assert len(long_unique_name) == 24 + 1 + len(str(1000))

    def test_create_model_missing_role(self):
        """Test creating a model without a role ARN"""
        # Patch the environment variable to ensure it's not set
        with patch.dict(os.environ, {"SAGEMAKER_EXECUTION_ROLE_ARN": ""}, clear=True):
            # Create a helper with no execution role ARN
            helper = SageMakerHelper(ecs_image_uri="test-image-uri", execution_role_arn=None)

            # Mock the sagemaker_client to avoid actual API calls
            helper.sagemaker_client = MagicMock()

            # Directly test the condition that should raise the error
            with pytest.raises(ValueError, match="SageMaker execution role ARN is required"):
                # Call the method that should raise the error
                helper.create_model(model_data_uri="test-model-data-uri")

    def test_create_model_missing_ecs_image_uri(self):
        """Test creating a model without a ECS Image URI"""
        helper = SageMakerHelper(execution_role_arn="test-role-arn")

        with pytest.raises(ValueError, match="ECS image URI is required to create a model"):
            helper.create_model(model_data_uri="test-model-data-uri")

    def test_create_endpoint_config_missing_ecs_image_uri(self):
        """Test creating an endpoint configuration without an ECS Image URI"""
        helper = SageMakerHelper(execution_role_arn="test-role-arn")

        with pytest.raises(ValueError, match="ECS image URI is required to create an endpoint configuration"):
            helper.create_endpoint_config(instance_type="ml.m5.xlarge")

    def test_create_endpoint_missing_ecs_image_uri(self):
        """Test creating an endpoint without a ECS Image URI"""
        helper = SageMakerHelper(execution_role_arn="test-role-arn")

        with pytest.raises(ValueError, match="ECS image URI is required to create an endpoint"):
            helper.create_endpoint(endpoint_config_name="test-endpoint-config", wait_for_completion=True)
