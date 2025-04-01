#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

from aws.osml.model_runner_validation_tool.sagemaker_compatibility import handler, run_sagemaker_compatibility_tests

# Add the src directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../")))

# Import after path setup


@pytest.fixture
def mock_event():
    """Create a mock event for testing"""
    return {
        "modelInfo": {
            "ecsImageUri": "test-image-uri",
            "modelName": "test-model",
            "modelArn": "arn:aws:sagemaker:us-west-2:123456789012:model/test-model",
            "modelId": "test-model-id",
            "modelVersion": "1.0",
            "modelDescription": "Test model for unit tests",
            "modelOwner": "Test User",
            "modelCreationDate": "2023-01-01T00:00:00Z",
        }
    }


@pytest.fixture
def mock_context():
    """Create a mock Lambda context"""
    context = MagicMock()
    context.function_name = "test-function"
    context.function_version = "$LATEST"
    context.invoked_function_arn = "arn:aws:lambda:us-west-2:123456789012:function:test-function"
    context.memory_limit_in_mb = 128
    context.log_group_name = "/aws/lambda/test-function"
    context.log_stream_name = "2023/01/01/[$LATEST]abcdef123456"
    return context


@pytest.fixture
def mock_s3_utils():
    """Create a mock S3Utils instance"""
    s3_utils_mock = MagicMock()
    s3_utils_mock.put_object.return_value = True
    s3_utils_mock.save_test_results.return_value = True
    return s3_utils_mock


@pytest.fixture
def mock_cloudwatch_client():
    """Create a mock CloudWatch client"""
    cloudwatch_client = MagicMock()
    cloudwatch_client.put_metric_data.return_value = {}
    return cloudwatch_client


@pytest.fixture
def mock_sagemaker_client():
    """Create a mock SageMaker client"""
    sagemaker_client = MagicMock()
    return sagemaker_client


def test_handler_success(mock_event, mock_context, mock_s3_utils, mock_cloudwatch_client, mock_sagemaker_client):
    """Test the Lambda handler with a successful event"""
    with patch("aws.osml.model_runner_validation_tool.sagemaker_compatibility.s3_utils", mock_s3_utils), patch(
        "aws.osml.model_runner_validation_tool.sagemaker_compatibility.cloudwatch_client", mock_cloudwatch_client
    ), patch("aws.osml.model_runner_validation_tool.sagemaker_compatibility.SageMakerHelper") as mock_helper, patch.dict(
        os.environ, {"REPORT_BUCKET": "test-bucket", "SAGEMAKER_EXECUTION_ROLE_ARN": "test-role-arn"}
    ):
        # Mock the SageMakerHelper methods
        mock_helper_instance = mock_helper.return_value
        mock_helper_instance.create_model.return_value = "test-model"
        mock_helper_instance.create_endpoint_config.return_value = "test-endpoint-config"
        mock_helper_instance.create_endpoint.return_value = {
            "endpointName": "test-endpoint",
            "endpointConfigName": "test-endpoint-config",
            "timeToInService": 60,
        }

        # Call the handler
        response = handler(mock_event, mock_context)

        # Verify the response
        assert response["statusCode"] == 200
        assert "sageMakerCompatibilityResults" in response
        assert response["sageMakerCompatibilityResults"]["sageMakerCompatible"] is True

        # Verify CloudWatch metrics were logged
        mock_cloudwatch_client.put_metric_data.assert_called()


def test_handler_missing_model_info(mock_context, mock_s3_utils, mock_cloudwatch_client, mock_sagemaker_client):
    """Test the Lambda handler with missing model info"""
    with patch("aws.osml.model_runner_validation_tool.sagemaker_compatibility.s3_utils", mock_s3_utils), patch(
        "aws.osml.model_runner_validation_tool.sagemaker_compatibility.cloudwatch_client", mock_cloudwatch_client
    ), patch.dict(os.environ, {"REPORT_BUCKET": "test-bucket", "SAGEMAKER_EXECUTION_ROLE_ARN": "test-role-arn"}):
        # Call the handler with an empty event
        with pytest.raises(Exception):
            handler({}, mock_context)


def test_run_sagemaker_compatibility_tests(mock_event, mock_s3_utils, mock_cloudwatch_client, mock_sagemaker_client):
    """Test the run_sagemaker_compatibility_tests function"""
    with patch("aws.osml.model_runner_validation_tool.sagemaker_compatibility.s3_utils", mock_s3_utils), patch(
        "aws.osml.model_runner_validation_tool.sagemaker_compatibility.cloudwatch_client", mock_cloudwatch_client
    ), patch("aws.osml.model_runner_validation_tool.sagemaker_compatibility.SageMakerHelper") as mock_helper, patch.dict(
        os.environ, {"REPORT_BUCKET": "test-bucket", "SAGEMAKER_EXECUTION_ROLE_ARN": "test-role-arn"}
    ):
        # Mock the SageMakerHelper methods
        mock_helper_instance = mock_helper.return_value
        mock_helper_instance.create_model.return_value = "test-model"
        mock_helper_instance.create_endpoint_config.return_value = "test-endpoint-config"
        mock_helper_instance.create_endpoint.return_value = {
            "endpointName": "test-endpoint",
            "endpointConfigName": "test-endpoint-config",
            "timeToInService": 60,
        }

        # Call the function
        model_info = mock_event["modelInfo"]
        test_results = run_sagemaker_compatibility_tests(model_info, "test-role-arn")

        # Verify the results
        assert test_results["modelName"] == "test-model"
        assert test_results["sageMakerCompatible"] is True

        # Verify CloudWatch metrics were logged
        mock_cloudwatch_client.put_metric_data.assert_called()
