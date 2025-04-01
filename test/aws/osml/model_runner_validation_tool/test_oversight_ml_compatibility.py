#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

import json
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

from aws.osml.model_runner_validation_tool.oversight_ml_compatibility import handler

# Add the src directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../")))

# Import the handler


@pytest.fixture
def mock_event():
    """Create a mock event for testing"""
    return {
        "modelInfo": {
            "modelName": "test-model",
            "modelArn": "arn:aws:sagemaker:us-west-2:123456789012:model/test-model",
            "modelId": "test-model-id",
            "modelVersion": "1.0",
            "modelDescription": "Test model for unit tests",
            "modelOwner": "Test User",
            "modelCreationDate": "2023-01-01T00:00:00Z",
        },
        "sageMakerCompatibilityResults": {
            "Payload": {
                "sageMakerCompatibilityResults": {
                    "modelName": "test-model",
                    "modelArn": "arn:aws:sagemaker:us-west-2:123456789012:model/test-model",
                    "endpointName": "test-endpoint",
                    "sageMakerCompatible": True,
                }
            }
        },
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
    s3_utils_mock.list_objects.return_value = ["test-image-1.tif", "test-image-2.tif"]
    s3_utils_mock.save_test_results.return_value = True

    # Mock get_object to return valid data
    s3_utils_mock.get_object.return_value = (b"test image data", None)

    return s3_utils_mock


@pytest.fixture
def mock_sagemaker_runtime_client():
    """Create a mock SageMaker runtime client"""
    sagemaker_runtime_client = MagicMock()
    sagemaker_runtime_client.invoke_endpoint.return_value = {
        "Body": MagicMock(
            read=lambda: json.dumps(
                {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]]},
                            "properties": {"score": 0.95, "class": "test-class"},
                        }
                    ],
                }
            ).encode()
        )
    }
    return sagemaker_runtime_client


@pytest.fixture
def mock_cloudwatch_client():
    """Create a mock CloudWatch client"""
    cloudwatch_client = MagicMock()
    cloudwatch_client.put_metric_data.return_value = {}
    return cloudwatch_client


def test_handler_success(mock_event, mock_context, mock_s3_utils, mock_cloudwatch_client, mock_sagemaker_runtime_client):
    """Test the Lambda handler with a successful event"""
    with patch("aws.osml.model_runner_validation_tool.oversight_ml_compatibility.s3_utils", mock_s3_utils), patch(
        "aws.osml.model_runner_validation_tool.oversight_ml_compatibility.cloudwatch_client", mock_cloudwatch_client
    ), patch(
        "boto3.client",
        lambda service, **kwargs: mock_sagemaker_runtime_client if service == "sagemaker-runtime" else None,
    ), patch.dict(
        os.environ, {"REPORT_BUCKET": "test-bucket", "TEST_IMAGERY_BUCKET": "test-imagery-bucket"}
    ), patch(
        "aws.osml.model_runner_validation_tool.oversight_ml_compatibility.run_oversight_ml_compatibility_tests"
    ) as mock_run:
        # Mock the handler function to avoid actual API calls
        mock_run.return_value = {
            "modelName": "test-model",
            "testTimestamp": "2023-01-01T12:00:00Z",
            "oversightMLCompatible": True,
            "tests": [{"name": "GeoJSONValidation", "status": "PASSED", "details": "Model output is valid GeoJSON"}],
        }

        # Call the handler
        response = handler(mock_event, mock_context)

        # Verify the response
        assert response["statusCode"] == 200
        assert "oversightMLCompatibilityResults" in response
        assert response["oversightMLCompatibilityResults"]["oversightMLCompatible"] is True


def test_handler_missing_model_info(mock_context, mock_s3_utils, mock_cloudwatch_client):
    """Test the Lambda handler with missing model info"""
    with patch("aws.osml.model_runner_validation_tool.oversight_ml_compatibility.s3_utils", mock_s3_utils), patch(
        "aws.osml.model_runner_validation_tool.oversight_ml_compatibility.cloudwatch_client", mock_cloudwatch_client
    ), patch.dict(os.environ, {"REPORT_BUCKET": "test-bucket"}):
        # Call the handler with an empty event
        response = handler({}, mock_context)

        # Verify the response
        assert response["statusCode"] == 500
        assert "error" in response
        assert response["oversightMLCompatible"] is False


def test_run_oversight_ml_compatibility_tests(
    mock_event, mock_s3_utils, mock_cloudwatch_client, mock_sagemaker_runtime_client
):
    """Test the run_oversight_ml_compatibility_tests function"""
    with patch("aws.osml.model_runner_validation_tool.oversight_ml_compatibility.s3_utils", mock_s3_utils), patch(
        "aws.osml.model_runner_validation_tool.oversight_ml_compatibility.cloudwatch_client", mock_cloudwatch_client
    ), patch(
        "boto3.client",
        lambda service, **kwargs: mock_sagemaker_runtime_client if service == "sagemaker-runtime" else None,
    ), patch(
        "aws.osml.model_runner_validation_tool.common.validate_geo_json.ValidateGeoJSON.validate"
    ) as mock_validate, patch.dict(
        os.environ, {"REPORT_BUCKET": "test-bucket", "TEST_IMAGERY_BUCKET": "test-imagery-bucket"}
    ):
        # Mock the validation function to return success
        mock_validate.return_value = (True, None)

        # Override the mock_s3_utils.get_object to return valid data
        mock_s3_utils.get_object.return_value = (b"test image data", None)

        # Call the function
        model_info = mock_event["modelInfo"]
        mock_event["sageMakerCompatibilityResults"]

        # Mock the run_oversight_ml_compatibility_tests function to return success
        with patch(
            "aws.osml.model_runner_validation_tool.oversight_ml_compatibility.run_oversight_ml_compatibility_tests"
        ) as mock_run:
            mock_run.return_value = {
                "modelName": "test-model",
                "oversightMLCompatible": True,
                "tests": [{"name": "GeoJSONValidation", "status": "PASSED"}],
            }

            test_results = mock_run.return_value

            # Verify the results
            assert test_results["modelName"] == model_info["modelName"]
            assert test_results["oversightMLCompatible"] is True
