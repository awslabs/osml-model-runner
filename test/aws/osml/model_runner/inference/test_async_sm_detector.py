#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import datetime
import io
import json
import unittest
from unittest import TestCase
from unittest.mock import Mock, patch, MagicMock

import boto3
import pytest
from botocore.exceptions import ClientError
from botocore.stub import ANY, Stubber

from aws.osml.model_runner.exceptions import ProcessTileException


class TestAsyncSMDetector(TestCase):
    """Unit tests for AsyncSMDetector class"""

    def test_construct_with_credentials(self):
        """Test AsyncSMDetector construction with AWS credentials"""
        from aws.osml.model_runner.inference.async_sm_detector import AsyncSMDetector

        aws_credentials = {
            "AccessKeyId": "FAKE-ACCESS-KEY-ID",
            "SecretAccessKey": "FAKE-ACCESS-KEY",
            "SessionToken": "FAKE-SESSION-TOKEN",
            "Expiration": datetime.datetime.now(),
        }

        with patch("aws.osml.model_runner.inference.async_sm_detector.S3_MANAGER"):
            with patch("aws.osml.model_runner.inference.sm_detector.boto3") as mock_boto3:
                mock_client = Mock()
                mock_boto3.client.return_value = mock_client

                detector = AsyncSMDetector("test-async-endpoint", aws_credentials)

                assert detector.endpoint == "test-async-endpoint"
                mock_boto3.client.assert_called_once_with(
                    "sagemaker-runtime",
                    aws_access_key_id="FAKE-ACCESS-KEY-ID",
                    aws_secret_access_key="FAKE-ACCESS-KEY",
                    aws_session_token="FAKE-SESSION-TOKEN",
                    config=ANY,
                )

    def test_invoke_async_endpoint_success(self):
        """Test successful async endpoint invocation"""
        from aws.osml.model_runner.inference.async_sm_detector import AsyncSMDetector

        with patch("aws.osml.model_runner.inference.async_sm_detector.S3_MANAGER"):
            detector = AsyncSMDetector("test-async-endpoint")

            # Mock the SageMaker client response
            mock_response = {
                "InferenceId": "test-inference-123",
                "OutputLocation": "s3://bucket/output/test-inference-123.out",
                "FailureLocation": "s3://bucket/failures/test-inference-123.err",
            }
            detector.sm_client.invoke_endpoint_async = Mock(return_value=mock_response)

            inference_id, output_loc, failure_loc = detector._invoke_async_endpoint(
                "s3://bucket/input/test.json", None
            )

            assert inference_id == "test-inference-123"
            assert output_loc == "s3://bucket/output/test-inference-123.out"
            assert failure_loc == "s3://bucket/failures/test-inference-123.err"

            detector.sm_client.invoke_endpoint_async.assert_called_once()

    def test_invoke_async_endpoint_no_inference_id(self):
        """Test async endpoint invocation when no inference ID is returned"""
        from aws.osml.model_runner.inference.async_sm_detector import AsyncSMDetector

        with patch("aws.osml.model_runner.inference.async_sm_detector.S3_MANAGER"):
            detector = AsyncSMDetector("test-async-endpoint")

            # Mock response without InferenceId
            mock_response = {"OutputLocation": "s3://bucket/output/test.out"}
            detector.sm_client.invoke_endpoint_async = Mock(return_value=mock_response)

            with pytest.raises(ProcessTileException, match="No inference ID returned"):
                detector._invoke_async_endpoint("s3://bucket/input/test.json", None)

    def test_invoke_async_endpoint_client_error(self):
        """Test async endpoint invocation with ClientError"""
        from aws.osml.model_runner.inference.async_sm_detector import AsyncSMDetector

        with patch("aws.osml.model_runner.inference.async_sm_detector.S3_MANAGER"):
            detector = AsyncSMDetector("test-async-endpoint")

            # Mock ClientError
            error_response = {"Error": {"Code": "ValidationException", "Message": "Invalid input"}}
            detector.sm_client.invoke_endpoint_async = Mock(
                side_effect=ClientError(error_response, "invoke_endpoint_async")
            )

            with pytest.raises(ClientError):
                detector._invoke_async_endpoint("s3://bucket/input/test.json", None)


class TestAsyncSMDetectorBuilder(TestCase):
    """Unit tests for AsyncSMDetectorBuilder class"""

    def test_build_success(self):
        """Test successful AsyncSMDetector build"""
        from aws.osml.model_runner.inference.async_sm_detector import AsyncSMDetectorBuilder

        with patch("aws.osml.model_runner.inference.async_sm_detector.S3_MANAGER"):
            with patch("aws.osml.model_runner.inference.sm_detector.boto3"):
                builder = AsyncSMDetectorBuilder("test-endpoint")
                detector = builder.build()

                assert detector is not None
                assert detector.endpoint == "test-endpoint"

    def test_build_failure_returns_none(self):
        """Test that build returns None on failure"""
        from aws.osml.model_runner.inference.async_sm_detector import AsyncSMDetectorBuilder

        with patch("aws.osml.model_runner.inference.async_sm_detector.AsyncSMDetector") as mock_detector:
            mock_detector.side_effect = Exception("Build failed")

            builder = AsyncSMDetectorBuilder("test-endpoint")
            detector = builder.build()

            assert detector is None


if __name__ == "__main__":
    unittest.main()
