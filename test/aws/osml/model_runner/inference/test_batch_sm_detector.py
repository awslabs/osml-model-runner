#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

import datetime
import unittest
from unittest import TestCase
from unittest.mock import Mock, patch

import boto3
import pytest
from botocore.exceptions import ClientError
from botocore.stub import ANY


class TestBatchSMDetector(TestCase):
    """Unit tests for BatchSMDetector class"""

    def test_construct_with_credentials(self):
        """Test BatchSMDetector construction with AWS credentials"""
        from aws.osml.model_runner.inference.batch_sm_detector import BatchSMDetector

        aws_credentials = {
            "AccessKeyId": "FAKE-ACCESS-KEY-ID",
            "SecretAccessKey": "FAKE-ACCESS-KEY",
            "SessionToken": "FAKE-SESSION-TOKEN",
            "Expiration": datetime.datetime.now(),
        }

        with patch("aws.osml.model_runner.inference.batch_sm_detector.S3_MANAGER"):
            with patch("aws.osml.model_runner.inference.batch_sm_detector.boto3") as mock_boto3:
                mock_sm_runtime = Mock()
                mock_sagemaker = Mock()

                def client_side_effect(service, **kwargs):
                    if service == "sagemaker-runtime":
                        return mock_sm_runtime
                    elif service == "sagemaker":
                        return mock_sagemaker
                    return Mock()

                mock_boto3.client.side_effect = client_side_effect

                detector = BatchSMDetector("test-batch-model", aws_credentials)

                assert detector.endpoint == "test-batch-model"
                # Verify sagemaker client was created with credentials
                assert mock_boto3.client.call_count == 1

    def test_submit_batch_job_success(self):
        """Test successful batch transform job submission"""
        from aws.osml.model_runner.inference.batch_sm_detector import BatchSMDetector

        with patch("aws.osml.model_runner.inference.batch_sm_detector.S3_MANAGER"):
            with patch("aws.osml.model_runner.inference.batch_sm_detector.boto3"):
                detector = BatchSMDetector("test-batch-model")

                # Mock the sagemaker client
                mock_response = {"TransformJobArn": "arn:aws:sagemaker:us-west-2:123456789:transform-job/test-job"}
                detector.sagemaker_client.create_transform_job = Mock(return_value=mock_response)

                detector._submit_batch_job(
                    transform_job_name="test-transform-job",
                    input_s3_uri="s3://bucket/input/",
                    output_s3_uri="s3://bucket/output/",
                    instance_type="ml.m5.xlarge",
                    instance_count=1,
                )

                # Verify the transform job was created
                detector.sagemaker_client.create_transform_job.assert_called_once()
                call_args = detector.sagemaker_client.create_transform_job.call_args[1]

                assert call_args["TransformJobName"] == "test-transform-job"
                assert call_args["ModelName"] == "test-batch-model"
                assert call_args["TransformInput"]["DataSource"]["S3DataSource"]["S3Uri"] == "s3://bucket/input/"
                assert call_args["TransformOutput"]["S3OutputPath"] == "s3://bucket/output/"
                assert call_args["TransformResources"]["InstanceType"] == "ml.m5.xlarge"
                assert call_args["TransformResources"]["InstanceCount"] == 1


class TestBatchSMDetectorBuilder(TestCase):
    """Unit tests for BatchSMDetectorBuilder class"""

    def test_build_success(self):
        """Test successful BatchSMDetector build"""
        from aws.osml.model_runner.inference.batch_sm_detector import BatchSMDetectorBuilder

        with patch("aws.osml.model_runner.inference.batch_sm_detector.S3_MANAGER"):
            with patch("aws.osml.model_runner.inference.batch_sm_detector.boto3"):
                builder = BatchSMDetectorBuilder("test-batch-model")
                detector = builder.build()

                assert detector is not None
                assert detector.endpoint == "test-batch-model"

    def test_build_failure_returns_none(self):
        """Test that build returns None on failure"""
        from aws.osml.model_runner.inference.batch_sm_detector import BatchSMDetectorBuilder

        with patch("aws.osml.model_runner.inference.batch_sm_detector.BatchSMDetector") as mock_detector:
            mock_detector.side_effect = Exception("Build failed")

            builder = BatchSMDetectorBuilder("test-batch-model")
            detector = builder.build()

            assert detector is None


if __name__ == "__main__":
    unittest.main()
