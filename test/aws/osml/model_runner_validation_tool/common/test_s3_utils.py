#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

import json
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from aws.osml.model_runner_validation_tool.common.s3_utils import S3Utils


@pytest.fixture
def s3_test_data():
    """Fixture providing common test data"""
    return {
        "bucket_name": "test-bucket",
        "key": "test-key",
        "prefix": "test-prefix",
        "test_data": {"test": "data"},
        "test_bytes": b"test bytes",
    }


class TestS3Utils:
    """Test cases for the S3Utils class"""

    @patch("boto3.client")
    def test_init_with_region(self, mock_boto_client):
        """Test initialization with region name"""
        region_name = "us-west-2"
        s3_utils = S3Utils(region_name=region_name)

        mock_boto_client.assert_called_once_with("s3", region_name=region_name)
        assert s3_utils.s3_client is not None

    @patch("boto3.client")
    def test_init_without_region(self, mock_boto_client):
        """Test initialization without region name"""
        s3_utils = S3Utils()

        mock_boto_client.assert_called_once_with("s3")
        assert s3_utils.s3_client is not None

    @patch("boto3.client")
    def test_list_objects_success(self, mock_boto_client, s3_test_data):
        """Test listing objects successfully"""
        # Setup mock
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {
            "Contents": [{"Key": "test-key1"}, {"Key": "test-key2"}, {"Key": "test-key3"}]
        }

        # Create S3Utils with the mocked boto3 client
        s3_utils = S3Utils()

        # Call the method
        result = s3_utils.list_objects(s3_test_data["bucket_name"], s3_test_data["prefix"])

        # Assertions
        mock_s3.list_objects_v2.assert_called_once_with(Bucket=s3_test_data["bucket_name"], Prefix=s3_test_data["prefix"])
        assert result == ["test-key1", "test-key2", "test-key3"]

    @patch("boto3.client")
    def test_list_objects_empty(self, mock_boto_client, s3_test_data):
        """Test listing objects with empty result"""
        # Setup mock
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {}

        # Create S3Utils with the mocked boto3 client
        s3_utils = S3Utils()

        # Call the method
        result = s3_utils.list_objects(s3_test_data["bucket_name"], s3_test_data["prefix"])

        # Assertions
        mock_s3.list_objects_v2.assert_called_once_with(Bucket=s3_test_data["bucket_name"], Prefix=s3_test_data["prefix"])
        assert result == []

    @patch("boto3.client")
    def test_list_objects_error(self, mock_boto_client, s3_test_data):
        """Test listing objects with error"""
        # Setup mock
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_s3.list_objects_v2.side_effect = ClientError(
            {"Error": {"Code": "NoSuchBucket", "Message": "The bucket does not exist"}}, "ListObjectsV2"
        )

        # Create S3Utils with the mocked boto3 client
        s3_utils = S3Utils()

        # Call the method
        result = s3_utils.list_objects(s3_test_data["bucket_name"], s3_test_data["prefix"])

        # Assertions
        mock_s3.list_objects_v2.assert_called_once_with(Bucket=s3_test_data["bucket_name"], Prefix=s3_test_data["prefix"])
        assert result == []

    @patch("boto3.client")
    def test_get_object_success(self, mock_boto_client, s3_test_data):
        """Test getting an object successfully"""
        # Setup mock
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_body = MagicMock()
        mock_body.read.return_value = s3_test_data["test_bytes"]
        mock_s3.get_object.return_value = {"Body": mock_body}

        # Create S3Utils with the mocked boto3 client
        s3_utils = S3Utils()

        # Call the method
        result, error = s3_utils.get_object(s3_test_data["bucket_name"], s3_test_data["key"])

        # Assertions
        mock_s3.get_object.assert_called_once_with(Bucket=s3_test_data["bucket_name"], Key=s3_test_data["key"])
        assert result == s3_test_data["test_bytes"]
        assert error is None

    @patch("boto3.client")
    def test_get_object_error(self, mock_boto_client, s3_test_data):
        """Test getting an object with error"""
        # Setup mock
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_s3.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "The specified key does not exist"}}, "GetObject"
        )

        # Create S3Utils with the mocked boto3 client
        s3_utils = S3Utils()

        # Call the method
        result, error = s3_utils.get_object(s3_test_data["bucket_name"], s3_test_data["key"])

        # Assertions
        mock_s3.get_object.assert_called_once_with(Bucket=s3_test_data["bucket_name"], Key=s3_test_data["key"])
        assert result is None
        assert "Error downloading file" in error
        assert "NoSuchKey" in error

    @patch("boto3.client")
    def test_put_object_success_with_string(self, mock_boto_client, s3_test_data):
        """Test putting an object successfully with string data"""
        # Setup mock
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        data = "test string data"
        content_type = "text/plain"

        # Create S3Utils with the mocked boto3 client
        s3_utils = S3Utils()

        # Call the method
        result = s3_utils.put_object(s3_test_data["bucket_name"], s3_test_data["key"], data, content_type)

        # Assertions
        mock_s3.put_object.assert_called_once_with(
            Bucket=s3_test_data["bucket_name"], Key=s3_test_data["key"], Body=data, ContentType=content_type
        )
        assert result is True

    @patch("boto3.client")
    def test_put_object_success_with_dict(self, mock_boto_client, s3_test_data):
        """Test putting an object successfully with dict data"""
        # Setup mock
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        # Create S3Utils with the mocked boto3 client
        s3_utils = S3Utils()

        # Call the method
        result = s3_utils.put_object(s3_test_data["bucket_name"], s3_test_data["key"], s3_test_data["test_data"])

        # Assertions
        mock_s3.put_object.assert_called_once_with(
            Bucket=s3_test_data["bucket_name"],
            Key=s3_test_data["key"],
            Body=json.dumps(s3_test_data["test_data"], indent=2),
            ContentType="application/json",
        )
        assert result is True

    @patch("boto3.client")
    def test_put_object_error(self, mock_boto_client, s3_test_data):
        """Test putting an object with error"""
        # Setup mock
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_s3.put_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}, "PutObject"
        )

        # Create S3Utils with the mocked boto3 client
        s3_utils = S3Utils()

        # Call the method
        result = s3_utils.put_object(s3_test_data["bucket_name"], s3_test_data["key"], s3_test_data["test_data"])

        # Assertions
        mock_s3.put_object.assert_called_once()
        assert result is False

    @patch("boto3.client")
    def test_save_test_results(self, mock_boto_client, s3_test_data):
        """Test saving test results"""
        # Setup mock
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        # Create S3Utils with the mocked boto3 client
        s3_utils = S3Utils()

        # Mock the put_object method directly
        s3_utils.put_object = MagicMock(return_value=True)

        model_name = "test-model"
        test_type = "test-type"

        # Call the method
        result = s3_utils.save_test_results(s3_test_data["bucket_name"], model_name, s3_test_data["test_data"], test_type)

        # Assertions
        s3_utils.put_object.assert_called_once()
        assert result is True

    @patch("boto3.client")
    def test_save_test_results_no_bucket(self, mock_boto_client, s3_test_data):
        """Test saving test results with no bucket"""
        # Setup
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        # Create S3Utils with the mocked boto3 client
        s3_utils = S3Utils()

        model_name = "test-model"
        test_type = "test-type"

        # Call the method with empty bucket
        result = s3_utils.save_test_results("", model_name, s3_test_data["test_data"], test_type)

        # Assertions
        mock_s3.put_object.assert_not_called()
        assert result is False
