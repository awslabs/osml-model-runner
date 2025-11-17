#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

import unittest
from io import BytesIO
from unittest import TestCase
from unittest.mock import Mock, patch, MagicMock

import pytest
from botocore.exceptions import ClientError


class TestS3Manager(TestCase):
    """Unit tests for S3Manager utility class"""

    def test_s3_manager_initialization(self):
        """Test S3Manager initialization"""
        from aws.osml.model_runner.utilities.s3_manager import S3Manager

        with patch("aws.osml.model_runner.utilities.s3_manager.boto3"):
            with patch("aws.osml.model_runner.utilities.s3_manager.ServiceConfig") as mock_config:
                mock_config.input_bucket = "test-bucket"
                manager = S3Manager()
                assert manager is not None

    def test_upload_payload_success(self):
        """Test successful payload upload to S3"""
        from aws.osml.model_runner.utilities.s3_manager import S3Manager

        with patch("aws.osml.model_runner.utilities.s3_manager.boto3") as mock_boto3:
            with patch("aws.osml.model_runner.utilities.s3_manager.ServiceConfig") as mock_config:
                mock_config.input_bucket = "test-bucket"
                mock_config.async_endpoint_config = Mock(max_retries=2)
                mock_s3 = Mock()
                mock_boto3.client.return_value = mock_s3

                manager = S3Manager()
                payload = BytesIO(b"test data content")
                s3_key = "test/path/file.txt"

                result = manager.upload_payload(payload, s3_key)

                mock_s3.put_object.assert_called_once()
                assert "s3://test-bucket/test/path/file.txt" == result

    def test_upload_payload_with_retry(self):
        """Test payload upload with retry on failure"""
        from aws.osml.model_runner.utilities.s3_manager import S3Manager, S3OperationError

        with patch("aws.osml.model_runner.utilities.s3_manager.boto3") as mock_boto3:
            with patch("aws.osml.model_runner.utilities.s3_manager.ServiceConfig") as mock_config:
                with patch("aws.osml.model_runner.utilities.s3_manager.time"):
                    mock_config.input_bucket = "test-bucket"
                    mock_config.async_endpoint_config = Mock(max_retries=1)
                    mock_s3 = Mock()
                    error_response = {"Error": {"Code": "ServiceUnavailable", "Message": "Service unavailable"}}
                    mock_s3.put_object.side_effect = ClientError(error_response, "put_object")
                    mock_boto3.client.return_value = mock_s3

                    manager = S3Manager()
                    payload = BytesIO(b"test data")

                    with pytest.raises(S3OperationError):
                        manager.upload_payload(payload, "test/file.txt")

    def test_download_results_success(self):
        """Test successful results download from S3"""
        from aws.osml.model_runner.utilities.s3_manager import S3Manager

        with patch("aws.osml.model_runner.utilities.s3_manager.boto3") as mock_boto3:
            with patch("aws.osml.model_runner.utilities.s3_manager.ServiceConfig") as mock_config:
                mock_config.input_bucket = "test-bucket"
                mock_config.async_endpoint_config = Mock(max_retries=2)
                mock_s3 = Mock()
                mock_response = {"Body": Mock()}
                mock_response["Body"].read.return_value = b"downloaded content"
                mock_s3.get_object.return_value = mock_response
                mock_boto3.client.return_value = mock_s3

                manager = S3Manager()
                result = manager.download_results("s3://bucket/path/file.txt")

                assert result == b"downloaded content"
                mock_s3.get_object.assert_called_once()

    def test_download_results_client_error(self):
        """Test download with ClientError"""
        from aws.osml.model_runner.utilities.s3_manager import S3Manager, S3OperationError

        with patch("aws.osml.model_runner.utilities.s3_manager.boto3") as mock_boto3:
            with patch("aws.osml.model_runner.utilities.s3_manager.ServiceConfig") as mock_config:
                with patch("aws.osml.model_runner.utilities.s3_manager.time"):
                    mock_config.input_bucket = "test-bucket"
                    mock_config.async_endpoint_config = Mock(max_retries=1)
                    mock_s3 = Mock()
                    error_response = {"Error": {"Code": "NoSuchKey", "Message": "Key not found"}}
                    mock_s3.get_object.side_effect = ClientError(error_response, "get_object")
                    mock_boto3.client.return_value = mock_s3

                    manager = S3Manager()

                    with pytest.raises(S3OperationError):
                        manager.download_results("s3://bucket/missing/file.txt")

    def test_delete_object_success(self):
        """Test successful object deletion"""
        from aws.osml.model_runner.utilities.s3_manager import S3Manager

        with patch("aws.osml.model_runner.utilities.s3_manager.boto3") as mock_boto3:
            with patch("aws.osml.model_runner.utilities.s3_manager.ServiceConfig") as mock_config:
                mock_config.input_bucket = "test-bucket"
                mock_s3 = Mock()
                mock_boto3.client.return_value = mock_s3

                manager = S3Manager()
                manager.delete_object("s3://bucket/path/file.txt")

                mock_s3.delete_object.assert_called_once()

    def test_delete_object_handles_error(self):
        """Test delete object handles errors gracefully"""
        from aws.osml.model_runner.utilities.s3_manager import S3Manager

        with patch("aws.osml.model_runner.utilities.s3_manager.boto3") as mock_boto3:
            with patch("aws.osml.model_runner.utilities.s3_manager.ServiceConfig") as mock_config:
                mock_config.input_bucket = "test-bucket"
                mock_s3 = Mock()
                error_response = {"Error": {"Code": "NoSuchKey", "Message": "Key not found"}}
                mock_s3.delete_object.side_effect = ClientError(error_response, "delete_object")
                mock_boto3.client.return_value = mock_s3

                manager = S3Manager()
                # Should not raise exception, just log warning
                manager.delete_object("s3://bucket/path/file.txt")

    def test_validate_bucket_access_success(self):
        """Test successful bucket access validation"""
        from aws.osml.model_runner.utilities.s3_manager import S3Manager

        with patch("aws.osml.model_runner.utilities.s3_manager.boto3") as mock_boto3:
            with patch("aws.osml.model_runner.utilities.s3_manager.ServiceConfig") as mock_config:
                mock_config.input_bucket = "test-bucket"
                mock_s3 = Mock()
                mock_s3.head_bucket.return_value = {}
                mock_boto3.client.return_value = mock_s3

                manager = S3Manager()
                # Should not raise exception
                manager.validate_bucket_access()

    def test_validate_bucket_access_failure(self):
        """Test bucket access validation failure"""
        from aws.osml.model_runner.utilities.s3_manager import S3Manager, S3OperationError

        with patch("aws.osml.model_runner.utilities.s3_manager.boto3") as mock_boto3:
            with patch("aws.osml.model_runner.utilities.s3_manager.ServiceConfig") as mock_config:
                mock_config.input_bucket = "test-bucket"
                mock_s3 = Mock()
                error_response = {"Error": {"Code": "403", "Message": "Access Denied"}}
                mock_s3.head_bucket.side_effect = ClientError(error_response, "head_bucket")
                mock_boto3.client.return_value = mock_s3

                manager = S3Manager()

                with pytest.raises(S3OperationError):
                    manager.validate_bucket_access()

    def test_does_object_exist_true(self):
        """Test checking if object exists returns True"""
        from aws.osml.model_runner.utilities.s3_manager import S3Manager

        with patch("aws.osml.model_runner.utilities.s3_manager.boto3") as mock_boto3:
            with patch("aws.osml.model_runner.utilities.s3_manager.ServiceConfig") as mock_config:
                mock_config.input_bucket = "test-bucket"
                mock_s3 = Mock()
                mock_s3.head_object.return_value = {}
                mock_boto3.client.return_value = mock_s3

                manager = S3Manager()
                result = manager.does_object_exist("s3://bucket/path/file.txt")

                assert result is True
                mock_s3.head_object.assert_called_once()

    def test_does_object_exist_false(self):
        """Test checking if object exists returns False"""
        from aws.osml.model_runner.utilities.s3_manager import S3Manager

        with patch("aws.osml.model_runner.utilities.s3_manager.boto3") as mock_boto3:
            with patch("aws.osml.model_runner.utilities.s3_manager.ServiceConfig") as mock_config:
                mock_config.input_bucket = "test-bucket"
                mock_s3 = Mock()
                error_response = {"Error": {"Code": "404", "Message": "Not Found"}}
                mock_s3.head_object.side_effect = ClientError(error_response, "head_object")
                mock_boto3.client.return_value = mock_s3

                manager = S3Manager()
                result = manager.does_object_exist("s3://bucket/path/missing.txt")

                assert result is False

    def test_parse_s3_uri(self):
        """Test S3 URI parsing"""
        from aws.osml.model_runner.utilities.s3_manager import S3Manager

        with patch("aws.osml.model_runner.utilities.s3_manager.ServiceConfig") as mock_config:
            mock_config.input_bucket = "test-bucket"
            manager = S3Manager()
            bucket, key = manager.parse_s3_uri("s3://my-bucket/path/to/file.txt")

            assert bucket == "my-bucket"
            assert key == "path/to/file.txt"

    def test_s3_manager_with_assumed_credentials(self):
        """Test S3Manager initialization with assumed credentials"""
        from aws.osml.model_runner.utilities.s3_manager import S3Manager

        assumed_creds = {
            "AccessKeyId": "test-key",
            "SecretAccessKey": "test-secret",
            "SessionToken": "test-token",
        }

        with patch("aws.osml.model_runner.utilities.s3_manager.boto3") as mock_boto3:
            with patch("aws.osml.model_runner.utilities.s3_manager.ServiceConfig") as mock_config:
                mock_config.input_bucket = "test-bucket"
                manager = S3Manager(assumed_credentials=assumed_creds)

                # Verify boto3 client was created with credentials
                mock_boto3.client.assert_called_once()
                call_kwargs = mock_boto3.client.call_args[1]
                assert call_kwargs["aws_access_key_id"] == "test-key"
                assert call_kwargs["aws_secret_access_key"] == "test-secret"
                assert call_kwargs["aws_session_token"] == "test-token"

    def test_s3_manager_initialization_no_bucket(self):
        """Test S3Manager initialization fails without input bucket"""
        from aws.osml.model_runner.utilities.s3_manager import S3Manager

        with patch("aws.osml.model_runner.utilities.s3_manager.boto3"):
            with patch("aws.osml.model_runner.utilities.s3_manager.ServiceConfig") as mock_config:
                mock_config.input_bucket = None

                with pytest.raises(ValueError, match="Input .* bucket is mandatory"):
                    S3Manager()

    def test_upload_payload_unexpected_exception(self):
        """Test upload_payload with unexpected exception"""
        from aws.osml.model_runner.utilities.s3_manager import S3Manager, S3OperationError

        with patch("aws.osml.model_runner.utilities.s3_manager.boto3") as mock_boto3:
            with patch("aws.osml.model_runner.utilities.s3_manager.ServiceConfig") as mock_config:
                mock_config.input_bucket = "test-bucket"
                mock_config.async_endpoint_config = Mock(max_retries=1)
                mock_s3 = Mock()
                mock_s3.put_object.side_effect = Exception("Unexpected error")
                mock_boto3.client.return_value = mock_s3

                manager = S3Manager()
                payload = BytesIO(b"test data")

                with pytest.raises(S3OperationError, match="Unexpected error"):
                    manager.upload_payload(payload, "test/file.txt")

    def test_upload_payload_no_credentials_error(self):
        """Test upload_payload with NoCredentialsError"""
        from aws.osml.model_runner.utilities.s3_manager import S3Manager, S3OperationError
        from botocore.exceptions import NoCredentialsError

        with patch("aws.osml.model_runner.utilities.s3_manager.boto3") as mock_boto3:
            with patch("aws.osml.model_runner.utilities.s3_manager.ServiceConfig") as mock_config:
                with patch("aws.osml.model_runner.utilities.s3_manager.time"):
                    mock_config.input_bucket = "test-bucket"
                    mock_config.async_endpoint_config = Mock(max_retries=1)
                    mock_s3 = Mock()
                    mock_s3.put_object.side_effect = NoCredentialsError()
                    mock_boto3.client.return_value = mock_s3

                    manager = S3Manager()
                    payload = BytesIO(b"test data")

                    with pytest.raises(S3OperationError):
                        manager.upload_payload(payload, "test/file.txt")

    def test_download_results_unexpected_exception(self):
        """Test download_results with unexpected exception"""
        from aws.osml.model_runner.utilities.s3_manager import S3Manager, S3OperationError

        with patch("aws.osml.model_runner.utilities.s3_manager.boto3") as mock_boto3:
            with patch("aws.osml.model_runner.utilities.s3_manager.ServiceConfig") as mock_config:
                mock_config.input_bucket = "test-bucket"
                mock_config.async_endpoint_config = Mock(max_retries=1)
                mock_s3 = Mock()
                mock_s3.get_object.side_effect = Exception("Unexpected download error")
                mock_boto3.client.return_value = mock_s3

                manager = S3Manager()

                with pytest.raises(S3OperationError, match="Unexpected error"):
                    manager.download_results("s3://bucket/file.txt")

    def test_delete_object_unexpected_exception(self):
        """Test delete_object with unexpected exception"""
        from aws.osml.model_runner.utilities.s3_manager import S3Manager

        with patch("aws.osml.model_runner.utilities.s3_manager.boto3") as mock_boto3:
            with patch("aws.osml.model_runner.utilities.s3_manager.ServiceConfig") as mock_config:
                mock_config.input_bucket = "test-bucket"
                mock_s3 = Mock()
                mock_s3.delete_object.side_effect = Exception("Unexpected delete error")
                mock_boto3.client.return_value = mock_s3

                manager = S3Manager()
                # Should not raise exception, just log warning
                manager.delete_object("s3://bucket/path/file.txt")

    def test_validate_bucket_access_unexpected_exception(self):
        """Test validate_bucket_access with unexpected exception"""
        from aws.osml.model_runner.utilities.s3_manager import S3Manager, S3OperationError

        with patch("aws.osml.model_runner.utilities.s3_manager.boto3") as mock_boto3:
            with patch("aws.osml.model_runner.utilities.s3_manager.ServiceConfig") as mock_config:
                mock_config.input_bucket = "test-bucket"
                mock_s3 = Mock()
                mock_s3.head_bucket.side_effect = Exception("Unexpected validation error")
                mock_boto3.client.return_value = mock_s3

                manager = S3Manager()

                with pytest.raises(S3OperationError, match="Unexpected error"):
                    manager.validate_bucket_access()

    def test_download_from_s3_success(self):
        """Test _download_from_s3 with valid GeoJSON"""
        from aws.osml.model_runner.utilities.s3_manager import S3Manager

        with patch("aws.osml.model_runner.utilities.s3_manager.boto3") as mock_boto3:
            with patch("aws.osml.model_runner.utilities.s3_manager.ServiceConfig") as mock_config:
                mock_config.input_bucket = "test-bucket"
                mock_config.async_endpoint_config = Mock(max_retries=2)
                mock_s3 = Mock()
                
                # Mock valid GeoJSON response
                geojson_data = b'{"type": "FeatureCollection", "features": [{"type": "Feature", "geometry": {"type": "Point", "coordinates": [0, 0]}}]}'
                mock_response = {"Body": Mock()}
                mock_response["Body"].read.return_value = geojson_data
                mock_s3.get_object.return_value = mock_response
                mock_boto3.client.return_value = mock_s3

                manager = S3Manager()
                result = manager._download_from_s3("s3://bucket/output.json")

                assert result["type"] == "FeatureCollection"
                assert len(result["features"]) == 1

    def test_download_from_s3_unicode_decode_error(self):
        """Test _download_from_s3 with invalid UTF-8"""
        from aws.osml.model_runner.utilities.s3_manager import S3Manager
        from json import JSONDecodeError

        with patch("aws.osml.model_runner.utilities.s3_manager.boto3") as mock_boto3:
            with patch("aws.osml.model_runner.utilities.s3_manager.ServiceConfig") as mock_config:
                mock_config.input_bucket = "test-bucket"
                mock_config.async_endpoint_config = Mock(max_retries=2)
                mock_s3 = Mock()
                
                # Mock invalid UTF-8 data
                mock_response = {"Body": Mock()}
                mock_response["Body"].read.return_value = b'\xff\xfe invalid utf-8'
                mock_s3.get_object.return_value = mock_response
                mock_boto3.client.return_value = mock_s3

                manager = S3Manager()

                with pytest.raises(JSONDecodeError):
                    manager._download_from_s3("s3://bucket/output.json")

    def test_download_from_s3_json_decode_error(self):
        """Test _download_from_s3 with invalid JSON"""
        from aws.osml.model_runner.utilities.s3_manager import S3Manager
        from json import JSONDecodeError

        with patch("aws.osml.model_runner.utilities.s3_manager.boto3") as mock_boto3:
            with patch("aws.osml.model_runner.utilities.s3_manager.ServiceConfig") as mock_config:
                mock_config.input_bucket = "test-bucket"
                mock_config.async_endpoint_config = Mock(max_retries=2)
                mock_s3 = Mock()
                
                # Mock invalid JSON data
                mock_response = {"Body": Mock()}
                mock_response["Body"].read.return_value = b'{"invalid json'
                mock_s3.get_object.return_value = mock_response
                mock_boto3.client.return_value = mock_s3

                manager = S3Manager()

                with pytest.raises(JSONDecodeError):
                    manager._download_from_s3("s3://bucket/output.json")

    def test_parse_s3_uri_with_query_params(self):
        """Test S3 URI parsing with query parameters"""
        from aws.osml.model_runner.utilities.s3_manager import S3Manager

        with patch("aws.osml.model_runner.utilities.s3_manager.ServiceConfig") as mock_config:
            mock_config.input_bucket = "test-bucket"
            manager = S3Manager()
            bucket, key = manager.parse_s3_uri("s3://my-bucket/path/to/file.txt?versionId=abc123")

            assert bucket == "my-bucket"
            assert key == "path/to/file.txt?versionId=abc123"

if __name__ == "__main__":
    unittest.main()
