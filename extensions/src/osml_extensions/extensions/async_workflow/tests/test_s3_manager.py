#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import unittest
from io import BytesIO
from unittest.mock import Mock, patch

import boto3
from botocore.exceptions import ClientError
from moto import mock_s3

from ..src.osml_extensions.config import AsyncEndpointConfig
from ..src.osml_extensions.s3 import S3Manager, S3OperationError


class TestS3Manager(unittest.TestCase):
    """Test cases for S3Manager."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = AsyncEndpointConfig(
            input_bucket="test-input-bucket",
            output_bucket="test-output-bucket",
            input_prefix="test-input/",
            output_prefix="test-output/",
            max_retries=2,
        )

        self.mock_s3_client = Mock()
        self.s3_manager = S3Manager(self.mock_s3_client, self.config)

        # Test payload
        self.test_payload = BytesIO(b'{"test": "data"}')
        self.test_key = "test-key"

    def test_upload_payload_success(self):
        """Test successful payload upload."""
        # Mock successful upload
        self.mock_s3_client.put_object.return_value = {}

        result_uri = self.s3_manager.upload_payload(self.test_payload, self.test_key)

        expected_uri = "s3://test-input-bucket/test-input/test-key"
        self.assertEqual(result_uri, expected_uri)

        # Verify S3 client was called correctly
        self.mock_s3_client.put_object.assert_called_once_with(
            Bucket="test-input-bucket", Key="test-input/test-key", Body=b'{"test": "data"}', ContentType="application/json"
        )

    def test_upload_payload_with_metrics(self):
        """Test payload upload with metrics logging."""
        mock_metrics = Mock()
        self.mock_s3_client.put_object.return_value = {}

        result_uri = self.s3_manager.upload_payload(self.test_payload, self.test_key, mock_metrics)

        # Verify metrics were logged
        mock_metrics.put_dimensions.assert_called()
        mock_metrics.put_metric.assert_called()

        expected_uri = "s3://test-input-bucket/test-input/test-key"
        self.assertEqual(result_uri, expected_uri)

    def test_upload_payload_retry_success(self):
        """Test payload upload with retry after initial failure."""
        # Mock first call fails, second succeeds
        self.mock_s3_client.put_object.side_effect = [
            ClientError(
                error_response={"Error": {"Code": "ServiceUnavailable", "Message": "Service unavailable"}},
                operation_name="PutObject",
            ),
            {},
        ]

        with patch("time.sleep"):  # Mock sleep to speed up test
            result_uri = self.s3_manager.upload_payload(self.test_payload, self.test_key)

        expected_uri = "s3://test-input-bucket/test-input/test-key"
        self.assertEqual(result_uri, expected_uri)
        self.assertEqual(self.mock_s3_client.put_object.call_count, 2)

    def test_upload_payload_max_retries_exceeded(self):
        """Test payload upload failure after max retries."""
        # Mock all calls fail
        self.mock_s3_client.put_object.side_effect = ClientError(
            error_response={"Error": {"Code": "AccessDenied", "Message": "Access denied"}}, operation_name="PutObject"
        )

        with patch("time.sleep"):  # Mock sleep to speed up test
            with self.assertRaises(S3OperationError) as context:
                self.s3_manager.upload_payload(self.test_payload, self.test_key)

        self.assertIn("Failed to upload payload to S3 after 3 attempts", str(context.exception))
        self.assertEqual(self.mock_s3_client.put_object.call_count, 3)  # max_retries + 1

    def test_download_results_success(self):
        """Test successful results download."""
        test_data = b'{"results": "data"}'
        mock_response = {"Body": BytesIO(test_data)}
        self.mock_s3_client.get_object.return_value = mock_response

        s3_uri = "s3://test-output-bucket/test-output/test-key"
        result_data = self.s3_manager.download_results(s3_uri)

        self.assertEqual(result_data, test_data)
        self.mock_s3_client.get_object.assert_called_once_with(Bucket="test-output-bucket", Key="test-output/test-key")

    def test_download_results_with_metrics(self):
        """Test results download with metrics logging."""
        test_data = b'{"results": "data"}'
        mock_response = {"Body": BytesIO(test_data)}
        self.mock_s3_client.get_object.return_value = mock_response
        mock_metrics = Mock()

        s3_uri = "s3://test-output-bucket/test-output/test-key"
        result_data = self.s3_manager.download_results(s3_uri, mock_metrics)

        self.assertEqual(result_data, test_data)
        mock_metrics.put_dimensions.assert_called()
        mock_metrics.put_metric.assert_called()

    def test_download_results_retry_success(self):
        """Test results download with retry after initial failure."""
        test_data = b'{"results": "data"}'
        mock_response = {"Body": BytesIO(test_data)}

        # Mock first call fails, second succeeds
        self.mock_s3_client.get_object.side_effect = [
            ClientError(
                error_response={"Error": {"Code": "NoSuchKey", "Message": "Key not found"}}, operation_name="GetObject"
            ),
            mock_response,
        ]

        with patch("time.sleep"):  # Mock sleep to speed up test
            s3_uri = "s3://test-output-bucket/test-output/test-key"
            result_data = self.s3_manager.download_results(s3_uri)

        self.assertEqual(result_data, test_data)
        self.assertEqual(self.mock_s3_client.get_object.call_count, 2)

    def test_download_results_max_retries_exceeded(self):
        """Test results download failure after max retries."""
        # Mock all calls fail
        self.mock_s3_client.get_object.side_effect = ClientError(
            error_response={"Error": {"Code": "AccessDenied", "Message": "Access denied"}}, operation_name="GetObject"
        )

        with patch("time.sleep"):  # Mock sleep to speed up test
            with self.assertRaises(S3OperationError) as context:
                s3_uri = "s3://test-output-bucket/test-output/test-key"
                self.s3_manager.download_results(s3_uri)

        self.assertIn("Failed to download results from S3 after 3 attempts", str(context.exception))
        self.assertEqual(self.mock_s3_client.get_object.call_count, 3)  # max_retries + 1

    def test_delete_object_success(self):
        """Test successful object deletion."""
        self.mock_s3_client.delete_object.return_value = {}

        s3_uri = "s3://test-bucket/test-key"
        self.s3_manager.delete_object(s3_uri)

        self.mock_s3_client.delete_object.assert_called_once_with(Bucket="test-bucket", Key="test-key")

    def test_delete_object_cleanup_disabled(self):
        """Test object deletion when cleanup is disabled."""
        self.config.cleanup_enabled = False

        s3_uri = "s3://test-bucket/test-key"
        self.s3_manager.delete_object(s3_uri)

        # Should not call S3 client when cleanup is disabled
        self.mock_s3_client.delete_object.assert_not_called()

    def test_delete_object_error_handling(self):
        """Test object deletion error handling."""
        self.mock_s3_client.delete_object.side_effect = ClientError(
            error_response={"Error": {"Code": "NoSuchKey", "Message": "Key not found"}}, operation_name="DeleteObject"
        )

        # Should not raise exception, just log warning
        s3_uri = "s3://test-bucket/test-key"
        self.s3_manager.delete_object(s3_uri)

        self.mock_s3_client.delete_object.assert_called_once()

    def test_generate_unique_key(self):
        """Test unique key generation."""
        key1 = self.s3_manager.generate_unique_key()
        key2 = self.s3_manager.generate_unique_key()

        # Keys should be different
        self.assertNotEqual(key1, key2)

        # Keys should contain timestamp and UUID components
        self.assertRegex(key1, r"\d{8}_\d{6}_[a-f0-9]{8}")
        self.assertRegex(key2, r"\d{8}_\d{6}_[a-f0-9]{8}")

    def test_generate_unique_key_with_prefix(self):
        """Test unique key generation with prefix."""
        prefix = "test-prefix"
        key = self.s3_manager.generate_unique_key(prefix)

        self.assertTrue(key.startswith(f"{prefix}_"))
        self.assertRegex(key, rf"{prefix}_\d{{8}}_\d{{6}}_[a-f0-9]{{8}}")

    def test_cleanup_s3_objects(self):
        """Test cleanup of multiple S3 objects."""
        s3_uris = ["s3://test-bucket/key1", "s3://test-bucket/key2", "s3://test-bucket/key3"]

        self.mock_s3_client.delete_object.return_value = {}

        self.s3_manager.cleanup_s3_objects(s3_uris)

        self.assertEqual(self.mock_s3_client.delete_object.call_count, 3)

    def test_cleanup_s3_objects_disabled(self):
        """Test cleanup when disabled."""
        self.config.cleanup_enabled = False

        s3_uris = ["s3://test-bucket/key1", "s3://test-bucket/key2"]
        self.s3_manager.cleanup_s3_objects(s3_uris)

        self.mock_s3_client.delete_object.assert_not_called()

    def test_validate_bucket_access_success(self):
        """Test successful bucket access validation."""
        self.mock_s3_client.head_bucket.return_value = {}

        # Should not raise exception
        self.s3_manager.validate_bucket_access()

        # Should check both buckets
        expected_calls = [unittest.mock.call(Bucket="test-input-bucket"), unittest.mock.call(Bucket="test-output-bucket")]
        self.mock_s3_client.head_bucket.assert_has_calls(expected_calls)

    def test_validate_bucket_access_failure(self):
        """Test bucket access validation failure."""
        self.mock_s3_client.head_bucket.side_effect = ClientError(
            error_response={"Error": {"Code": "NoSuchBucket", "Message": "Bucket not found"}}, operation_name="HeadBucket"
        )

        with self.assertRaises(S3OperationError) as context:
            self.s3_manager.validate_bucket_access()

        self.assertIn("S3 bucket access validation failed", str(context.exception))


@mock_s3
class TestS3ManagerIntegration(unittest.TestCase):
    """Integration tests for S3Manager using moto."""

    def setUp(self):
        """Set up test fixtures with real S3 client."""
        self.config = AsyncEndpointConfig(
            input_bucket="test-input-bucket",
            output_bucket="test-output-bucket",
            input_prefix="test-input/",
            output_prefix="test-output/",
        )

        # Create real S3 client with moto
        self.s3_client = boto3.client("s3", region_name="us-east-1")
        self.s3_manager = S3Manager(self.s3_client, self.config)

        # Create test buckets
        self.s3_client.create_bucket(Bucket="test-input-bucket")
        self.s3_client.create_bucket(Bucket="test-output-bucket")

        self.test_payload = BytesIO(b'{"test": "integration_data"}')
        self.test_key = "integration-test-key"

    def test_upload_download_integration(self):
        """Test complete upload and download workflow."""
        # Upload payload
        s3_uri = self.s3_manager.upload_payload(self.test_payload, self.test_key)

        expected_uri = "s3://test-input-bucket/test-input/integration-test-key"
        self.assertEqual(s3_uri, expected_uri)

        # Verify object exists in S3
        response = self.s3_client.get_object(Bucket="test-input-bucket", Key="test-input/integration-test-key")
        uploaded_data = response["Body"].read()
        self.assertEqual(uploaded_data, b'{"test": "integration_data"}')

        # Test download
        downloaded_data = self.s3_manager.download_results(s3_uri)
        self.assertEqual(downloaded_data, b'{"test": "integration_data"}')

    def test_validate_bucket_access_integration(self):
        """Test bucket access validation with real buckets."""
        # Should succeed with existing buckets
        self.s3_manager.validate_bucket_access()

        # Should fail with non-existent bucket
        bad_config = AsyncEndpointConfig(input_bucket="non-existent-bucket", output_bucket="test-output-bucket")
        bad_manager = S3Manager(self.s3_client, bad_config)

        with self.assertRaises(S3OperationError):
            bad_manager.validate_bucket_access()


if __name__ == "__main__":
    unittest.main()
