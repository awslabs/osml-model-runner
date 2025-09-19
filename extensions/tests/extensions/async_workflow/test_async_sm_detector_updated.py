#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import unittest
from io import BytesIO
from unittest.mock import MagicMock, Mock, patch

import geojson
from botocore.exceptions import ClientError
from moto import mock_s3, mock_sagemaker

from ..src.osml_extensions.config import AsyncEndpointConfig
from ..src.osml_extensions.detectors.async_sm_detector import AsyncSMDetector, AsyncSMDetectorBuilder
from ..src.osml_extensions.errors import ExtensionConfigurationError
from ..src.osml_extensions.polling import AsyncInferenceTimeoutError
from ..src.osml_extensions.s3 import S3OperationError


class TestAsyncSMDetectorUpdated(unittest.TestCase):
    """Test cases for the updated AsyncSMDetector with true async capabilities."""

    def setUp(self):
        """Set up test fixtures."""
        self.endpoint = "test-async-endpoint"
        self.assumed_credentials = {
            "AccessKeyId": "test-key",
            "SecretAccessKey": "test-secret",
            "SessionToken": "test-token",
        }

        self.async_config = AsyncEndpointConfig(
            input_bucket="test-input-bucket", output_bucket="test-output-bucket", max_wait_time=300, polling_interval=10
        )

        # Create test payload
        self.test_payload = BytesIO(b'{"test": "payload"}')

        # Create test feature collection
        self.test_feature_collection = {
            "type": "FeatureCollection",
            "features": [
                {"type": "Feature", "geometry": {"type": "Point", "coordinates": [0, 0]}, "properties": {"confidence": 0.9}}
            ],
        }

    @patch("osml_extensions.detectors.async_sm_detector.boto3")
    def test_init_with_credentials(self, mock_boto3):
        """Test AsyncSMDetector initialization with credentials."""
        mock_sm_client = Mock()
        mock_s3_client = Mock()
        mock_boto3.client.side_effect = [mock_sm_client, mock_s3_client]

        with patch("osml_extensions.detectors.async_sm_detector.S3Manager") as mock_s3_manager_class:
            with patch("osml_extensions.detectors.async_sm_detector.AsyncInferencePoller") as mock_poller_class:
                mock_s3_manager = Mock()
                mock_poller = Mock()
                mock_s3_manager_class.return_value = mock_s3_manager
                mock_poller_class.return_value = mock_poller
                mock_s3_manager.validate_bucket_access.return_value = None

                detector = AsyncSMDetector(
                    endpoint=self.endpoint, assumed_credentials=self.assumed_credentials, async_config=self.async_config
                )

                self.assertEqual(detector.endpoint, self.endpoint)
                self.assertEqual(detector.async_config, self.async_config)
                mock_s3_manager.validate_bucket_access.assert_called_once()

    @patch("osml_extensions.detectors.async_sm_detector.boto3")
    def test_init_without_credentials(self, mock_boto3):
        """Test AsyncSMDetector initialization without credentials."""
        mock_sm_client = Mock()
        mock_s3_client = Mock()
        mock_boto3.client.side_effect = [mock_sm_client, mock_s3_client]

        with patch("osml_extensions.detectors.async_sm_detector.S3Manager") as mock_s3_manager_class:
            with patch("osml_extensions.detectors.async_sm_detector.AsyncInferencePoller") as mock_poller_class:
                mock_s3_manager = Mock()
                mock_poller = Mock()
                mock_s3_manager_class.return_value = mock_s3_manager
                mock_poller_class.return_value = mock_poller
                mock_s3_manager.validate_bucket_access.return_value = None

                detector = AsyncSMDetector(endpoint=self.endpoint, async_config=self.async_config)

                self.assertEqual(detector.endpoint, self.endpoint)

    @patch("osml_extensions.detectors.async_sm_detector.boto3")
    def test_init_with_default_config(self, mock_boto3):
        """Test AsyncSMDetector initialization with default configuration."""
        mock_sm_client = Mock()
        mock_s3_client = Mock()
        mock_boto3.client.side_effect = [mock_sm_client, mock_s3_client]

        with patch("osml_extensions.detectors.async_sm_detector.S3Manager") as mock_s3_manager_class:
            with patch("osml_extensions.detectors.async_sm_detector.AsyncInferencePoller") as mock_poller_class:
                with patch("osml_extensions.detectors.async_sm_detector.AsyncEndpointConfig") as mock_config_class:
                    mock_config = Mock()
                    mock_config_class.return_value = mock_config
                    mock_s3_manager = Mock()
                    mock_poller = Mock()
                    mock_s3_manager_class.return_value = mock_s3_manager
                    mock_poller_class.return_value = mock_poller
                    mock_s3_manager.validate_bucket_access.return_value = None

                    detector = AsyncSMDetector(endpoint=self.endpoint)

                    self.assertEqual(detector.endpoint, self.endpoint)
                    mock_config_class.assert_called_once()

    @patch("osml_extensions.detectors.async_sm_detector.boto3")
    def test_find_features_success(self, mock_boto3):
        """Test successful async feature detection workflow."""
        # Setup mocks
        mock_sm_client = Mock()
        mock_s3_client = Mock()
        mock_boto3.client.side_effect = [mock_sm_client, mock_s3_client]

        with patch("osml_extensions.detectors.async_sm_detector.S3Manager") as mock_s3_manager_class:
            with patch("osml_extensions.detectors.async_sm_detector.AsyncInferencePoller") as mock_poller_class:
                # Setup S3Manager mock
                mock_s3_manager = Mock()
                mock_s3_manager_class.return_value = mock_s3_manager
                mock_s3_manager.validate_bucket_access.return_value = None
                mock_s3_manager.generate_unique_key.side_effect = ["input-key", "output-key"]
                mock_s3_manager.upload_payload.return_value = "s3://test-input-bucket/input-key"
                mock_s3_manager.download_results.return_value = geojson.dumps(self.test_feature_collection).encode("utf-8")

                # Setup AsyncInferencePoller mock
                mock_poller = Mock()
                mock_poller_class.return_value = mock_poller
                mock_poller.poll_until_complete.return_value = "s3://test-output-bucket/output-key"

                # Setup SageMaker client mock
                mock_sm_client.invoke_endpoint_async.return_value = {"InferenceId": "test-inference-123"}

                # Create detector and test
                detector = AsyncSMDetector(endpoint=self.endpoint, async_config=self.async_config)
                result = detector.find_features(self.test_payload)

                # Verify result
                self.assertEqual(result, self.test_feature_collection)

                # Verify workflow calls
                mock_s3_manager.upload_payload.assert_called_once()
                mock_sm_client.invoke_endpoint_async.assert_called_once()
                mock_poller.poll_until_complete.assert_called_once_with("test-inference-123", None)
                mock_s3_manager.download_results.assert_called_once()
                mock_s3_manager.cleanup_s3_objects.assert_called_once()

    @patch("osml_extensions.detectors.async_sm_detector.boto3")
    def test_find_features_with_metrics(self, mock_boto3):
        """Test async feature detection with metrics logging."""
        mock_sm_client = Mock()
        mock_s3_client = Mock()
        mock_boto3.client.side_effect = [mock_sm_client, mock_s3_client]
        mock_metrics = Mock()

        with patch("osml_extensions.detectors.async_sm_detector.S3Manager") as mock_s3_manager_class:
            with patch("osml_extensions.detectors.async_sm_detector.AsyncInferencePoller") as mock_poller_class:
                # Setup mocks
                mock_s3_manager = Mock()
                mock_s3_manager_class.return_value = mock_s3_manager
                mock_s3_manager.validate_bucket_access.return_value = None
                mock_s3_manager.generate_unique_key.side_effect = ["input-key", "output-key"]
                mock_s3_manager.upload_payload.return_value = "s3://test-input-bucket/input-key"
                mock_s3_manager.download_results.return_value = geojson.dumps(self.test_feature_collection).encode("utf-8")

                mock_poller = Mock()
                mock_poller_class.return_value = mock_poller
                mock_poller.poll_until_complete.return_value = "s3://test-output-bucket/output-key"

                mock_sm_client.invoke_endpoint_async.return_value = {"InferenceId": "test-inference-123"}

                detector = AsyncSMDetector(endpoint=self.endpoint, async_config=self.async_config)
                result = detector.find_features(self.test_payload, mock_metrics)

                # Verify metrics were used
                mock_metrics.set_dimensions.assert_called()
                mock_metrics.put_dimensions.assert_called()
                mock_metrics.put_metric.assert_called()

    @patch("osml_extensions.detectors.async_sm_detector.boto3")
    def test_find_features_s3_upload_error(self, mock_boto3):
        """Test async feature detection with S3 upload error."""
        mock_sm_client = Mock()
        mock_s3_client = Mock()
        mock_boto3.client.side_effect = [mock_sm_client, mock_s3_client]

        with patch("osml_extensions.detectors.async_sm_detector.S3Manager") as mock_s3_manager_class:
            with patch("osml_extensions.detectors.async_sm_detector.AsyncInferencePoller") as mock_poller_class:
                mock_s3_manager = Mock()
                mock_s3_manager_class.return_value = mock_s3_manager
                mock_s3_manager.validate_bucket_access.return_value = None
                mock_s3_manager.generate_unique_key.return_value = "input-key"
                mock_s3_manager.upload_payload.side_effect = S3OperationError("Upload failed")

                mock_poller = Mock()
                mock_poller_class.return_value = mock_poller

                detector = AsyncSMDetector(endpoint=self.endpoint, async_config=self.async_config)

                with self.assertRaises(S3OperationError):
                    detector.find_features(self.test_payload)

    @patch("osml_extensions.detectors.async_sm_detector.boto3")
    def test_find_features_async_endpoint_error(self, mock_boto3):
        """Test async feature detection with SageMaker endpoint error."""
        mock_sm_client = Mock()
        mock_s3_client = Mock()
        mock_boto3.client.side_effect = [mock_sm_client, mock_s3_client]

        with patch("osml_extensions.detectors.async_sm_detector.S3Manager") as mock_s3_manager_class:
            with patch("osml_extensions.detectors.async_sm_detector.AsyncInferencePoller") as mock_poller_class:
                mock_s3_manager = Mock()
                mock_s3_manager_class.return_value = mock_s3_manager
                mock_s3_manager.validate_bucket_access.return_value = None
                mock_s3_manager.generate_unique_key.side_effect = ["input-key", "output-key"]
                mock_s3_manager.upload_payload.return_value = "s3://test-input-bucket/input-key"

                mock_poller = Mock()
                mock_poller_class.return_value = mock_poller

                # Mock SageMaker error
                mock_sm_client.invoke_endpoint_async.side_effect = ClientError(
                    error_response={"Error": {"Code": "ValidationException", "Message": "Invalid endpoint"}},
                    operation_name="InvokeEndpointAsync",
                )

                detector = AsyncSMDetector(endpoint=self.endpoint, async_config=self.async_config)

                with self.assertRaises(ClientError):
                    detector.find_features(self.test_payload)

    @patch("osml_extensions.detectors.async_sm_detector.boto3")
    def test_find_features_polling_timeout(self, mock_boto3):
        """Test async feature detection with polling timeout."""
        mock_sm_client = Mock()
        mock_s3_client = Mock()
        mock_boto3.client.side_effect = [mock_sm_client, mock_s3_client]

        with patch("osml_extensions.detectors.async_sm_detector.S3Manager") as mock_s3_manager_class:
            with patch("osml_extensions.detectors.async_sm_detector.AsyncInferencePoller") as mock_poller_class:
                mock_s3_manager = Mock()
                mock_s3_manager_class.return_value = mock_s3_manager
                mock_s3_manager.validate_bucket_access.return_value = None
                mock_s3_manager.generate_unique_key.side_effect = ["input-key", "output-key"]
                mock_s3_manager.upload_payload.return_value = "s3://test-input-bucket/input-key"

                mock_poller = Mock()
                mock_poller_class.return_value = mock_poller
                mock_poller.poll_until_complete.side_effect = AsyncInferenceTimeoutError("Polling timed out")

                mock_sm_client.invoke_endpoint_async.return_value = {"InferenceId": "test-inference-123"}

                detector = AsyncSMDetector(endpoint=self.endpoint, async_config=self.async_config)

                with self.assertRaises(AsyncInferenceTimeoutError):
                    detector.find_features(self.test_payload)

    @patch("osml_extensions.detectors.async_sm_detector.boto3")
    def test_find_features_json_decode_error(self, mock_boto3):
        """Test async feature detection with JSON decode error."""
        mock_sm_client = Mock()
        mock_s3_client = Mock()
        mock_boto3.client.side_effect = [mock_sm_client, mock_s3_client]

        with patch("osml_extensions.detectors.async_sm_detector.S3Manager") as mock_s3_manager_class:
            with patch("osml_extensions.detectors.async_sm_detector.AsyncInferencePoller") as mock_poller_class:
                mock_s3_manager = Mock()
                mock_s3_manager_class.return_value = mock_s3_manager
                mock_s3_manager.validate_bucket_access.return_value = None
                mock_s3_manager.generate_unique_key.side_effect = ["input-key", "output-key"]
                mock_s3_manager.upload_payload.return_value = "s3://test-input-bucket/input-key"
                mock_s3_manager.download_results.return_value = b"invalid json"

                mock_poller = Mock()
                mock_poller_class.return_value = mock_poller
                mock_poller.poll_until_complete.return_value = "s3://test-output-bucket/output-key"

                mock_sm_client.invoke_endpoint_async.return_value = {"InferenceId": "test-inference-123"}

                detector = AsyncSMDetector(endpoint=self.endpoint, async_config=self.async_config)

                with self.assertRaises(JSONDecodeError):
                    detector.find_features(self.test_payload)


class TestAsyncSMDetectorBuilder(unittest.TestCase):
    """Test cases for AsyncSMDetectorBuilder."""

    def setUp(self):
        """Set up test fixtures."""
        self.endpoint = "test-async-endpoint"
        self.assumed_credentials = {
            "AccessKeyId": "test-key",
            "SecretAccessKey": "test-secret",
            "SessionToken": "test-token",
        }
        self.async_config = AsyncEndpointConfig(input_bucket="test-input-bucket", output_bucket="test-output-bucket")

    def test_builder_initialization(self):
        """Test AsyncSMDetectorBuilder initialization."""
        builder = AsyncSMDetectorBuilder(
            endpoint=self.endpoint, assumed_credentials=self.assumed_credentials, async_config=self.async_config
        )

        self.assertEqual(builder.endpoint, self.endpoint)
        self.assertEqual(builder.assumed_credentials, self.assumed_credentials)
        self.assertEqual(builder.async_config, self.async_config)

    def test_builder_initialization_defaults(self):
        """Test AsyncSMDetectorBuilder initialization with defaults."""
        builder = AsyncSMDetectorBuilder(endpoint=self.endpoint)

        self.assertEqual(builder.endpoint, self.endpoint)
        self.assertEqual(builder.assumed_credentials, {})
        self.assertIsNone(builder.async_config)

    def test_validate_parameters_success(self):
        """Test successful parameter validation."""
        builder = AsyncSMDetectorBuilder(
            endpoint=self.endpoint, assumed_credentials=self.assumed_credentials, async_config=self.async_config
        )

        # Should not raise exception
        builder._validate_parameters()

    def test_validate_parameters_missing_endpoint(self):
        """Test parameter validation with missing endpoint."""
        builder = AsyncSMDetectorBuilder(endpoint="")

        with self.assertRaises(ExtensionConfigurationError) as context:
            builder._validate_parameters()

        self.assertIn("Endpoint name is required", str(context.exception))

    def test_validate_parameters_invalid_endpoint_type(self):
        """Test parameter validation with invalid endpoint type."""
        builder = AsyncSMDetectorBuilder(endpoint=123)

        with self.assertRaises(ExtensionConfigurationError) as context:
            builder._validate_parameters()

        self.assertIn("Endpoint name must be a string", str(context.exception))

    def test_validate_parameters_invalid_credentials_type(self):
        """Test parameter validation with invalid credentials type."""
        builder = AsyncSMDetectorBuilder(endpoint=self.endpoint, assumed_credentials="invalid")

        with self.assertRaises(ExtensionConfigurationError) as context:
            builder._validate_parameters()

        self.assertIn("Assumed credentials must be a dictionary", str(context.exception))

    def test_validate_parameters_invalid_config_type(self):
        """Test parameter validation with invalid config type."""
        builder = AsyncSMDetectorBuilder(endpoint=self.endpoint, async_config="invalid")

        with self.assertRaises(ExtensionConfigurationError) as context:
            builder._validate_parameters()

        self.assertIn("async_config must be an AsyncEndpointConfig instance", str(context.exception))

    @patch("osml_extensions.detectors.async_sm_detector.AsyncSMDetector")
    def test_build_success(self, mock_detector_class):
        """Test successful detector building."""
        mock_detector = Mock()
        mock_detector_class.return_value = mock_detector

        builder = AsyncSMDetectorBuilder(
            endpoint=self.endpoint, assumed_credentials=self.assumed_credentials, async_config=self.async_config
        )

        result = builder.build()

        self.assertEqual(result, mock_detector)
        mock_detector_class.assert_called_once_with(
            endpoint=self.endpoint, assumed_credentials=self.assumed_credentials, async_config=self.async_config
        )

    @patch("osml_extensions.detectors.async_sm_detector.AsyncSMDetector")
    def test_build_with_exception(self, mock_detector_class):
        """Test detector building with exception."""
        mock_detector_class.side_effect = Exception("Build failed")

        builder = AsyncSMDetectorBuilder(endpoint=self.endpoint)

        result = builder.build()

        # Should return None on exception
        self.assertIsNone(result)

    @patch("osml_extensions.detectors.async_sm_detector.AsyncEndpointConfig")
    def test_from_environment(self, mock_config_class):
        """Test creating builder from environment."""
        mock_config = Mock()
        mock_config_class.from_environment.return_value = mock_config

        builder = AsyncSMDetectorBuilder.from_environment(
            endpoint=self.endpoint, assumed_credentials=self.assumed_credentials
        )

        self.assertEqual(builder.endpoint, self.endpoint)
        self.assertEqual(builder.assumed_credentials, self.assumed_credentials)
        self.assertEqual(builder.async_config, mock_config)
        mock_config_class.from_environment.assert_called_once()


class TestAsyncSMDetectorResourceManagement(unittest.TestCase):
    """Test cases for AsyncSMDetector resource management functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.endpoint = "test-async-endpoint"
        self.async_config = AsyncEndpointConfig(
            input_bucket="test-input-bucket",
            output_bucket="test-output-bucket",
            cleanup_enabled=True,
            cleanup_policy="immediate",
        )

        # Create test payload
        self.test_payload = BytesIO(b'{"test": "payload"}')

    @patch("osml_extensions.detectors.async_sm_detector.boto3")
    @patch("osml_extensions.detectors.async_sm_detector.ResourceManager")
    def test_resource_manager_initialization(self, mock_resource_manager_class, mock_boto3):
        """Test that ResourceManager is properly initialized."""
        mock_resource_manager = Mock()
        mock_resource_manager_class.return_value = mock_resource_manager

        detector = AsyncSMDetector(endpoint=self.endpoint, async_config=self.async_config)

        # Verify ResourceManager was created and started
        mock_resource_manager_class.assert_called_once_with(self.async_config, detector.s3_client)
        mock_resource_manager.start_cleanup_worker.assert_called_once()
        self.assertEqual(detector.resource_manager, mock_resource_manager)

    @patch("osml_extensions.detectors.async_sm_detector.boto3")
    def test_cleanup_resources_method(self, mock_boto3):
        """Test cleanup_resources method."""
        detector = AsyncSMDetector(endpoint=self.endpoint, async_config=self.async_config)

        with patch.object(detector.resource_manager, "cleanup_all_resources", return_value=5) as mock_cleanup:
            result = detector.cleanup_resources(force=True)

            self.assertEqual(result, 5)
            mock_cleanup.assert_called_once_with(force=True)

    @patch("osml_extensions.detectors.async_sm_detector.boto3")
    def test_get_resource_stats_method(self, mock_boto3):
        """Test get_resource_stats method."""
        detector = AsyncSMDetector(endpoint=self.endpoint, async_config=self.async_config)

        expected_stats = {"total_resources": 3, "by_type": {}}
        with patch.object(detector.resource_manager, "get_resource_stats", return_value=expected_stats) as mock_stats:
            result = detector.get_resource_stats()

            self.assertEqual(result, expected_stats)
            mock_stats.assert_called_once()

    @patch("osml_extensions.detectors.async_sm_detector.boto3")
    def test_context_manager_functionality(self, mock_boto3):
        """Test AsyncSMDetector as context manager."""
        detector = AsyncSMDetector(endpoint=self.endpoint, async_config=self.async_config)

        with patch.object(detector, "cleanup_resources") as mock_cleanup, patch.object(
            detector.resource_manager, "stop_cleanup_worker"
        ) as mock_stop:

            with detector as ctx_detector:
                self.assertEqual(ctx_detector, detector)

            mock_cleanup.assert_called_once_with(force=True)
            mock_stop.assert_called_once()

    @patch("osml_extensions.detectors.async_sm_detector.boto3")
    def test_destructor_cleanup(self, mock_boto3):
        """Test destructor performs cleanup."""
        detector = AsyncSMDetector(endpoint=self.endpoint, async_config=self.async_config)

        with patch.object(detector.resource_manager, "cleanup_all_resources") as mock_cleanup, patch.object(
            detector.resource_manager, "stop_cleanup_worker"
        ) as mock_stop:

            # Trigger destructor
            del detector

            mock_cleanup.assert_called_once_with(force=True)
            mock_stop.assert_called_once_with(timeout=5.0)

    @patch("osml_extensions.detectors.async_sm_detector.boto3")
    @patch("osml_extensions.detectors.async_sm_detector.CleanupPolicy")
    def test_resource_registration_in_find_features(self, mock_cleanup_policy_class, mock_boto3):
        """Test that resources are registered during find_features."""
        mock_cleanup_policy = Mock()
        mock_cleanup_policy_class.return_value = mock_cleanup_policy

        detector = AsyncSMDetector(endpoint=self.endpoint, async_config=self.async_config)

        # Mock all the dependencies
        with patch.object(
            detector, "_upload_to_s3", return_value="s3://input-bucket/input-key"
        ) as mock_upload, patch.object(
            detector, "_invoke_async_endpoint", return_value="inference-123"
        ) as mock_invoke, patch.object(
            detector, "_poll_for_completion", return_value="s3://output-bucket/output-key"
        ) as mock_poll, patch.object(
            detector, "_download_from_s3", return_value=self.test_feature_collection
        ) as mock_download, patch.object(
            detector.s3_manager, "generate_unique_key", side_effect=["input-key", "output-key"]
        ) as mock_key_gen, patch.object(
            detector.resource_manager, "register_s3_object"
        ) as mock_register_s3, patch.object(
            detector.resource_manager, "register_inference_job"
        ) as mock_register_job:

            result = detector.find_features(self.test_payload)

            # Verify resources were registered
            self.assertEqual(mock_register_s3.call_count, 2)  # Input and output S3 objects
            mock_register_job.assert_called_once()

            # Verify the result
            self.assertEqual(result, self.test_feature_collection)

    @patch("osml_extensions.detectors.async_sm_detector.boto3")
    def test_failed_job_cleanup(self, mock_boto3):
        """Test cleanup of failed job resources."""
        detector = AsyncSMDetector(endpoint=self.endpoint, async_config=self.async_config)

        # Mock dependencies to simulate failure
        with patch.object(detector, "_upload_to_s3", return_value="s3://input-bucket/input-key"), patch.object(
            detector, "_invoke_async_endpoint", return_value="inference-123"
        ), patch.object(detector, "_poll_for_completion", side_effect=AsyncInferenceTimeoutError("Timeout")), patch.object(
            detector.s3_manager, "generate_unique_key", side_effect=["input-key", "output-key"]
        ), patch.object(
            detector.resource_manager, "register_s3_object"
        ), patch.object(
            detector.resource_manager, "register_inference_job"
        ), patch.object(
            detector.resource_manager, "cleanup_failed_job_resources"
        ) as mock_cleanup_failed:

            with self.assertRaises(AsyncInferenceTimeoutError):
                detector.find_features(self.test_payload)

            # Verify failed job cleanup was called
            mock_cleanup_failed.assert_called_once_with("inference-123")


if __name__ == "__main__":
    unittest.main()
