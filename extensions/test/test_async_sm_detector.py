#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

from io import BytesIO
from unittest.mock import Mock, patch

import geojson
import pytest
from aws_embedded_metrics.logger.metrics_logger import MetricsLogger
from osml_extensions.detectors.async_sm_detector import AsyncSMDetector


class TestAsyncSMDetector:
    """Test suite for AsyncSMDetector class."""

    @pytest.fixture
    def mock_sm_client(self):
        """Create a mock SageMaker client."""
        mock_client = Mock()
        mock_response = {"Body": Mock(), "ResponseMetadata": {"RetryAttempts": 0}}
        mock_client.invoke_endpoint.return_value = mock_response
        return mock_client

    @pytest.fixture
    def sample_features(self):
        """Create sample feature collection for testing."""
        return geojson.FeatureCollection(
            [geojson.Feature(geometry=geojson.Point((0, 0)), properties={"confidence": 0.9, "class": "test"})]
        )

    @pytest.fixture
    def sample_payload(self):
        """Create sample payload for testing."""
        data = b"test image data"
        return BytesIO(data)

    @pytest.fixture
    def mock_metrics(self):
        """Create mock metrics logger."""
        return Mock(spec=MetricsLogger)

    def test_init_default_config(self):
        """Test AsyncSMDetector initialization."""
        detector = AsyncSMDetector("test-endpoint")
        assert detector.endpoint == "test-endpoint"

    def test_init_with_credentials(self):
        """Test AsyncSMDetector initialization with assumed credentials."""
        credentials = {"AccessKeyId": "test-key", "SecretAccessKey": "test-secret", "SessionToken": "test-token"}

        with patch("boto3.client") as mock_boto3:
            AsyncSMDetector("test-endpoint", assumed_credentials=credentials)

            mock_boto3.assert_called_once()
            call_args = mock_boto3.call_args
            assert call_args[0][0] == "sagemaker-runtime"
            assert call_args[1]["aws_access_key_id"] == "test-key"

    def test_add_processing_metadata(self, sample_features):
        """Test adding processing metadata to features."""
        detector = AsyncSMDetector("test-endpoint")

        result = detector._add_processing_metadata(sample_features)

        # Should add processed_by metadata
        for feature in result["features"]:
            assert feature["properties"]["processed_by"] == "AsyncSMDetector"

    @patch("osml_extensions.detectors.async_sm_detector.geojson.loads")
    def test_find_features_success(self, mock_geojson_loads, mock_sm_client, sample_payload, sample_features, mock_metrics):
        """Test successful feature detection."""
        # Setup mocks
        mock_geojson_loads.return_value = sample_features

        detector = AsyncSMDetector("test-endpoint")
        detector.sm_client = mock_sm_client

        result = detector.find_features(sample_payload, mock_metrics)

        # Verify SageMaker client was called
        mock_sm_client.invoke_endpoint.assert_called_once_with(EndpointName="test-endpoint", Body=sample_payload)

        # Verify result has postprocessing metadata
        for feature in result["features"]:
            assert feature["properties"]["processed_by"] == "AsyncSMDetector"

    def test_mode_property(self):
        """Test that mode property returns correct value."""
        from aws.osml.model_runner.api import ModelInvokeMode

        detector = AsyncSMDetector("test-endpoint")
        assert detector.mode == ModelInvokeMode.SM_ENDPOINT

    def test_inheritance_from_sm_detector(self):
        """Test that AsyncSMDetector properly inherits from SMDetector."""
        from aws.osml.model_runner.inference.sm_detector import SMDetector

        detector = AsyncSMDetector("test-endpoint")
        assert isinstance(detector, SMDetector)
        assert hasattr(detector, "endpoint")
        assert hasattr(detector, "request_count")
        assert hasattr(detector, "mode")
        assert hasattr(detector, "find_features")
