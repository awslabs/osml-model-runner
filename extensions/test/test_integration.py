#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import os
from io import BytesIO
from unittest.mock import Mock, patch

import geojson
import pytest
from aws_embedded_metrics.logger.metrics_logger import MetricsLogger
from osml_extensions.detectors.async_sm_detector import AsyncSMDetector
from osml_extensions.factory.enhanced_factory import EnhancedFeatureDetectorFactory

from aws.osml.model_runner.api import ModelInvokeMode


class TestIntegration:
    """Integration tests for the full extension pipeline."""

    @pytest.fixture
    def sample_payload(self):
        """Create sample payload for testing."""
        data = b"test image data"
        return BytesIO(data)

    @pytest.fixture
    def sample_features(self):
        """Create sample feature collection for testing."""
        return geojson.FeatureCollection(
            [geojson.Feature(geometry=geojson.Point((0, 0)), properties={"confidence": 0.9, "class": "test"})]
        )

    @pytest.fixture
    def mock_metrics(self):
        """Create mock metrics logger."""
        return Mock(spec=MetricsLogger)

    @pytest.fixture
    def mock_sm_client(self):
        """Create a mock SageMaker client."""
        mock_client = Mock()
        mock_response = {"Body": Mock(), "ResponseMetadata": {"RetryAttempts": 0}}
        mock_client.invoke_endpoint.return_value = mock_response
        return mock_client

    def test_full_pipeline_with_extensions_enabled(self, sample_payload, sample_features, mock_metrics, mock_sm_client):
        """Test complete pipeline with extensions enabled."""
        # Set environment variables for extensions
        with patch.dict(os.environ, {"USE_EXTENSIONS": "true"}):
            # Mock SageMaker response
            with patch("osml_extensions.detectors.async_sm_detector.geojson.loads", return_value=sample_features):
                with patch("boto3.client", return_value=mock_sm_client):
                    # Create factory
                    factory = EnhancedFeatureDetectorFactory(
                        endpoint="test-endpoint", endpoint_mode=ModelInvokeMode.SM_ENDPOINT
                    )

                    # Build detector
                    detector = factory.build()

                    # Verify we got an AsyncSMDetector
                    assert detector is not None
                    assert isinstance(detector, AsyncSMDetector)

                    # Run feature detection
                    result = detector.find_features(sample_payload, mock_metrics)

                    # Verify results have postprocessing metadata
                    assert result is not None
                    assert "features" in result
                    for feature in result["features"]:
                        assert feature["properties"]["processed_by"] == "AsyncSMDetector"

    def test_full_pipeline_with_extensions_disabled(self, sample_payload, sample_features, mock_metrics, mock_sm_client):
        """Test complete pipeline with extensions disabled."""
        # Set environment variables to disable extensions
        with patch.dict(os.environ, {"USE_EXTENSIONS": "false"}):
            with patch("aws.osml.model_runner.inference.endpoint_factory.SMDetectorBuilder") as mock_base_builder:
                mock_base_detector = Mock()
                mock_base_detector.find_features.return_value = sample_features
                mock_base_builder.return_value.build.return_value = mock_base_detector

                # Create factory
                factory = EnhancedFeatureDetectorFactory(endpoint="test-endpoint", endpoint_mode=ModelInvokeMode.SM_ENDPOINT)

                # Build detector
                detector = factory.build()

                # Verify we got the base detector, not AsyncSMDetector
                assert detector is not None
                assert detector == mock_base_detector
                assert not isinstance(detector, AsyncSMDetector)
