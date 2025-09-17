#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import pytest
import json
from io import BufferedReader, BytesIO
from unittest.mock import Mock, patch, MagicMock
import asyncio

import geojson
from aws_embedded_metrics.logger.metrics_logger import MetricsLogger
from botocore.exceptions import ClientError

from osml_extensions.detectors.async_sm_detector import AsyncSMDetector


class TestAsyncSMDetector:
    """Test suite for AsyncSMDetector class."""

    @pytest.fixture
    def mock_sm_client(self):
        """Create a mock SageMaker client."""
        mock_client = Mock()
        mock_response = {
            'Body': Mock(),
            'ResponseMetadata': {'RetryAttempts': 0}
        }
        mock_client.invoke_endpoint.return_value = mock_response
        return mock_client

    @pytest.fixture
    def sample_features(self):
        """Create sample feature collection for testing."""
        return geojson.FeatureCollection([
            geojson.Feature(
                geometry=geojson.Point((0, 0)),
                properties={'confidence': 0.9, 'class': 'test'}
            ),
            geojson.Feature(
                geometry=geojson.Point((1, 1)),
                properties={'confidence': 0.7, 'class': 'test'}
            ),
            geojson.Feature(
                geometry=geojson.Point((2, 2)),
                properties={'confidence': 0.3, 'class': 'test'}
            )
        ])

    @pytest.fixture
    def sample_payload(self):
        """Create sample payload for testing."""
        data = b"test image data"
        return BufferedReader(BytesIO(data))

    @pytest.fixture
    def mock_metrics(self):
        """Create mock metrics logger."""
        return Mock(spec=MetricsLogger)

    def test_init_default_config(self):
        """Test AsyncSMDetector initialization with default configuration."""
        detector = AsyncSMDetector("test-endpoint")
        
        assert detector.endpoint == "test-endpoint"
        assert detector.custom_config == {}
        assert detector.preprocessing_enabled is True
        assert detector.postprocessing_enabled is True
        assert detector.timeout_multiplier == 1.0
        assert detector.custom_parameters == {}

    def test_init_custom_config(self):
        """Test AsyncSMDetector initialization with custom configuration."""
        custom_config = {
            'preprocessing_enabled': False,
            'postprocessing_enabled': True,
            'timeout_multiplier': 2.0,
            'custom_parameters': {'confidence_threshold': 0.8}
        }
        
        detector = AsyncSMDetector("test-endpoint", custom_config=custom_config)
        
        assert detector.preprocessing_enabled is False
        assert detector.postprocessing_enabled is True
        assert detector.timeout_multiplier == 2.0
        assert detector.custom_parameters['confidence_threshold'] == 0.8

    def test_init_with_credentials(self):
        """Test AsyncSMDetector initialization with assumed credentials."""
        credentials = {
            'AccessKeyId': 'test-key',
            'SecretAccessKey': 'test-secret',
            'SessionToken': 'test-token'
        }
        
        with patch('boto3.client') as mock_boto3:
            detector = AsyncSMDetector("test-endpoint", assumed_credentials=credentials)
            
            mock_boto3.assert_called_once()
            call_args = mock_boto3.call_args
            assert call_args[0][0] == "sagemaker-runtime"
            assert call_args[1]['aws_access_key_id'] == 'test-key'
            assert call_args[1]['aws_secret_access_key'] == 'test-secret'
            assert call_args[1]['aws_session_token'] == 'test-token'

    def test_preprocess_payload_enabled(self, sample_payload):
        """Test payload preprocessing when enabled."""
        detector = AsyncSMDetector("test-endpoint", custom_config={'preprocessing_enabled': True})
        
        result = detector._preprocess_payload(sample_payload)
        
        # Should return the same payload for now (no actual preprocessing implemented)
        assert result == sample_payload

    def test_preprocess_payload_disabled(self, sample_payload):
        """Test payload preprocessing when disabled."""
        detector = AsyncSMDetector("test-endpoint", custom_config={'preprocessing_enabled': False})
        
        result = detector._preprocess_payload(sample_payload)
        
        assert result == sample_payload

    def test_preprocess_payload_exception(self, sample_payload):
        """Test payload preprocessing handles exceptions gracefully."""
        detector = AsyncSMDetector("test-endpoint")
        
        # Mock the preprocessing to raise an exception
        with patch.object(detector, '_preprocess_payload', side_effect=Exception("Test error")) as mock_preprocess:
            # Should not raise exception, should return original payload
            result = detector._preprocess_payload(sample_payload)
            assert result == sample_payload

    def test_postprocess_features_enabled(self, sample_features):
        """Test feature postprocessing when enabled."""
        detector = AsyncSMDetector("test-endpoint", custom_config={'postprocessing_enabled': True})
        
        result = detector._postprocess_features(sample_features)
        
        # Should add processed_by metadata
        for feature in result['features']:
            assert feature['properties']['processed_by'] == 'AsyncSMDetector'
            assert 'processing_timestamp' in feature['properties']

    def test_postprocess_features_disabled(self, sample_features):
        """Test feature postprocessing when disabled."""
        detector = AsyncSMDetector("test-endpoint", custom_config={'postprocessing_enabled': False})
        
        result = detector._postprocess_features(sample_features)
        
        assert result == sample_features

    def test_postprocess_features_confidence_filtering(self, sample_features):
        """Test feature postprocessing with confidence threshold filtering."""
        custom_config = {
            'postprocessing_enabled': True,
            'custom_parameters': {'confidence_threshold': 0.8}
        }
        detector = AsyncSMDetector("test-endpoint", custom_config=custom_config)
        
        result = detector._postprocess_features(sample_features)
        
        # Should only keep features with confidence >= 0.8
        assert len(result['features']) == 1
        assert result['features'][0]['properties']['confidence'] == 0.9

    def test_postprocess_features_exception(self, sample_features):
        """Test feature postprocessing handles exceptions gracefully."""
        detector = AsyncSMDetector("test-endpoint")
        
        # Mock postprocessing to raise an exception
        with patch.object(detector, '_postprocess_features', side_effect=Exception("Test error")):
            result = detector._postprocess_features(sample_features)
            assert result == sample_features

    @patch('osml_extensions.detectors.async_sm_detector.geojson.loads')
    def test_find_features_success(self, mock_geojson_loads, mock_sm_client, sample_payload, 
                                  sample_features, mock_metrics):
        """Test successful feature detection."""
        # Setup mocks
        mock_geojson_loads.return_value = sample_features
        mock_sm_client.invoke_endpoint.return_value = {
            'Body': Mock(),
            'ResponseMetadata': {'RetryAttempts': 0}
        }
        
        detector = AsyncSMDetector("test-endpoint")
        detector.sm_client = mock_sm_client
        
        result = detector.find_features(sample_payload, mock_metrics)
        
        # Verify SageMaker client was called
        mock_sm_client.invoke_endpoint.assert_called_once_with(
            EndpointName="test-endpoint",
            Body=sample_payload
        )
        
        # Verify metrics were recorded
        mock_metrics.put_metric.assert_called()
        
        # Verify result has postprocessing metadata
        for feature in result['features']:
            assert feature['properties']['processed_by'] == 'AsyncSMDetector'

    @patch('osml_extensions.detectors.async_sm_detector.geojson.loads')
    def test_find_features_client_error(self, mock_geojson_loads, mock_sm_client, 
                                       sample_payload, mock_metrics):
        """Test feature detection with SageMaker client error."""
        # Setup client error
        error_response = {
            'Error': {'Code': 'ValidationException'},
            'ResponseMetadata': {'HTTPStatusCode': 400}
        }
        mock_sm_client.invoke_endpoint.side_effect = ClientError(error_response, 'InvokeEndpoint')
        
        detector = AsyncSMDetector("test-endpoint")
        detector.sm_client = mock_sm_client
        
        with pytest.raises(ClientError):
            detector.find_features(sample_payload, mock_metrics)
        
        # Verify error metric was recorded
        mock_metrics.put_metric.assert_called()

    @patch('osml_extensions.detectors.async_sm_detector.geojson.loads')
    def test_find_features_json_decode_error(self, mock_geojson_loads, mock_sm_client, 
                                           sample_payload, mock_metrics):
        """Test feature detection with JSON decode error."""
        # Setup JSON decode error
        mock_geojson_loads.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)
        mock_sm_client.invoke_endpoint.return_value = {
            'Body': Mock(),
            'ResponseMetadata': {'RetryAttempts': 0}
        }
        
        detector = AsyncSMDetector("test-endpoint")
        detector.sm_client = mock_sm_client
        
        with pytest.raises(json.JSONDecodeError):
            detector.find_features(sample_payload, mock_metrics)
        
        # Verify error metric was recorded
        mock_metrics.put_metric.assert_called()

    @pytest.mark.asyncio
    async def test_find_features_async(self, sample_payload, sample_features, mock_metrics):
        """Test asynchronous feature detection."""
        detector = AsyncSMDetector("test-endpoint")
        
        # Mock the synchronous find_features method
        with patch.object(detector, 'find_features', return_value=sample_features) as mock_find:
            result = await detector.find_features_async(sample_payload, mock_metrics)
            
            mock_find.assert_called_once_with(sample_payload, mock_metrics)
            assert result == sample_features

    def test_mode_property(self):
        """Test that mode property returns correct value."""
        from aws.osml.model_runner.api import ModelInvokeMode
        
        detector = AsyncSMDetector("test-endpoint")
        assert detector.mode == ModelInvokeMode.SM_ENDPOINT

    def test_request_count_increment(self, mock_sm_client, sample_payload, 
                                   sample_features, mock_metrics):
        """Test that request count is incremented on each call."""
        detector = AsyncSMDetector("test-endpoint")
        detector.sm_client = mock_sm_client
        
        # Mock the response
        mock_sm_client.invoke_endpoint.return_value = {
            'Body': Mock(),
            'ResponseMetadata': {'RetryAttempts': 0}
        }
        
        with patch('osml_extensions.detectors.async_sm_detector.geojson.loads', return_value=sample_features):
            initial_count = detector.request_count
            detector.find_features(sample_payload, mock_metrics)
            assert detector.request_count == initial_count + 1

    def test_inheritance_from_sm_detector(self):
        """Test that AsyncSMDetector properly inherits from SMDetector."""
        from aws.osml.model_runner.inference.sm_detector import SMDetector
        
        detector = AsyncSMDetector("test-endpoint")
        assert isinstance(detector, SMDetector)
        assert hasattr(detector, 'endpoint')
        assert hasattr(detector, 'request_count')
        assert hasattr(detector, 'mode')
        assert hasattr(detector, 'find_features')