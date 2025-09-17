#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import pytest
import os
import json
from io import BufferedReader, BytesIO
from unittest.mock import Mock, patch, MagicMock

import geojson
from aws_embedded_metrics.logger.metrics_logger import MetricsLogger

from aws.osml.model_runner.api import ModelInvokeMode
from osml_extensions.factory.enhanced_factory import EnhancedFeatureDetectorFactory
from osml_extensions.config.extension_config import ExtensionConfig
from osml_extensions.detectors.async_sm_detector import AsyncSMDetector


class TestIntegration:
    """Integration tests for the full extension pipeline."""

    @pytest.fixture
    def sample_payload(self):
        """Create sample payload for testing."""
        data = b"test image data"
        return BufferedReader(BytesIO(data))

    @pytest.fixture
    def sample_features(self):
        """Create sample feature collection for testing."""
        return geojson.FeatureCollection([
            geojson.Feature(
                geometry=geojson.Point((0, 0)),
                properties={'confidence': 0.9, 'class': 'test'}
            )
        ])

    @pytest.fixture
    def mock_metrics(self):
        """Create mock metrics logger."""
        return Mock(spec=MetricsLogger)

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

    def test_full_pipeline_with_extensions_enabled(self, sample_payload, sample_features, 
                                                  mock_metrics, mock_sm_client):
        """Test complete pipeline with extensions enabled."""
        # Set environment variables for extensions
        with patch.dict(os.environ, {
            'USE_EXTENSIONS': 'true',
            'EXTENSION_CONFIG': json.dumps({
                'preprocessing_enabled': True,
                'postprocessing_enabled': True,
                'custom_parameters': {'confidence_threshold': 0.8}
            })
        }):
            # Mock SageMaker response
            with patch('osml_extensions.detectors.async_sm_detector.geojson.loads', return_value=sample_features):
                with patch('boto3.client', return_value=mock_sm_client):
                    # Create factory from environment
                    factory = EnhancedFeatureDetectorFactory.create_from_environment(
                        endpoint="test-endpoint",
                        endpoint_mode=ModelInvokeMode.SM_ENDPOINT
                    )
                    
                    # Build detector
                    detector = factory.build()
                    
                    # Verify we got an AsyncSMDetector
                    assert detector is not None
                    assert isinstance(detector, AsyncSMDetector)
                    assert detector.preprocessing_enabled is True
                    assert detector.postprocessing_enabled is True
                    
                    # Run feature detection
                    result = detector.find_features(sample_payload, mock_metrics)
                    
                    # Verify results have postprocessing metadata
                    assert result is not None
                    assert 'features' in result
                    for feature in result['features']:
                        assert feature['properties']['processed_by'] == 'AsyncSMDetector'
                        assert 'processing_timestamp' in feature['properties']

    def test_full_pipeline_with_extensions_disabled(self, sample_payload, sample_features, 
                                                   mock_metrics, mock_sm_client):
        """Test complete pipeline with extensions disabled."""
        # Set environment variables to disable extensions
        with patch.dict(os.environ, {
            'USE_EXTENSIONS': 'false'
        }):
            with patch('aws.osml.model_runner.inference.endpoint_factory.SMDetectorBuilder') as mock_base_builder:
                mock_base_detector = Mock()
                mock_base_detector.find_features.return_value = sample_features
                mock_base_builder.return_value.build.return_value = mock_base_detector
                
                # Create factory from environment
                factory = EnhancedFeatureDetectorFactory.create_from_environment(
                    endpoint="test-endpoint",
                    endpoint_mode=ModelInvokeMode.SM_ENDPOINT
                )
                
                # Build detector
                detector = factory.build()
                
                # Verify we got the base detector, not AsyncSMDetector
                assert detector is not None
                assert detector == mock_base_detector
                assert not isinstance(detector, AsyncSMDetector)

    def test_extension_fallback_on_import_error(self, sample_payload, sample_features, 
                                              mock_metrics, mock_sm_client):
        """Test fallback to base implementation when extension import fails."""
        with patch.dict(os.environ, {'USE_EXTENSIONS': 'true'}):
            # Mock import error for AsyncSMDetector
            with patch('osml_extensions.factory.enhanced_factory.AsyncSMDetectorBuilder', 
                      side_effect=ImportError("Module not found")):
                with patch('aws.osml.model_runner.inference.endpoint_factory.SMDetectorBuilder') as mock_base_builder:
                    mock_base_detector = Mock()
                    mock_base_detector.find_features.return_value = sample_features
                    mock_base_builder.return_value.build.return_value = mock_base_detector
                    
                    factory = EnhancedFeatureDetectorFactory.create_from_environment(
                        endpoint="test-endpoint",
                        endpoint_mode=ModelInvokeMode.SM_ENDPOINT
                    )
                    
                    detector = factory.build()
                    
                    # Should fall back to base detector
                    assert detector is not None
                    assert detector == mock_base_detector

    def test_configuration_validation_and_fallback(self, sample_payload, sample_features, 
                                                  mock_metrics, mock_sm_client):
        """Test configuration validation and fallback to defaults."""
        # Set invalid configuration
        invalid_config = json.dumps({
            'preprocessing_enabled': 'invalid',  # Should be boolean
            'timeout_multiplier': -1             # Should be positive
        })
        
        with patch.dict(os.environ, {
            'USE_EXTENSIONS': 'true',
            'EXTENSION_CONFIG': invalid_config
        }):
            with patch('osml_extensions.detectors.async_sm_detector.geojson.loads', return_value=sample_features):
                with patch('boto3.client', return_value=mock_sm_client):
                    factory = EnhancedFeatureDetectorFactory.create_from_environment(
                        endpoint="test-endpoint",
                        endpoint_mode=ModelInvokeMode.SM_ENDPOINT
                    )
                    
                    detector = factory.build()
                    
                    # Should still create detector with default config
                    assert detector is not None
                    assert isinstance(detector, AsyncSMDetector)
                    # Should use default values due to invalid config
                    assert detector.preprocessing_enabled is True  # Default
                    assert detector.timeout_multiplier == 1.0     # Default

    def test_environment_variable_parsing(self):
        """Test various environment variable formats are parsed correctly."""
        test_cases = [
            ('true', True),
            ('True', True),
            ('TRUE', True),
            ('1', True),
            ('yes', True),
            ('on', True),
            ('enabled', True),
            ('false', False),
            ('False', False),
            ('FALSE', False),
            ('0', False),
            ('no', False),
            ('off', False),
            ('disabled', False),
            ('invalid', True)  # Should default to True for invalid values
        ]
        
        for env_value, expected in test_cases:
            with patch.dict(os.environ, {'USE_EXTENSIONS': env_value}):
                result = ExtensionConfig.use_extensions()
                assert result == expected, f"Failed for env_value: {env_value}"

    def test_json_config_parsing(self):
        """Test JSON configuration parsing from environment."""
        valid_config = {
            'preprocessing_enabled': False,
            'postprocessing_enabled': True,
            'custom_parameters': {'confidence_threshold': 0.9}
        }
        
        with patch.dict(os.environ, {
            'EXTENSION_CONFIG': json.dumps(valid_config)
        }):
            result = ExtensionConfig.get_extension_config()
            assert result == valid_config

    def test_invalid_json_config_fallback(self):
        """Test fallback when JSON configuration is invalid."""
        with patch.dict(os.environ, {
            'EXTENSION_CONFIG': 'invalid json'
        }):
            result = ExtensionConfig.get_extension_config()
            assert result == {}

    def test_extension_logging_configuration(self):
        """Test extension logging configuration."""
        with patch.dict(os.environ, {
            'EXTENSION_LOG_LEVEL': 'DEBUG'
        }):
            with patch('logging.getLogger') as mock_get_logger:
                mock_logger = Mock()
                mock_get_logger.return_value = mock_logger
                
                ExtensionConfig.set_extension_logging()
                
                mock_get_logger.assert_called_with('osml_extensions')
                mock_logger.setLevel.assert_called()

    def test_factory_info_collection(self):
        """Test factory information collection for debugging."""
        custom_config = {'preprocessing_enabled': False}
        credentials = {'AccessKeyId': 'test-key'}
        
        with patch('osml_extensions.factory.enhanced_factory.ExtensionConfig') as mock_config:
            mock_config.get_config_summary.return_value = {'test': 'summary'}
            mock_config.use_extensions.return_value = True
            mock_config.get_extension_config.return_value = custom_config
            mock_config.validate_config.return_value = True
            
            factory = EnhancedFeatureDetectorFactory(
                endpoint="test-endpoint",
                endpoint_mode=ModelInvokeMode.SM_ENDPOINT,
                use_extensions=True,
                extension_config=custom_config,
                assumed_credentials=credentials
            )
            
            info = factory.get_factory_info()
            
            assert info['endpoint'] == 'test-endpoint'
            assert info['endpoint_mode'] == 'SM_ENDPOINT'
            assert info['use_extensions'] is True
            assert info['extension_config'] == custom_config
            assert info['has_credentials'] is True
            assert info['extension_config_summary'] == {'test': 'summary'}

    def test_extension_availability_check(self):
        """Test extension availability checking."""
        factory = EnhancedFeatureDetectorFactory(
            endpoint="test-endpoint",
            endpoint_mode=ModelInvokeMode.SM_ENDPOINT
        )
        
        # AsyncSMDetector should be available
        assert factory.is_extension_available('AsyncSMDetector') is True
        
        # Unknown extension should not be available
        assert factory.is_extension_available('UnknownExtension') is False

    def test_confidence_threshold_filtering_integration(self, sample_payload, mock_metrics, mock_sm_client):
        """Test confidence threshold filtering in full pipeline."""
        # Create features with different confidence levels
        features_with_confidence = geojson.FeatureCollection([
            geojson.Feature(
                geometry=geojson.Point((0, 0)),
                properties={'confidence': 0.9, 'class': 'high_confidence'}
            ),
            geojson.Feature(
                geometry=geojson.Point((1, 1)),
                properties={'confidence': 0.7, 'class': 'medium_confidence'}
            ),
            geojson.Feature(
                geometry=geojson.Point((2, 2)),
                properties={'confidence': 0.3, 'class': 'low_confidence'}
            )
        ])
        
        config = {
            'preprocessing_enabled': True,
            'postprocessing_enabled': True,
            'custom_parameters': {'confidence_threshold': 0.8}
        }
        
        with patch.dict(os.environ, {
            'USE_EXTENSIONS': 'true',
            'EXTENSION_CONFIG': json.dumps(config)
        }):
            with patch('osml_extensions.detectors.async_sm_detector.geojson.loads', 
                      return_value=features_with_confidence):
                with patch('boto3.client', return_value=mock_sm_client):
                    factory = EnhancedFeatureDetectorFactory.create_from_environment(
                        endpoint="test-endpoint",
                        endpoint_mode=ModelInvokeMode.SM_ENDPOINT
                    )
                    
                    detector = factory.build()
                    result = detector.find_features(sample_payload, mock_metrics)
                    
                    # Should only keep features with confidence >= 0.8
                    assert len(result['features']) == 1
                    assert result['features'][0]['properties']['confidence'] == 0.9
                    assert result['features'][0]['properties']['class'] == 'high_confidence'

    def test_async_feature_detection(self, sample_payload, sample_features, mock_metrics, mock_sm_client):
        """Test asynchronous feature detection capability."""
        with patch('osml_extensions.detectors.async_sm_detector.geojson.loads', return_value=sample_features):
            with patch('boto3.client', return_value=mock_sm_client):
                detector = AsyncSMDetector("test-endpoint")
                
                # Test async method
                import asyncio
                
                async def run_async_test():
                    result = await detector.find_features_async(sample_payload, mock_metrics)
                    return result
                
                # Run the async test
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    result = loop.run_until_complete(run_async_test())
                    assert result is not None
                    assert 'features' in result
                finally:
                    loop.close()

    def test_docker_environment_simulation(self, sample_payload, sample_features, 
                                         mock_metrics, mock_sm_client):
        """Test simulation of Docker container environment."""
        # Simulate Docker environment variables
        docker_env = {
            'USE_EXTENSIONS': 'true',
            'EXTENSION_CONFIG': json.dumps({
                'preprocessing_enabled': True,
                'postprocessing_enabled': True,
                'timeout_multiplier': 1.5
            }),
            'EXTENSION_LOG_LEVEL': 'INFO'
        }
        
        with patch.dict(os.environ, docker_env):
            with patch('osml_extensions.detectors.async_sm_detector.geojson.loads', return_value=sample_features):
                with patch('boto3.client', return_value=mock_sm_client):
                    # This simulates what would happen in the Docker container
                    factory = EnhancedFeatureDetectorFactory.create_from_environment(
                        endpoint="test-endpoint",
                        endpoint_mode=ModelInvokeMode.SM_ENDPOINT
                    )
                    
                    detector = factory.build()
                    
                    assert detector is not None
                    assert isinstance(detector, AsyncSMDetector)
                    assert detector.timeout_multiplier == 1.5
                    
                    # Verify the detector works end-to-end
                    result = detector.find_features(sample_payload, mock_metrics)
                    assert result is not None