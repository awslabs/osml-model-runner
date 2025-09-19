#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import os
import unittest
from io import BufferedReader, BytesIO
from unittest.mock import Mock, patch, MagicMock

from aws_embedded_metrics.logger.metrics_logger import MetricsLogger

from ..src.osml_extensions.detectors import AsyncSMDetector
from ..src.osml_extensions.errors import ExtensionRuntimeError


class TestAsyncSMDetector(unittest.TestCase):
    """Test cases for AsyncSMDetector."""

    def setUp(self):
        """Set up test fixtures."""
        self.endpoint = "test-endpoint"
        self.credentials = {"access_key": "test", "secret_key": "test"}
        self.test_payload = BufferedReader(BytesIO(b"test image data"))
        self.test_features = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"confidence": 0.9},
                    "geometry": {"type": "Point", "coordinates": [0, 0]}
                }
            ]
        }

    def test_init(self):
        """Test AsyncSMDetector initialization."""
        detector = AsyncSMDetector(self.endpoint, self.credentials)
        
        self.assertEqual(detector.endpoint, self.endpoint)
        self.assertEqual(detector.assumed_credentials, self.credentials)
        self.assertTrue(hasattr(detector, 'enhanced_processing_enabled'))

    @patch.dict(os.environ, {"ENHANCED_MONITORING_ENABLED": "true"})
    def test_should_enable_enhanced_processing_true(self):
        """Test enhanced processing enablement from config."""
        detector = AsyncSMDetector(self.endpoint, self.credentials)
        self.assertTrue(detector._should_enable_enhanced_processing())

    @patch.dict(os.environ, {"ENHANCED_MONITORING_ENABLED": "false"})
    def test_should_enable_enhanced_processing_false(self):
        """Test enhanced processing disablement from config."""
        detector = AsyncSMDetector(self.endpoint, self.credentials)
        self.assertFalse(detector._should_enable_enhanced_processing())

    def test_add_enhanced_metrics_enabled(self):
        """Test enhanced metrics addition when enabled."""
        detector = AsyncSMDetector(self.endpoint, self.credentials)
        detector.enhanced_processing_enabled = True
        
        mock_metrics = Mock(spec=MetricsLogger)
        detector._add_enhanced_metrics(mock_metrics)
        
        # Verify metrics were added
        mock_metrics.put_metric.assert_called()
        mock_metrics.put_dimensions.assert_called()

    def test_add_enhanced_metrics_disabled(self):
        """Test enhanced metrics addition when disabled."""
        detector = AsyncSMDetector(self.endpoint, self.credentials)
        detector.enhanced_processing_enabled = False
        
        mock_metrics = Mock(spec=MetricsLogger)
        detector._add_enhanced_metrics(mock_metrics)
        
        # Verify no metrics were added
        mock_metrics.put_metric.assert_not_called()
        mock_metrics.put_dimensions.assert_not_called()

    def test_add_enhanced_metrics_invalid_logger(self):
        """Test enhanced metrics addition with invalid logger."""
        detector = AsyncSMDetector(self.endpoint, self.credentials)
        detector.enhanced_processing_enabled = True
        
        # Should not raise exception with invalid logger
        detector._add_enhanced_metrics(None)
        detector._add_enhanced_metrics("invalid")

    @patch('osml_extensions.detectors.async_sm_detector.super')
    def test_find_features_success(self, mock_super):
        """Test successful feature detection."""
        mock_super.return_value.find_features.return_value = self.test_features
        
        detector = AsyncSMDetector(self.endpoint, self.credentials)
        detector.enhanced_processing_enabled = True
        
        mock_metrics = Mock(spec=MetricsLogger)
        result = detector.find_features(self.test_payload, mock_metrics)
        
        # Verify parent method was called
        mock_super.return_value.find_features.assert_called_once()
        
        # Verify enhanced metadata was added
        self.assertIn("features", result)
        feature = result["features"][0]
        self.assertEqual(feature["properties"]["detector_type"], "AsyncSMDetector")

    @patch('osml_extensions.detectors.async_sm_detector.super')
    def test_find_features_parent_error_with_fallback(self, mock_super):
        """Test feature detection when parent method fails and fallback is enabled."""
        mock_super.return_value.find_features.side_effect = Exception("Parent error")
        
        detector = AsyncSMDetector(self.endpoint, self.credentials)
        
        mock_metrics = Mock(spec=MetricsLogger)
        
        with patch.dict(os.environ, {"EXTENSION_FALLBACK_ENABLED": "true"}):
            with self.assertRaises(Exception) as context:
                detector.find_features(self.test_payload, mock_metrics)
            
            self.assertEqual(str(context.exception), "Parent error")

    @patch('osml_extensions.detectors.async_sm_detector.super')
    def test_find_features_parent_error_no_fallback(self, mock_super):
        """Test feature detection when parent method fails and fallback is disabled."""
        mock_super.return_value.find_features.side_effect = Exception("Parent error")
        
        detector = AsyncSMDetector(self.endpoint, self.credentials)
        
        mock_metrics = Mock(spec=MetricsLogger)
        
        with patch.dict(os.environ, {"EXTENSION_FALLBACK_ENABLED": "false"}):
            with self.assertRaises(ExtensionRuntimeError):
                detector.find_features(self.test_payload, mock_metrics)

    @patch('osml_extensions.detectors.async_sm_detector.super')
    def test_find_features_with_metrics_error(self, mock_super):
        """Test feature detection when metrics logging fails."""
        mock_super.return_value.find_features.return_value = self.test_features
        
        detector = AsyncSMDetector(self.endpoint, self.credentials)
        detector.enhanced_processing_enabled = True
        
        # Mock metrics that raises exception
        mock_metrics = Mock(spec=MetricsLogger)
        mock_metrics.put_metric.side_effect = Exception("Metrics error")
        
        # Should not fail even if metrics fail
        result = detector.find_features(self.test_payload, mock_metrics)
        
        # Should still return processed features
        self.assertIn("features", result)


if __name__ == '__main__':
    unittest.main()