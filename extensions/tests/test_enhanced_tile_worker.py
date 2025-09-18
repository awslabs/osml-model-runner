#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import os
import tempfile
import unittest
from queue import Queue
from unittest.mock import Mock, patch, MagicMock

from aws_embedded_metrics.logger.metrics_logger import MetricsLogger

from osml_extensions.workers import EnhancedTileWorker
from osml_extensions.errors import ExtensionRuntimeError


class TestEnhancedTileWorker(unittest.TestCase):
    """Test cases for EnhancedTileWorker."""

    def setUp(self):
        """Set up test fixtures."""
        self.queue = Queue()
        self.feature_detector = Mock()
        self.feature_detector.endpoint = "test-endpoint"
        self.geolocator = Mock()
        self.feature_table = Mock()
        self.region_request_table = Mock()
        
        self.test_image_info = {
            "image_path": "/tmp/test_image.jpg",
            "region": [[0, 0], [100, 100]],
            "image_id": "test-image-id",
            "job_id": "test-job-id",
            "region_id": "test-region-id"
        }
        
        self.test_features = [
            {
                "type": "Feature",
                "properties": {"confidence": 0.9},
                "geometry": {"type": "Point", "coordinates": [0, 0]}
            }
        ]

    def test_init_with_defaults(self):
        """Test EnhancedTileWorker initialization with defaults."""
        worker = EnhancedTileWorker(
            self.queue,
            self.feature_detector,
            self.geolocator,
            self.feature_table,
            self.region_request_table
        )
        
        self.assertEqual(worker.in_queue, self.queue)
        self.assertEqual(worker.feature_detector, self.feature_detector)
        self.assertTrue(hasattr(worker, 'enhanced_processing_enabled'))

    def test_init_with_enhanced_processing_enabled(self):
        """Test initialization with enhanced processing explicitly enabled."""
        worker = EnhancedTileWorker(
            self.queue,
            self.feature_detector,
            self.geolocator,
            self.feature_table,
            self.region_request_table,
            enhanced_processing_enabled=True
        )
        
        self.assertTrue(worker.enhanced_processing_enabled)

    def test_init_with_enhanced_processing_disabled(self):
        """Test initialization with enhanced processing explicitly disabled."""
        worker = EnhancedTileWorker(
            self.queue,
            self.feature_detector,
            self.geolocator,
            self.feature_table,
            self.region_request_table,
            enhanced_processing_enabled=False
        )
        
        self.assertFalse(worker.enhanced_processing_enabled)

    @patch.dict(os.environ, {"ENHANCED_MONITORING_ENABLED": "true"})
    def test_should_enable_enhanced_processing_true(self):
        """Test enhanced processing enablement from config."""
        worker = EnhancedTileWorker(
            self.queue,
            self.feature_detector,
            self.geolocator,
            self.feature_table,
            self.region_request_table
        )
        
        self.assertTrue(worker._should_enable_enhanced_processing())

    @patch.dict(os.environ, {"ENHANCED_MONITORING_ENABLED": "false"})
    def test_should_enable_enhanced_processing_false(self):
        """Test enhanced processing disablement from config."""
        worker = EnhancedTileWorker(
            self.queue,
            self.feature_detector,
            self.geolocator,
            self.feature_table,
            self.region_request_table
        )
        
        self.assertFalse(worker._should_enable_enhanced_processing())

    def test_preprocess_tile_enabled(self):
        """Test tile preprocessing when enhanced processing is enabled."""
        worker = EnhancedTileWorker(
            self.queue,
            self.feature_detector,
            self.geolocator,
            self.feature_table,
            self.region_request_table,
            enhanced_processing_enabled=True
        )
        
        result = worker._preprocess_tile(self.test_image_info)
        
        # Check that enhanced metadata was added
        self.assertTrue(result["enhanced_processing"])
        self.assertEqual(result["worker_type"], "EnhancedTileWorker")
        self.assertEqual(result["image_path"], self.test_image_info["image_path"])

    def test_preprocess_tile_disabled(self):
        """Test tile preprocessing when enhanced processing is disabled."""
        worker = EnhancedTileWorker(
            self.queue,
            self.feature_detector,
            self.geolocator,
            self.feature_table,
            self.region_request_table,
            enhanced_processing_enabled=False
        )
        
        result = worker._preprocess_tile(self.test_image_info)
        
        # Should return original image info unchanged
        self.assertEqual(result, self.test_image_info)

    def test_add_enhanced_metrics_enabled(self):
        """Test enhanced metrics addition when enabled."""
        worker = EnhancedTileWorker(
            self.queue,
            self.feature_detector,
            self.geolocator,
            self.feature_table,
            self.region_request_table,
            enhanced_processing_enabled=True
        )
        
        mock_metrics = Mock(spec=MetricsLogger)
        processing_stats = {"feature_count": 5, "processing_time": 100.5}
        
        worker._add_enhanced_metrics(mock_metrics, processing_stats)
        
        # Verify metrics were added
        mock_metrics.put_metric.assert_called()
        mock_metrics.put_dimensions.assert_called()

    def test_add_enhanced_metrics_disabled(self):
        """Test enhanced metrics addition when disabled."""
        worker = EnhancedTileWorker(
            self.queue,
            self.feature_detector,
            self.geolocator,
            self.feature_table,
            self.region_request_table,
            enhanced_processing_enabled=False
        )
        
        mock_metrics = Mock(spec=MetricsLogger)
        processing_stats = {"feature_count": 5}
        
        worker._add_enhanced_metrics(mock_metrics, processing_stats)
        
        # Verify no metrics were added
        mock_metrics.put_metric.assert_not_called()
        mock_metrics.put_dimensions.assert_not_called()

    @patch('builtins.open')
    @patch('osml_extensions.workers.enhanced_tile_worker.time')
    def test_process_tile_success(self, mock_time, mock_open):
        """Test successful tile processing."""
        # Setup mocks
        mock_time.time.side_effect = [1000.0, 1000.1]  # Start and end times
        mock_file = Mock()
        mock_open.return_value.__enter__.return_value = mock_file
        
        feature_collection = {"features": self.test_features}
        self.feature_detector.find_features.return_value = feature_collection
        
        worker = EnhancedTileWorker(
            self.queue,
            self.feature_detector,
            self.geolocator,
            self.feature_table,
            self.region_request_table,
            enhanced_processing_enabled=True
        )
        
        # Mock _refine_features method
        worker._refine_features = Mock(return_value=self.test_features)
        
        mock_metrics = Mock(spec=MetricsLogger)
        worker.process_tile(self.test_image_info, mock_metrics)
        
        # Verify detector was called
        self.feature_detector.find_features.assert_called_once()
        
        # Verify features were stored
        self.feature_table.add_features.assert_called_once()
        
        # Verify region was marked as succeeded
        self.region_request_table.add_tile.assert_called_with(
            self.test_image_info["image_id"],
            self.test_image_info["region_id"],
            self.test_image_info["region"],
            "SUCCEEDED"
        )

    @patch('builtins.open')
    def test_process_tile_detector_error_with_fallback(self, mock_open):
        """Test tile processing when detector fails and fallback is enabled."""
        mock_file = Mock()
        mock_open.return_value.__enter__.return_value = mock_file
        
        self.feature_detector.find_features.side_effect = Exception("Detector error")
        
        worker = EnhancedTileWorker(
            self.queue,
            self.feature_detector,
            self.geolocator,
            self.feature_table,
            self.region_request_table,
            enhanced_processing_enabled=True
        )
        
        mock_metrics = Mock(spec=MetricsLogger)
        
        with patch.dict(os.environ, {"EXTENSION_FALLBACK_ENABLED": "true"}):
            # Should not raise exception with fallback enabled
            worker.process_tile(self.test_image_info, mock_metrics)
        
        # Verify region was marked as failed
        self.region_request_table.add_tile.assert_called_with(
            self.test_image_info["image_id"],
            self.test_image_info["region_id"],
            self.test_image_info["region"],
            "FAILED"
        )
        
        # Verify failed tile count was incremented
        self.assertEqual(worker.failed_tile_count, 1)

    @patch('builtins.open')
    def test_process_tile_detector_error_no_fallback(self, mock_open):
        """Test tile processing when detector fails and fallback is disabled."""
        mock_file = Mock()
        mock_open.return_value.__enter__.return_value = mock_file
        
        self.feature_detector.find_features.side_effect = Exception("Detector error")
        
        worker = EnhancedTileWorker(
            self.queue,
            self.feature_detector,
            self.geolocator,
            self.feature_table,
            self.region_request_table,
            enhanced_processing_enabled=True
        )
        
        mock_metrics = Mock(spec=MetricsLogger)
        
        with patch.dict(os.environ, {"EXTENSION_FALLBACK_ENABLED": "false"}):
            with self.assertRaises(ExtensionRuntimeError):
                worker.process_tile(self.test_image_info, mock_metrics)


if __name__ == '__main__':
    unittest.main()