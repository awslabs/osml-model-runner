#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import os
import unittest
from unittest.mock import Mock, patch, MagicMock

from aws_embedded_metrics.logger.metrics_logger import MetricsLogger
from osgeo import gdal

from aws.osml.model_runner.api import RegionRequest
from aws.osml.model_runner.app_config import ServiceConfig
from aws.osml.model_runner.database import RegionRequestItem

from osml_extensions.handlers import EnhancedRegionRequestHandler
from osml_extensions.errors import ExtensionRuntimeError


class TestEnhancedRegionRequestHandler(unittest.TestCase):
    """Test cases for EnhancedRegionRequestHandler."""

    def setUp(self):
        """Set up test fixtures."""
        self.region_request_table = Mock()
        self.job_table = Mock()
        self.region_status_monitor = Mock()
        self.endpoint_statistics_table = Mock()
        self.tiling_strategy = Mock()
        self.endpoint_utils = Mock()
        self.config = Mock(spec=ServiceConfig)
        self.config.self_throttling = False
        self.config.elevation_model = None
        
        self.factory = Mock()
        self.tile_worker_class = Mock()
        
        self.region_request = Mock(spec=RegionRequest)
        self.region_request.region_id = "test-region-id"
        self.region_request.image_id = "test-image-id"
        self.region_request.model_name = "test-model"
        self.region_request.model_invocation_role = None
        self.region_request.is_valid.return_value = True
        
        self.region_request_item = Mock(spec=RegionRequestItem)
        self.region_request_item.region_id = "test-region-id"
        self.region_request_item.image_id = "test-image-id"
        
        self.raster_dataset = Mock(spec=gdal.Dataset)
        self.raster_dataset.GetDriver.return_value.ShortName = "GTiff"

    def test_init_with_defaults(self):
        """Test EnhancedRegionRequestHandler initialization with defaults."""
        handler = EnhancedRegionRequestHandler(
            self.region_request_table,
            self.job_table,
            self.region_status_monitor,
            self.endpoint_statistics_table,
            self.tiling_strategy,
            self.endpoint_utils,
            self.config
        )
        
        self.assertEqual(handler.region_request_table, self.region_request_table)
        self.assertEqual(handler.job_table, self.job_table)
        self.assertIsNone(handler.factory)
        self.assertIsNone(handler.tile_worker_class)
        self.assertTrue(hasattr(handler, 'enhanced_processing_enabled'))

    def test_init_with_custom_factory_and_worker_class(self):
        """Test initialization with custom factory and worker class."""
        handler = EnhancedRegionRequestHandler(
            self.region_request_table,
            self.job_table,
            self.region_status_monitor,
            self.endpoint_statistics_table,
            self.tiling_strategy,
            self.endpoint_utils,
            self.config,
            factory=self.factory,
            tile_worker_class=self.tile_worker_class
        )
        
        self.assertEqual(handler.factory, self.factory)
        self.assertEqual(handler.tile_worker_class, self.tile_worker_class)

    @patch.dict(os.environ, {"ENHANCED_MONITORING_ENABLED": "true"})
    def test_should_enable_enhanced_processing_true(self):
        """Test enhanced processing enablement from config."""
        handler = EnhancedRegionRequestHandler(
            self.region_request_table,
            self.job_table,
            self.region_status_monitor,
            self.endpoint_statistics_table,
            self.tiling_strategy,
            self.endpoint_utils,
            self.config
        )
        
        self.assertTrue(handler._should_enable_enhanced_processing())

    @patch.dict(os.environ, {"ENHANCED_MONITORING_ENABLED": "false"})
    def test_should_enable_enhanced_processing_false(self):
        """Test enhanced processing disablement from config."""
        handler = EnhancedRegionRequestHandler(
            self.region_request_table,
            self.job_table,
            self.region_status_monitor,
            self.endpoint_statistics_table,
            self.tiling_strategy,
            self.endpoint_utils,
            self.config
        )
        
        self.assertFalse(handler._should_enable_enhanced_processing())

    def test_enhance_region_processing_enabled(self):
        """Test region processing enhancement when enabled."""
        handler = EnhancedRegionRequestHandler(
            self.region_request_table,
            self.job_table,
            self.region_status_monitor,
            self.endpoint_statistics_table,
            self.tiling_strategy,
            self.endpoint_utils,
            self.config
        )
        handler.enhanced_processing_enabled = True
        
        result = handler._enhance_region_processing(self.region_request)
        
        # For now, should return the same request
        self.assertEqual(result, self.region_request)

    def test_enhance_region_processing_disabled(self):
        """Test region processing enhancement when disabled."""
        handler = EnhancedRegionRequestHandler(
            self.region_request_table,
            self.job_table,
            self.region_status_monitor,
            self.endpoint_statistics_table,
            self.tiling_strategy,
            self.endpoint_utils,
            self.config
        )
        handler.enhanced_processing_enabled = False
        
        result = handler._enhance_region_processing(self.region_request)
        
        self.assertEqual(result, self.region_request)

    def test_add_enhanced_monitoring_enabled(self):
        """Test enhanced monitoring addition when enabled."""
        handler = EnhancedRegionRequestHandler(
            self.region_request_table,
            self.job_table,
            self.region_status_monitor,
            self.endpoint_statistics_table,
            self.tiling_strategy,
            self.endpoint_utils,
            self.config
        )
        handler.enhanced_processing_enabled = True
        
        mock_metrics = Mock(spec=MetricsLogger)
        handler._add_enhanced_monitoring(mock_metrics)
        
        # Verify metrics were added
        mock_metrics.put_metric.assert_called()
        mock_metrics.put_dimensions.assert_called()

    def test_add_enhanced_monitoring_disabled(self):
        """Test enhanced monitoring addition when disabled."""
        handler = EnhancedRegionRequestHandler(
            self.region_request_table,
            self.job_table,
            self.region_status_monitor,
            self.endpoint_statistics_table,
            self.tiling_strategy,
            self.endpoint_utils,
            self.config
        )
        handler.enhanced_processing_enabled = False
        
        mock_metrics = Mock(spec=MetricsLogger)
        handler._add_enhanced_monitoring(mock_metrics)
        
        # Verify no metrics were added
        mock_metrics.put_metric.assert_not_called()
        mock_metrics.put_dimensions.assert_not_called()

    @patch('osml_extensions.handlers.enhanced_region_handler.setup_enhanced_tile_workers')
    def test_setup_enhanced_tile_workers_success(self, mock_setup_workers):
        """Test successful setup of enhanced tile workers."""
        mock_queue = Mock()
        mock_workers = [Mock(), Mock()]
        mock_setup_workers.return_value = (mock_queue, mock_workers)
        
        handler = EnhancedRegionRequestHandler(
            self.region_request_table,
            self.job_table,
            self.region_status_monitor,
            self.endpoint_statistics_table,
            self.tiling_strategy,
            self.endpoint_utils,
            self.config,
            factory=self.factory,
            tile_worker_class=self.tile_worker_class
        )
        
        sensor_model = Mock()
        result = handler._setup_enhanced_tile_workers(self.region_request, sensor_model)
        
        self.assertEqual(result, (mock_queue, mock_workers))
        mock_setup_workers.assert_called_once_with(
            region_request=self.region_request,
            sensor_model=sensor_model,
            elevation_model=None,
            factory=self.factory,
            worker_class=self.tile_worker_class
        )

    @patch('osml_extensions.handlers.enhanced_region_handler.setup_enhanced_tile_workers')
    @patch('aws.osml.model_runner.tile_worker.tile_worker_utils.setup_tile_workers')
    def test_setup_enhanced_tile_workers_fallback(self, mock_base_setup, mock_enhanced_setup):
        """Test fallback to base tile workers when enhanced setup fails."""
        mock_enhanced_setup.side_effect = Exception("Enhanced setup failed")
        
        mock_queue = Mock()
        mock_workers = [Mock()]
        mock_base_setup.return_value = (mock_queue, mock_workers)
        
        handler = EnhancedRegionRequestHandler(
            self.region_request_table,
            self.job_table,
            self.region_status_monitor,
            self.endpoint_statistics_table,
            self.tiling_strategy,
            self.endpoint_utils,
            self.config
        )
        
        result = handler._setup_enhanced_tile_workers(self.region_request)
        
        self.assertEqual(result, (mock_queue, mock_workers))
        mock_base_setup.assert_called_once()

    @patch('osml_extensions.handlers.enhanced_region_handler.process_tiles')
    def test_process_region_request_success(self, mock_process_tiles):
        """Test successful region request processing."""
        # Setup mocks
        mock_process_tiles.return_value = (10, 0)  # total_tiles, failed_tiles
        
        mock_image_request_item = Mock()
        self.job_table.complete_region_request.return_value = mock_image_request_item
        
        mock_region_status = "SUCCEEDED"
        self.region_status_monitor.get_status.return_value = mock_region_status
        
        updated_region_item = Mock()
        self.region_request_table.update_region_request.return_value = updated_region_item
        self.region_request_table.complete_region_request.return_value = updated_region_item
        
        handler = EnhancedRegionRequestHandler(
            self.region_request_table,
            self.job_table,
            self.region_status_monitor,
            self.endpoint_statistics_table,
            self.tiling_strategy,
            self.endpoint_utils,
            self.config
        )
        
        # Mock the tile worker setup
        handler._setup_enhanced_tile_workers = Mock(return_value=(Mock(), [Mock()]))
        
        mock_metrics = Mock(spec=MetricsLogger)
        result = handler.process_region_request(
            self.region_request,
            self.region_request_item,
            self.raster_dataset,
            metrics=mock_metrics
        )
        
        self.assertEqual(result, mock_image_request_item)
        
        # Verify region processing was started
        self.region_request_table.start_region_request.assert_called_once_with(self.region_request_item)
        
        # Verify tiles were processed
        mock_process_tiles.assert_called_once()
        
        # Verify region was completed
        self.job_table.complete_region_request.assert_called_once()
        self.region_request_table.complete_region_request.assert_called_once()

    def test_process_region_request_invalid_request(self):
        """Test processing with invalid region request."""
        self.region_request.is_valid.return_value = False
        
        handler = EnhancedRegionRequestHandler(
            self.region_request_table,
            self.job_table,
            self.region_status_monitor,
            self.endpoint_statistics_table,
            self.tiling_strategy,
            self.endpoint_utils,
            self.config
        )
        
        with self.assertRaises(ValueError) as context:
            handler.process_region_request(
                self.region_request,
                self.region_request_item,
                self.raster_dataset
            )
        
        self.assertIn("Invalid Enhanced Region Request", str(context.exception))

    @patch('osml_extensions.handlers.enhanced_region_handler.process_tiles')
    def test_process_region_request_processing_error_with_fallback(self, mock_process_tiles):
        """Test processing error with fallback enabled."""
        mock_process_tiles.side_effect = Exception("Processing failed")
        
        handler = EnhancedRegionRequestHandler(
            self.region_request_table,
            self.job_table,
            self.region_status_monitor,
            self.endpoint_statistics_table,
            self.tiling_strategy,
            self.endpoint_utils,
            self.config
        )
        
        # Mock the tile worker setup
        handler._setup_enhanced_tile_workers = Mock(return_value=(Mock(), [Mock()]))
        handler.fail_region_request = Mock(return_value=Mock())
        
        mock_metrics = Mock(spec=MetricsLogger)
        
        with patch.dict(os.environ, {"EXTENSION_FALLBACK_ENABLED": "true"}):
            result = handler.process_region_request(
                self.region_request,
                self.region_request_item,
                self.raster_dataset,
                metrics=mock_metrics
            )
        
        # Should call fail_region_request
        handler.fail_region_request.assert_called_once()

    @patch('osml_extensions.handlers.enhanced_region_handler.process_tiles')
    def test_process_region_request_processing_error_no_fallback(self, mock_process_tiles):
        """Test processing error with fallback disabled."""
        mock_process_tiles.side_effect = Exception("Processing failed")
        
        handler = EnhancedRegionRequestHandler(
            self.region_request_table,
            self.job_table,
            self.region_status_monitor,
            self.endpoint_statistics_table,
            self.tiling_strategy,
            self.endpoint_utils,
            self.config
        )
        
        # Mock the tile worker setup
        handler._setup_enhanced_tile_workers = Mock(return_value=(Mock(), [Mock()]))
        
        mock_metrics = Mock(spec=MetricsLogger)
        
        with patch.dict(os.environ, {"EXTENSION_FALLBACK_ENABLED": "false"}):
            with self.assertRaises(ExtensionRuntimeError):
                handler.process_region_request(
                    self.region_request,
                    self.region_request_item,
                    self.raster_dataset,
                    metrics=mock_metrics
                )


if __name__ == '__main__':
    unittest.main()