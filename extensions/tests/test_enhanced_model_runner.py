#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import os
import unittest
from unittest.mock import Mock, patch, MagicMock

from aws.osml.model_runner.inference import FeatureDetectorFactory
from aws.osml.model_runner.region_request_handler import RegionRequestHandler
from aws.osml.model_runner.tile_worker import TileWorker, VariableOverlapTilingStrategy

from osml_extensions.enhanced_model_runner import EnhancedModelRunner
from osml_extensions.factory import EnhancedFeatureDetectorFactory
from osml_extensions.handlers import EnhancedRegionRequestHandler


class TestEnhancedModelRunner(unittest.TestCase):
    """Test cases for EnhancedModelRunner."""

    def setUp(self):
        """Set up test fixtures."""
        # Mock all the dependencies to avoid actual AWS service calls
        self.mock_config_patcher = patch('osml_extensions.enhanced_model_runner.ServiceConfig')
        self.mock_config = self.mock_config_patcher.start()
        
        self.mock_queue_patcher = patch('osml_extensions.enhanced_model_runner.RequestQueue')
        self.mock_queue = self.mock_queue_patcher.start()
        
        self.mock_job_table_patcher = patch('osml_extensions.enhanced_model_runner.JobTable')
        self.mock_job_table = self.mock_job_table_patcher.start()
        
        self.mock_region_table_patcher = patch('osml_extensions.enhanced_model_runner.RegionRequestTable')
        self.mock_region_table = self.mock_region_table_patcher.start()
        
        self.mock_stats_table_patcher = patch('osml_extensions.enhanced_model_runner.EndpointStatisticsTable')
        self.mock_stats_table = self.mock_stats_table_patcher.start()
        
        self.mock_image_monitor_patcher = patch('osml_extensions.enhanced_model_runner.ImageStatusMonitor')
        self.mock_image_monitor = self.mock_image_monitor_patcher.start()
        
        self.mock_region_monitor_patcher = patch('osml_extensions.enhanced_model_runner.RegionStatusMonitor')
        self.mock_region_monitor = self.mock_region_monitor_patcher.start()
        
        self.mock_endpoint_utils_patcher = patch('osml_extensions.enhanced_model_runner.EndpointUtils')
        self.mock_endpoint_utils = self.mock_endpoint_utils_patcher.start()
        
        self.mock_image_handler_patcher = patch('osml_extensions.enhanced_model_runner.ImageRequestHandler')
        self.mock_image_handler = self.mock_image_handler_patcher.start()

    def tearDown(self):
        """Clean up patches."""
        self.mock_config_patcher.stop()
        self.mock_queue_patcher.stop()
        self.mock_job_table_patcher.stop()
        self.mock_region_table_patcher.stop()
        self.mock_stats_table_patcher.stop()
        self.mock_image_monitor_patcher.stop()
        self.mock_region_monitor_patcher.stop()
        self.mock_endpoint_utils_patcher.stop()
        self.mock_image_handler_patcher.stop()

    def test_init_with_defaults(self):
        """Test EnhancedModelRunner initialization with defaults."""
        runner = EnhancedModelRunner()
        
        self.assertIsInstance(runner.tiling_strategy, VariableOverlapTilingStrategy)
        self.assertIsNone(runner.factory_class)
        self.assertIsNone(runner.region_handler_class)
        self.assertIsNone(runner.tile_worker_class)
        self.assertFalse(runner.running)

    def test_init_with_custom_classes(self):
        """Test initialization with custom classes."""
        custom_factory = Mock()
        custom_handler = Mock()
        custom_worker = Mock()
        
        runner = EnhancedModelRunner(
            factory_class=custom_factory,
            region_handler_class=custom_handler,
            tile_worker_class=custom_worker
        )
        
        self.assertEqual(runner.factory_class, custom_factory)
        self.assertEqual(runner.region_handler_class, custom_handler)
        self.assertEqual(runner.tile_worker_class, custom_worker)

    def test_create_factory_with_injected_class(self):
        """Test factory creation with injected class."""
        custom_factory = Mock(spec=FeatureDetectorFactory)
        
        runner = EnhancedModelRunner(factory_class=custom_factory)
        
        result = runner._create_factory()
        self.assertEqual(result, custom_factory)

    @patch.dict(os.environ, {"USE_EXTENSIONS": "true"})
    def test_create_factory_from_config_extensions_enabled(self):
        """Test factory creation from config when extensions are enabled."""
        runner = EnhancedModelRunner()
        
        result = runner._create_factory()
        self.assertEqual(result, EnhancedFeatureDetectorFactory)

    @patch.dict(os.environ, {"USE_EXTENSIONS": "false"})
    def test_create_factory_from_config_extensions_disabled(self):
        """Test factory creation from config when extensions are disabled."""
        runner = EnhancedModelRunner()
        
        result = runner._create_factory()
        self.assertEqual(result, FeatureDetectorFactory)

    def test_create_region_handler_with_injected_enhanced_class(self):
        """Test region handler creation with injected enhanced class."""
        mock_factory = Mock()
        
        runner = EnhancedModelRunner(region_handler_class=EnhancedRegionRequestHandler)
        
        result = runner._create_region_handler(mock_factory)
        self.assertIsInstance(result, EnhancedRegionRequestHandler)

    def test_create_region_handler_with_injected_base_class(self):
        """Test region handler creation with injected base class."""
        mock_factory = Mock()
        
        runner = EnhancedModelRunner(region_handler_class=RegionRequestHandler)
        
        result = runner._create_region_handler(mock_factory)
        self.assertIsInstance(result, RegionRequestHandler)

    @patch.dict(os.environ, {"USE_EXTENSIONS": "true"})
    def test_create_region_handler_from_config_extensions_enabled(self):
        """Test region handler creation from config when extensions are enabled."""
        mock_factory = Mock()
        
        runner = EnhancedModelRunner()
        
        result = runner._create_region_handler(mock_factory)
        self.assertIsInstance(result, EnhancedRegionRequestHandler)

    @patch.dict(os.environ, {"USE_EXTENSIONS": "false"})
    def test_create_region_handler_from_config_extensions_disabled(self):
        """Test region handler creation from config when extensions are disabled."""
        mock_factory = Mock()
        
        runner = EnhancedModelRunner()
        
        result = runner._create_region_handler(mock_factory)
        self.assertIsInstance(result, RegionRequestHandler)

    def test_get_tile_worker_class_with_injected_class(self):
        """Test tile worker class selection with injected class."""
        custom_worker = Mock(spec=TileWorker)
        
        runner = EnhancedModelRunner(tile_worker_class=custom_worker)
        
        result = runner._get_tile_worker_class()
        self.assertEqual(result, custom_worker)

    @patch.dict(os.environ, {"USE_EXTENSIONS": "true"})
    def test_get_tile_worker_class_from_config_extensions_enabled(self):
        """Test tile worker class selection when extensions are enabled."""
        runner = EnhancedModelRunner()
        
        result = runner._get_tile_worker_class()
        # Should return EnhancedTileWorker when extensions are enabled
        from osml_extensions.workers import EnhancedTileWorker
        self.assertEqual(result, EnhancedTileWorker)

    @patch.dict(os.environ, {"USE_EXTENSIONS": "false"})
    def test_get_tile_worker_class_from_config_extensions_disabled(self):
        """Test tile worker class selection when extensions are disabled."""
        runner = EnhancedModelRunner()
        
        result = runner._get_tile_worker_class()
        self.assertEqual(result, TileWorker)

    def test_create_image_request_handler(self):
        """Test image request handler creation."""
        runner = EnhancedModelRunner()
        
        result = runner._create_image_request_handler()
        
        # Verify ImageRequestHandler was created with correct dependencies
        self.mock_image_handler.assert_called_once()
        call_args = self.mock_image_handler.call_args[1]
        self.assertEqual(call_args['region_request_handler'], runner.region_request_handler)

    def test_run_and_stop(self):
        """Test run and stop methods."""
        runner = EnhancedModelRunner()
        
        # Mock monitor_work_queues to avoid infinite loop
        runner.monitor_work_queues = Mock()
        
        # Test run
        runner.run()
        self.assertTrue(runner.running)
        runner.monitor_work_queues.assert_called_once()
        
        # Test stop
        runner.stop()
        self.assertFalse(runner.running)

    @patch('osml_extensions.enhanced_model_runner.set_gdal_default_configuration')
    def test_monitor_work_queues_initialization(self, mock_gdal_config):
        """Test monitor_work_queues initialization."""
        runner = EnhancedModelRunner()
        runner.running = False  # Prevent infinite loop
        
        # Mock the processing methods
        runner._process_region_requests = Mock(return_value=False)
        runner._process_image_requests = Mock(return_value=False)
        
        runner.monitor_work_queues()
        
        mock_gdal_config.assert_called_once()

    def test_monitor_work_queues_exception_handling(self):
        """Test monitor_work_queues exception handling."""
        runner = EnhancedModelRunner()
        
        # Mock processing methods to raise exception
        runner._process_region_requests = Mock(side_effect=Exception("Test error"))
        runner._process_image_requests = Mock()
        
        with patch('osml_extensions.enhanced_model_runner.set_gdal_default_configuration'):
            runner.monitor_work_queues()
        
        # Should have stopped running due to exception
        self.assertFalse(runner.running)


if __name__ == '__main__':
    unittest.main()