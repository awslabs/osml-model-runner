#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import unittest
from unittest.mock import Mock, patch, MagicMock

from aws.osml.model_runner.api import RegionRequest
from ..src.osml_extensions.api import ExtendedModelInvokeMode
from ..src.osml_extensions.handlers.enhanced_region_handler import EnhancedRegionRequestHandler
from ..src.osml_extensions.workers import AsyncTileWorkerPool
from osml_extensions import EnhancedServiceConfig


class TestEnhancedRegionHandlerAsyncIntegration(unittest.TestCase):
    """Test cases for EnhancedRegionRequestHandler async integration."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create mock dependencies
        self.mock_region_request_table = Mock()
        self.mock_job_table = Mock()
        self.mock_region_status_monitor = Mock()
        self.mock_endpoint_statistics_table = Mock()
        self.mock_tiling_strategy = Mock()
        self.mock_endpoint_utils = Mock()
        
        # Create enhanced service config
        self.enhanced_config = EnhancedServiceConfig()
        self.enhanced_config.enhanced_monitoring_enabled = True
        self.enhanced_config.async_worker_pool_enabled = True
        
        # Create handler
        self.handler = EnhancedRegionRequestHandler(
            region_request_table=self.mock_region_request_table,
            job_table=self.mock_job_table,
            region_status_monitor=self.mock_region_status_monitor,
            endpoint_statistics_table=self.mock_endpoint_statistics_table,
            tiling_strategy=self.mock_tiling_strategy,
            endpoint_utils=self.mock_endpoint_utils,
            config=self.enhanced_config
        )
        
        # Create test region request
        self.region_request = RegionRequest(
            region_id="test-region-123",
            image_id="test-image-456",
            model_name="test-async-endpoint",
            model_invoke_mode=ExtendedModelInvokeMode.SM_ENDPOINT_ASYNC
        )
    
    def test_should_use_async_worker_pool_true(self):
        """Test async worker pool should be used when conditions are met."""
        result = self.handler._should_use_async_worker_pool(self.region_request)
        self.assertTrue(result)
    
    def test_should_use_async_worker_pool_false_not_enhanced(self):
        """Test async worker pool should not be used when enhanced processing is disabled."""
        self.handler.enhanced_processing_enabled = False
        result = self.handler._should_use_async_worker_pool(self.region_request)
        self.assertFalse(result)
    
    def test_should_use_async_worker_pool_false_not_async_mode(self):
        """Test async worker pool should not be used for non-async invoke mode."""
        self.region_request.model_invoke_mode = "SM_ENDPOINT"  # Not async
        result = self.handler._should_use_async_worker_pool(self.region_request)
        self.assertFalse(result)
    
    def test_should_use_async_worker_pool_false_config_disabled(self):
        """Test async worker pool should not be used when disabled in config."""
        self.enhanced_config.async_worker_pool_enabled = False
        result = self.handler._should_use_async_worker_pool(self.region_request)
        self.assertFalse(result)
    
    @patch('osml_extensions.handlers.enhanced_region_handler.AsyncSMDetector')
    @patch('osml_extensions.handlers.enhanced_region_handler.AsyncEndpointConfig')
    @patch('osml_extensions.handlers.enhanced_region_handler.AsyncMetricsTracker')
    @patch('osml_extensions.handlers.enhanced_region_handler.AsyncTileWorkerPool')
    def test_setup_async_worker_pool_success(self, mock_pool_class, mock_metrics_class, 
                                           mock_config_class, mock_detector_class):
        """Test successful async worker pool setup."""
        # Setup mocks
        mock_config = Mock()
        mock_config.submission_workers = 4
        mock_config.polling_workers = 2
        mock_config_class.return_value = mock_config
        
        mock_detector = Mock()
        mock_detector_class.return_value = mock_detector
        
        mock_metrics = Mock()
        mock_metrics_class.return_value = mock_metrics
        
        mock_pool = Mock()
        mock_pool_class.return_value = mock_pool
        
        # Test setup
        result = self.handler._setup_async_worker_pool(self.region_request)
        
        # Verify result
        self.assertEqual(result, mock_pool)
        
        # Verify components were created correctly
        mock_config_class.assert_called_once()
        mock_detector_class.assert_called_once_with(
            endpoint=self.region_request.model_name,
            assumed_credentials=None,
            async_config=mock_config
        )
        mock_metrics_class.assert_called_once()
        mock_pool_class.assert_called_once_with(
            async_detector=mock_detector,
            config=mock_config,
            metrics_tracker=mock_metrics
        )
    
    @patch('osml_extensions.handlers.enhanced_region_handler.get_credentials_for_assumed_role')
    @patch('osml_extensions.handlers.enhanced_region_handler.AsyncSMDetector')
    @patch('osml_extensions.handlers.enhanced_region_handler.AsyncEndpointConfig')
    @patch('osml_extensions.handlers.enhanced_region_handler.AsyncMetricsTracker')
    @patch('osml_extensions.handlers.enhanced_region_handler.AsyncTileWorkerPool')
    def test_setup_async_worker_pool_with_credentials(self, mock_pool_class, mock_metrics_class,
                                                    mock_config_class, mock_detector_class, mock_get_creds):
        """Test async worker pool setup with model invocation role."""
        # Setup region request with role
        self.region_request.model_invocation_role = "arn:aws:iam::123456789012:role/TestRole"
        
        # Setup mocks
        mock_credentials = {"AccessKeyId": "test", "SecretAccessKey": "test", "SessionToken": "test"}
        mock_get_creds.return_value = mock_credentials
        
        mock_config = Mock()
        mock_config_class.return_value = mock_config
        
        mock_detector = Mock()
        mock_detector_class.return_value = mock_detector
        
        mock_metrics = Mock()
        mock_metrics_class.return_value = mock_metrics
        
        mock_pool = Mock()
        mock_pool_class.return_value = mock_pool
        
        # Test setup
        result = self.handler._setup_async_worker_pool(self.region_request)
        
        # Verify credentials were obtained and used
        mock_get_creds.assert_called_once_with(self.region_request.model_invocation_role)
        mock_detector_class.assert_called_once_with(
            endpoint=self.region_request.model_name,
            assumed_credentials=mock_credentials,
            async_config=mock_config
        )
    
    @patch('osml_extensions.handlers.enhanced_region_handler.AsyncTileWorkerPool')
    @patch('osml_extensions.handlers.enhanced_region_handler.AsyncMetricsTracker')
    @patch('osml_extensions.handlers.enhanced_region_handler.AsyncEndpointConfig')
    @patch('osml_extensions.handlers.enhanced_region_handler.AsyncSMDetector')
    def test_setup_enhanced_tile_workers_async(self, mock_detector_class, mock_config_class,
                                             mock_metrics_class, mock_pool_class):
        """Test setup uses async worker pool when appropriate."""
        # Setup mocks
        mock_pool = Mock()
        mock_pool_class.return_value = mock_pool
        
        result = self.handler._setup_enhanced_tile_workers(self.region_request)
        
        # Should return async pool
        self.assertEqual(result, mock_pool)
    
    @patch('osml_extensions.handlers.enhanced_region_handler.Queue')
    @patch('osml_extensions.handlers.enhanced_region_handler.tempfile')
    @patch('osml_extensions.handlers.enhanced_region_handler.GDALConfigEnv')
    @patch('osml_extensions.handlers.enhanced_region_handler.GDALTileFactory')
    @patch('osml_extensions.handlers.enhanced_region_handler._create_tile')
    def test_process_tiles_with_async_pool(self, mock_create_tile, mock_tile_factory_class,
                                         mock_gdal_env_class, mock_tempfile, mock_queue_class):
        """Test processing tiles with async worker pool."""
        # Setup mocks
        mock_async_pool = Mock()
        mock_async_pool.config.submission_workers = 2
        mock_async_pool.process_tiles_async.return_value = (10, 1)
        mock_async_pool.get_worker_stats.return_value = {
            "submission_workers": {"workers": 2, "total_processed": 9, "total_failed": 1},
            "polling_workers": {"workers": 1, "total_completed": 9, "total_failed": 0}
        }
        
        mock_region_request_item = Mock()
        mock_region_request_item.region_bounds = [[0, 0], [1000, 1000]]
        mock_region_request_item.tile_size = [512, 512]
        mock_region_request_item.tile_overlap = [64, 64]
        mock_region_request_item.succeeded_tiles = None
        mock_region_request_item.tile_format = "NITF"
        mock_region_request_item.tile_compression = "JPEG"
        mock_region_request_item.image_read_role = None
        mock_region_request_item.image_id = "test-image"
        mock_region_request_item.job_id = "test-job"
        mock_region_request_item.region_id = "test-region"
        
        mock_raster_dataset = Mock()
        mock_sensor_model = Mock()
        mock_metrics = Mock()
        
        # Setup tiling strategy
        self.mock_tiling_strategy.compute_tiles.return_value = [
            ((0, 0), (512, 512)),
            ((448, 0), (960, 512)),
        ]
        
        # Setup tile creation
        mock_create_tile.return_value = "/tmp/tile.nitf"
        
        # Setup temporary directory
        mock_tempfile.TemporaryDirectory.return_value.__enter__.return_value = "/tmp"
        
        # Setup GDAL environment
        mock_gdal_env = Mock()
        mock_gdal_env_class.return_value = mock_gdal_env
        mock_gdal_env.with_aws_credentials.return_value = mock_gdal_env
        mock_gdal_env.__enter__ = Mock(return_value=mock_gdal_env)
        mock_gdal_env.__exit__ = Mock(return_value=None)
        
        # Setup tile factory
        mock_tile_factory = Mock()
        mock_tile_factory_class.return_value = mock_tile_factory
        
        # Setup queue
        mock_queue = Mock()
        mock_queue_class.return_value = mock_queue
        
        # Test processing
        total_count, failed_count = self.handler._process_tiles_with_async_pool(
            mock_async_pool,
            self.mock_tiling_strategy,
            mock_region_request_item,
            mock_raster_dataset,
            mock_sensor_model,
            mock_metrics
        )
        
        # Verify results
        self.assertEqual(total_count, 10)
        self.assertEqual(failed_count, 1)
        
        # Verify async pool was used
        mock_async_pool.process_tiles_async.assert_called_once()
        mock_async_pool.get_worker_stats.assert_called_once()
        
        # Verify metrics were logged
        mock_metrics.put_metric.assert_called()
    
    def test_process_tiles_with_async_pool_no_tiles(self):
        """Test processing with async pool when no tiles to process."""
        mock_async_pool = Mock()
        mock_region_request_item = Mock()
        mock_region_request_item.region_bounds = [[0, 0], [100, 100]]
        mock_region_request_item.tile_size = [512, 512]
        mock_region_request_item.tile_overlap = [64, 64]
        mock_region_request_item.succeeded_tiles = None
        
        # Setup tiling strategy to return no tiles
        self.mock_tiling_strategy.compute_tiles.return_value = []
        
        total_count, failed_count = self.handler._process_tiles_with_async_pool(
            mock_async_pool,
            self.mock_tiling_strategy,
            mock_region_request_item,
            Mock(),
            None,
            None
        )
        
        # Should return zero counts
        self.assertEqual(total_count, 0)
        self.assertEqual(failed_count, 0)
        
        # Async pool should not be called
        mock_async_pool.process_tiles_async.assert_not_called()


class TestEnhancedRegionHandlerAsyncFallback(unittest.TestCase):
    """Test cases for async integration fallback scenarios."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create mock dependencies
        self.mock_region_request_table = Mock()
        self.mock_job_table = Mock()
        self.mock_region_status_monitor = Mock()
        self.mock_endpoint_statistics_table = Mock()
        self.mock_tiling_strategy = Mock()
        self.mock_endpoint_utils = Mock()
        
        # Create base service config (not enhanced)
        self.base_config = Mock()
        self.base_config.enhanced_monitoring_enabled = False
        
        # Create handler with base config
        self.handler = EnhancedRegionRequestHandler(
            region_request_table=self.mock_region_request_table,
            job_table=self.mock_job_table,
            region_status_monitor=self.mock_region_status_monitor,
            endpoint_statistics_table=self.mock_endpoint_statistics_table,
            tiling_strategy=self.mock_tiling_strategy,
            endpoint_utils=self.mock_endpoint_utils,
            config=self.base_config
        )


if __name__ == "__main__":
    unittest.main()