#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import os
import unittest
from queue import Queue
from unittest.mock import Mock, patch, MagicMock

from aws.osml.model_runner.api import RegionRequest, ModelInvokeMode
from aws.osml.model_runner.tile_worker import TileWorker

from osml_extensions.workers import (
    setup_enhanced_tile_workers,
    create_enhanced_tile_worker,
    setup_tile_workers_with_factory,
    EnhancedTileWorker
)
from osml_extensions.errors import ExtensionRuntimeError


class TestEnhancedTileWorkerUtils(unittest.TestCase):
    """Test cases for enhanced tile worker utility functions."""

    def setUp(self):
        """Set up test fixtures."""
        self.region_request = Mock(spec=RegionRequest)
        self.region_request.model_name = "test-model"
        self.region_request.model_invoke_mode = ModelInvokeMode.SM_ENDPOINT
        self.region_request.model_invocation_role = None
        self.region_request.tile_size = [512, 512]
        self.region_request.tile_overlap = [64, 64]
        self.region_request.region_id = "test-region-id"
        
        self.sensor_model = Mock()
        self.elevation_model = Mock()
        self.factory = Mock()
        self.detector = Mock()
        self.factory.build.return_value = self.detector

    @patch('osml_extensions.workers.enhanced_tile_worker_utils.ServiceConfig')
    @patch('osml_extensions.workers.enhanced_tile_worker_utils.create_enhanced_tile_worker')
    def test_setup_enhanced_tile_workers_success(self, mock_create_worker, mock_service_config):
        """Test successful setup of enhanced tile workers."""
        mock_service_config.workers = "2"
        
        mock_worker1 = Mock(spec=EnhancedTileWorker)
        mock_worker2 = Mock(spec=EnhancedTileWorker)
        mock_create_worker.side_effect = [mock_worker1, mock_worker2]
        
        queue, workers = setup_enhanced_tile_workers(self.region_request)
        
        self.assertIsInstance(queue, Queue)
        self.assertEqual(len(workers), 2)
        self.assertEqual(workers[0], mock_worker1)
        self.assertEqual(workers[1], mock_worker2)
        
        # Verify workers were started
        mock_worker1.start.assert_called_once()
        mock_worker2.start.assert_called_once()

    @patch('osml_extensions.workers.enhanced_tile_worker_utils.ServiceConfig')
    @patch('osml_extensions.workers.enhanced_tile_worker_utils.create_enhanced_tile_worker')
    def test_setup_enhanced_tile_workers_with_custom_factory(self, mock_create_worker, mock_service_config):
        """Test setup with custom factory."""
        mock_service_config.workers = "1"
        
        mock_worker = Mock(spec=EnhancedTileWorker)
        mock_create_worker.return_value = mock_worker
        
        queue, workers = setup_enhanced_tile_workers(
            self.region_request,
            factory=self.factory,
            worker_class=EnhancedTileWorker
        )
        
        self.assertEqual(len(workers), 1)
        mock_create_worker.assert_called_with(
            tile_queue=queue,
            region_request=self.region_request,
            model_invocation_credentials=None,
            sensor_model=None,
            elevation_model=None,
            factory=self.factory,
            worker_class=EnhancedTileWorker
        )

    @patch('osml_extensions.workers.enhanced_tile_worker_utils.ServiceConfig')
    @patch('osml_extensions.workers.enhanced_tile_worker_utils.create_enhanced_tile_worker')
    def test_setup_enhanced_tile_workers_creation_failure(self, mock_create_worker, mock_service_config):
        """Test setup when worker creation fails."""
        mock_service_config.workers = "1"
        mock_create_worker.side_effect = Exception("Creation failed")
        
        with self.assertRaises(ExtensionRuntimeError):
            setup_enhanced_tile_workers(self.region_request)

    @patch('osml_extensions.workers.enhanced_tile_worker_utils.get_credentials_for_assumed_role')
    def test_setup_enhanced_tile_workers_with_credentials(self, mock_get_credentials):
        """Test setup with model invocation role."""
        self.region_request.model_invocation_role = "test-role"
        mock_credentials = {"access_key": "test"}
        mock_get_credentials.return_value = mock_credentials
        
        with patch('osml_extensions.workers.enhanced_tile_worker_utils.ServiceConfig') as mock_service_config:
            mock_service_config.workers = "1"
            with patch('osml_extensions.workers.enhanced_tile_worker_utils.create_enhanced_tile_worker') as mock_create:
                mock_create.return_value = Mock(spec=EnhancedTileWorker)
                
                setup_enhanced_tile_workers(self.region_request)
                
                mock_get_credentials.assert_called_once_with("test-role")
                mock_create.assert_called_with(
                    tile_queue=unittest.mock.ANY,
                    region_request=self.region_request,
                    model_invocation_credentials=mock_credentials,
                    sensor_model=None,
                    elevation_model=None,
                    factory=None,
                    worker_class=unittest.mock.ANY
                )

    @patch('osml_extensions.workers.enhanced_tile_worker_utils.FeatureTable')
    @patch('osml_extensions.workers.enhanced_tile_worker_utils.RegionRequestTable')
    @patch('osml_extensions.workers.enhanced_tile_worker_utils.ServiceConfig')
    def test_create_enhanced_tile_worker_success(self, mock_service_config, mock_region_table_class, mock_feature_table_class):
        """Test successful creation of enhanced tile worker."""
        mock_service_config.feature_table = "test-feature-table"
        mock_service_config.region_request_table = "test-region-table"
        
        mock_feature_table = Mock()
        mock_region_table = Mock()
        mock_feature_table_class.return_value = mock_feature_table
        mock_region_table_class.return_value = mock_region_table
        
        queue = Queue()
        
        with patch('osml_extensions.workers.enhanced_tile_worker_utils.EnhancedTileWorker') as mock_worker_class:
            mock_worker = Mock(spec=EnhancedTileWorker)
            mock_worker_class.return_value = mock_worker
            
            result = create_enhanced_tile_worker(
                tile_queue=queue,
                region_request=self.region_request,
                factory=self.factory,
                worker_class=EnhancedTileWorker
            )
            
            self.assertEqual(result, mock_worker)
            mock_worker_class.assert_called_once_with(
                queue, self.detector, None, mock_feature_table, mock_region_table
            )

    @patch('osml_extensions.workers.enhanced_tile_worker_utils.FeatureTable')
    @patch('osml_extensions.workers.enhanced_tile_worker_utils.RegionRequestTable')
    @patch('osml_extensions.workers.enhanced_tile_worker_utils.ServiceConfig')
    def test_create_enhanced_tile_worker_with_geolocator(self, mock_service_config, mock_region_table_class, mock_feature_table_class):
        """Test creation with geolocator when sensor model is provided."""
        mock_service_config.feature_table = "test-feature-table"
        mock_service_config.region_request_table = "test-region-table"
        
        mock_feature_table = Mock()
        mock_region_table = Mock()
        mock_feature_table_class.return_value = mock_feature_table
        mock_region_table_class.return_value = mock_region_table
        
        queue = Queue()
        
        with patch('osml_extensions.workers.enhanced_tile_worker_utils.Geolocator') as mock_geolocator_class:
            with patch('osml_extensions.workers.enhanced_tile_worker_utils.EnhancedTileWorker') as mock_worker_class:
                mock_geolocator = Mock()
                mock_geolocator_class.return_value = mock_geolocator
                mock_worker = Mock(spec=EnhancedTileWorker)
                mock_worker_class.return_value = mock_worker
                
                result = create_enhanced_tile_worker(
                    tile_queue=queue,
                    region_request=self.region_request,
                    sensor_model=self.sensor_model,
                    elevation_model=self.elevation_model,
                    factory=self.factory,
                    worker_class=EnhancedTileWorker
                )
                
                self.assertEqual(result, mock_worker)
                mock_geolocator_class.assert_called_once()
                mock_worker_class.assert_called_once_with(
                    queue, self.detector, mock_geolocator, mock_feature_table, mock_region_table
                )

    def test_create_enhanced_tile_worker_detector_creation_failure(self):
        """Test worker creation when detector creation fails."""
        self.factory.build.return_value = None
        
        queue = Queue()
        
        result = create_enhanced_tile_worker(
            tile_queue=queue,
            region_request=self.region_request,
            factory=self.factory
        )
        
        self.assertIsNone(result)

    @patch('osml_extensions.workers.enhanced_tile_worker_utils.FeatureTable')
    @patch('osml_extensions.workers.enhanced_tile_worker_utils.RegionRequestTable')
    @patch('osml_extensions.workers.enhanced_tile_worker_utils.ServiceConfig')
    def test_create_enhanced_tile_worker_with_base_worker_class(self, mock_service_config, mock_region_table_class, mock_feature_table_class):
        """Test creation with base TileWorker class."""
        mock_service_config.feature_table = "test-feature-table"
        mock_service_config.region_request_table = "test-region-table"
        
        mock_feature_table = Mock()
        mock_region_table = Mock()
        mock_feature_table_class.return_value = mock_feature_table
        mock_region_table_class.return_value = mock_region_table
        
        queue = Queue()
        
        with patch('osml_extensions.workers.enhanced_tile_worker_utils.TileWorker') as mock_worker_class:
            mock_worker = Mock(spec=TileWorker)
            mock_worker_class.return_value = mock_worker
            
            result = create_enhanced_tile_worker(
                tile_queue=queue,
                region_request=self.region_request,
                factory=self.factory,
                worker_class=TileWorker
            )
            
            self.assertEqual(result, mock_worker)
            mock_worker_class.assert_called_once_with(
                queue, self.detector, None, mock_feature_table, mock_region_table
            )

    def test_setup_tile_workers_with_factory_backward_compatibility(self):
        """Test backward compatibility wrapper function."""
        with patch('osml_extensions.workers.enhanced_tile_worker_utils.setup_enhanced_tile_workers') as mock_setup:
            mock_queue = Queue()
            mock_workers = [Mock()]
            mock_setup.return_value = (mock_queue, mock_workers)
            
            result = setup_tile_workers_with_factory(
                self.region_request,
                self.sensor_model,
                self.elevation_model,
                self.factory
            )
            
            self.assertEqual(result, (mock_queue, mock_workers))
            mock_setup.assert_called_once_with(
                region_request=self.region_request,
                sensor_model=self.sensor_model,
                elevation_model=self.elevation_model,
                factory=self.factory
            )


if __name__ == '__main__':
    unittest.main()