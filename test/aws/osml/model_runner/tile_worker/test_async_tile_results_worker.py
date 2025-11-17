# Copyright 2024 Amazon.com, Inc. or its affiliates.

from queue import Queue
from unittest import TestCase
from unittest.mock import MagicMock, Mock, patch, PropertyMock

from aws.osml.model_runner.tile_worker.async_tile_results_worker import AsyncResultsWorker


class TestAsyncResultsWorker(TestCase):
    """Test cases for AsyncResultsWorker"""

    def setUp(self):
        """Set up test fixtures"""
        self.in_queue = Queue()
        self.completion_queue = Queue()
        self.mock_feature_table = MagicMock()
        self.mock_region_request_table = MagicMock()
        self.mock_detector = MagicMock()
        self.mock_geolocator = MagicMock()

    @patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.TileRequestTable")
    @patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.ImageRequestTable")
    @patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.boto3")
    def test_worker_initialization(self, mock_boto3, mock_image_table, mock_tile_table):
        """Test worker initializes correctly"""
        worker = AsyncResultsWorker(
            worker_id=1,
            feature_table=self.mock_feature_table,
            geolocator=self.mock_geolocator,
            region_request_table=self.mock_region_request_table,
            in_queue=self.in_queue,
            feature_detector=self.mock_detector,
            completion_queue=self.completion_queue,
        )
        
        assert worker.worker_id == 1
        assert worker.name == "AsyncResultsWorker-1"
        assert worker.completion_queue == self.completion_queue
        assert worker._cached_geolocator is None
        assert worker._cached_image_id is None

    @patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.TileRequestTable")
    @patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.ImageRequestTable")
    @patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.boto3")
    def test_worker_initialization_with_credentials(self, mock_boto3, mock_image_table, mock_tile_table):
        """Test worker initializes with assumed credentials"""
        assumed_creds = {
            "AccessKeyId": "test-key",
            "SecretAccessKey": "test-secret",
            "SessionToken": "test-token",
        }
        
        worker = AsyncResultsWorker(
            worker_id=1,
            feature_table=self.mock_feature_table,
            geolocator=self.mock_geolocator,
            region_request_table=self.mock_region_request_table,
            in_queue=self.in_queue,
            feature_detector=self.mock_detector,
            assumed_credentials=assumed_creds,
        )
        
        # Verify boto3 client was created with credentials
        mock_boto3.client.assert_called()
        call_kwargs = mock_boto3.client.call_args[1]
        assert call_kwargs["aws_access_key_id"] == "test-key"
        assert call_kwargs["aws_secret_access_key"] == "test-secret"
        assert call_kwargs["aws_session_token"] == "test-token"

    @patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.TileRequestTable")
    @patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.ImageRequestTable")
    @patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.boto3")
    def test_get_or_create_geolocator_reuses_cached(self, mock_boto3, mock_image_table, mock_tile_table):
        """Test geolocator reuse for same image_id"""
        mock_sensor_model = MagicMock()
        mock_elevation_model = MagicMock()
        mock_cached_geolocator = MagicMock()
        
        worker = AsyncResultsWorker(
            worker_id=1,
            feature_table=self.mock_feature_table,
            geolocator=None,
            region_request_table=self.mock_region_request_table,
            in_queue=self.in_queue,
            feature_detector=self.mock_detector,
        )
        
        # Set up cached geolocator
        worker._cached_geolocator = mock_cached_geolocator
        worker._cached_image_id = "image-123"
        
        # Get geolocator for same image
        result = worker._get_or_create_geolocator("image-123", mock_sensor_model, mock_elevation_model)
        
        assert result == mock_cached_geolocator

    @patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.TileRequestTable")
    @patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.ImageRequestTable")
    @patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.boto3")
    def test_get_or_create_geolocator_returns_none_without_sensor_model(self, mock_boto3, mock_image_table, mock_tile_table):
        """Test geolocator returns None when sensor_model is None"""
        worker = AsyncResultsWorker(
            worker_id=1,
            feature_table=self.mock_feature_table,
            geolocator=None,
            region_request_table=self.mock_region_request_table,
            in_queue=self.in_queue,
            feature_detector=self.mock_detector,
        )
        
        # Get geolocator without sensor model
        result = worker._get_or_create_geolocator("image-123", None, None)
        
        assert result is None
        assert worker._cached_geolocator is None
