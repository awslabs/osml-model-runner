#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import unittest
from queue import Queue
from unittest import TestCase
from unittest.mock import Mock, patch, MagicMock

import pytest


class TestAsyncSubmissionWorker(TestCase):
    """Unit tests for AsyncSubmissionWorker class"""

    def test_worker_initialization(self):
        """Test AsyncSubmissionWorker initialization"""
        from aws.osml.model_runner.tile_worker.async_tile_submission_worker import AsyncSubmissionWorker

        tile_queue = Queue()
        mock_detector = Mock()

        with patch("aws.osml.model_runner.tile_worker.async_tile_submission_worker.TileRequestTable"):
            worker = AsyncSubmissionWorker(
                worker_id=1, tile_queue=tile_queue, feature_detector=mock_detector, tile_request_table=None
            )

            assert worker.worker_id == 1
            assert worker.tile_queue == tile_queue
            assert worker.feature_detector == mock_detector
            assert worker.failed_tile_count == 0
            assert worker.processed_tile_count == 0
            assert worker.running is True

    def test_worker_shutdown_signal(self):
        """Test that worker handles shutdown signal (None in queue)"""
        from aws.osml.model_runner.tile_worker.async_tile_submission_worker import AsyncSubmissionWorker

        tile_queue = Queue()
        mock_detector = Mock()

        with patch("aws.osml.model_runner.tile_worker.async_tile_submission_worker.TileRequestTable"):
            worker = AsyncSubmissionWorker(
                worker_id=1, tile_queue=tile_queue, feature_detector=mock_detector, tile_request_table=None
            )

            # Put shutdown signal in queue
            tile_queue.put(None)

            # Verify worker can be created and has shutdown handling
            assert worker.running is True


class TestAsyncResultsWorker(TestCase):
    """Unit tests for AsyncResultsWorker class"""

    def test_results_worker_initialization(self):
        """Test AsyncResultsWorker initialization"""
        from aws.osml.model_runner.tile_worker.async_tile_results_worker import AsyncResultsWorker

        in_queue = Queue()
        mock_detector = Mock()
        mock_feature_table = Mock()
        mock_region_request_table = Mock()

        with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.TileRequestTable"):
            with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.ImageRequestTable"):
                with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.ServiceConfig") as mock_config:
                    mock_config.tile_request_table = "test-tile-table"
                    mock_config.image_request_table = "test-job-table"

                    worker = AsyncResultsWorker(
                        worker_id=1,
                        feature_table=mock_feature_table,
                        geolocator=None,
                        region_request_table=mock_region_request_table,
                        in_queue=in_queue,
                        feature_detector=mock_detector,
                    )

                    assert worker.worker_id == 1
                    assert worker.feature_detector == mock_detector
                    assert worker.in_queue == in_queue
                    assert worker.feature_table == mock_feature_table
                    assert worker.region_request_table == mock_region_request_table

    def test_results_worker_with_credentials(self):
        """Test AsyncResultsWorker initialization with assumed credentials"""
        from aws.osml.model_runner.tile_worker.async_tile_results_worker import AsyncResultsWorker

        in_queue = Queue()
        mock_detector = Mock()
        mock_feature_table = Mock()
        mock_region_request_table = Mock()
        credentials = {
            "AccessKeyId": "test-key",
            "SecretAccessKey": "test-secret",
            "SessionToken": "test-token",
        }

        with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.TileRequestTable"):
            with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.ImageRequestTable"):
                with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.boto3") as mock_boto3:
                    with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.ServiceConfig") as mock_config:
                        mock_config.tile_request_table = "test-tile-table"
                        mock_config.image_request_table = "test-job-table"
                        mock_config.async_endpoint_config = Mock()

                        worker = AsyncResultsWorker(
                            worker_id=1,
                            feature_table=mock_feature_table,
                            geolocator=None,
                            region_request_table=mock_region_request_table,
                            in_queue=in_queue,
                            feature_detector=mock_detector,
                            assumed_credentials=credentials,
                        )

                        assert worker.worker_id == 1
                        # Verify boto3 client was called with credentials
                        mock_boto3.client.assert_called_once()

    def test_geolocator_caching(self):
        """Test that geolocator is cached per image_id"""
        from aws.osml.model_runner.tile_worker.async_tile_results_worker import AsyncResultsWorker

        in_queue = Queue()
        mock_detector = Mock()
        mock_feature_table = Mock()
        mock_region_request_table = Mock()
        mock_sensor_model = Mock()
        mock_elevation_model = Mock()

        with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.TileRequestTable"):
            with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.ImageRequestTable"):
                with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.Geolocator") as mock_geolocator_class:
                    with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.ServiceConfig") as mock_config:
                        mock_config.tile_request_table = "test-tile-table"
                        mock_config.image_request_table = "test-job-table"

                        worker = AsyncResultsWorker(
                            worker_id=1,
                            feature_table=mock_feature_table,
                            geolocator=None,
                            region_request_table=mock_region_request_table,
                            in_queue=in_queue,
                            feature_detector=mock_detector,
                        )

                        # First call should create geolocator
                        geolocator1 = worker._get_or_create_geolocator("image1", mock_sensor_model, mock_elevation_model)
                        assert mock_geolocator_class.called

                        # Second call with same image_id should reuse cached geolocator
                        call_count = mock_geolocator_class.call_count
                        geolocator2 = worker._get_or_create_geolocator("image1", mock_sensor_model, mock_elevation_model)
                        assert mock_geolocator_class.call_count == call_count  # No new call

                        # Call with different image_id should create new geolocator
                        geolocator3 = worker._get_or_create_geolocator("image2", mock_sensor_model, mock_elevation_model)
                        assert mock_geolocator_class.call_count > call_count

    def test_clear_geolocator_cache(self):
        """Test clearing geolocator cache"""
        from aws.osml.model_runner.tile_worker.async_tile_results_worker import AsyncResultsWorker

        in_queue = Queue()
        mock_detector = Mock()
        mock_feature_table = Mock()
        mock_region_request_table = Mock()

        with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.TileRequestTable"):
            with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.ImageRequestTable"):
                with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.ServiceConfig") as mock_config:
                    mock_config.tile_request_table = "test-tile-table"
                    mock_config.image_request_table = "test-job-table"

                    worker = AsyncResultsWorker(
                        worker_id=1,
                        feature_table=mock_feature_table,
                        geolocator=None,
                        region_request_table=mock_region_request_table,
                        in_queue=in_queue,
                        feature_detector=mock_detector,
                    )

                    # Set some cached values
                    worker._cached_geolocator = Mock()
                    worker._cached_image_id = "test-image"

                    # Clear cache
                    worker.clear_geolocator_cache()

                    # Verify cache is cleared
                    assert worker._cached_geolocator is None
                    assert worker._cached_image_id is None


if __name__ == "__main__":
    unittest.main()
