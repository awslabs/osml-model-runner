#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

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
        mock_detector.endpoint = "test-endpoint"

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
        mock_detector.endpoint = "test-endpoint"
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
        mock_detector.endpoint = "test-endpoint"
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
        mock_detector.endpoint = "test-endpoint"
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
        mock_detector.endpoint = "test-endpoint"
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

    def test_process_tile_with_geolocator(self):
        """Test process_tile_with_geolocator method"""
        from aws.osml.model_runner.tile_worker.async_tile_results_worker import AsyncResultsWorker

        in_queue = Queue()
        mock_detector = Mock()
        mock_detector.endpoint = "test-endpoint"
        mock_feature_table = Mock()
        mock_region_request_table = Mock()
        mock_geolocator = Mock()

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

                    image_info = {"tile_id": "test-tile", "region_id": "test-region"}

                    # Mock process_tile to verify it's called
                    with patch.object(worker, "process_tile") as mock_process_tile:
                        worker.process_tile_with_geolocator(image_info, mock_geolocator)

                        # Verify geolocator was set and process_tile was called
                        mock_process_tile.assert_called_once_with(image_info)

    def test_submission_worker_stop(self):
        """Test AsyncSubmissionWorker stop method"""
        from aws.osml.model_runner.tile_worker.async_tile_submission_worker import AsyncSubmissionWorker

        tile_queue = Queue()
        mock_detector = Mock()
        mock_detector.endpoint = "test-endpoint"

        with patch("aws.osml.model_runner.tile_worker.async_tile_submission_worker.TileRequestTable"):
            worker = AsyncSubmissionWorker(
                worker_id=1, tile_queue=tile_queue, feature_detector=mock_detector, tile_request_table=None
            )

            assert worker.running is True
            worker.stop()
            assert worker.running is False

    def test_submission_worker_name(self):
        """Test AsyncSubmissionWorker has correct name"""
        from aws.osml.model_runner.tile_worker.async_tile_submission_worker import AsyncSubmissionWorker

        tile_queue = Queue()
        mock_detector = Mock()

        with patch("aws.osml.model_runner.tile_worker.async_tile_submission_worker.TileRequestTable"):
            worker = AsyncSubmissionWorker(
                worker_id=5, tile_queue=tile_queue, feature_detector=mock_detector, tile_request_table=None
            )

            assert worker.name == "AsyncSubmissionWorker-5"

    def test_submission_worker_process_tile_success(self):
        """Test AsyncSubmissionWorker process_tile_submission success"""
        from aws.osml.model_runner.tile_worker.async_tile_submission_worker import AsyncSubmissionWorker
        import tempfile
        import os

        tile_queue = Queue()
        mock_detector = Mock()
        mock_detector.endpoint = "test-endpoint"
        mock_detector._invoke_async_endpoint.return_value = ("inference-123", "s3://bucket/output", "s3://bucket/failure")

        # Create a temporary file
        with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.tif') as tmp_file:
            tmp_file.write(b"fake image data")
            tmp_path = tmp_file.name

        try:
            tile_info = {
                "job_id": "test-job-123",
                "tile_id": "test-tile-456",
                "region_id": "test-region-789",
                "image_path": tmp_path,
                "region": [[0, 0], [512, 512]],
            }

            mock_tile_table = Mock()

            with patch("aws.osml.model_runner.tile_worker.async_tile_submission_worker.TileRequestTable", return_value=mock_tile_table):
                with patch("aws.osml.model_runner.tile_worker.async_tile_submission_worker.S3_MANAGER") as mock_s3:
                    with patch("aws.osml.model_runner.tile_worker.async_tile_submission_worker.ServiceConfig") as mock_config:
                        mock_s3.upload_payload.return_value = "s3://bucket/input/test.tif"
                        mock_config.async_input_prefix = "async-input/"

                        worker = AsyncSubmissionWorker(
                            worker_id=1, tile_queue=tile_queue, feature_detector=mock_detector, tile_request_table=mock_tile_table
                        )

                        result = worker.process_tile_submission(tile_info)

                        assert result is True
                        mock_s3.upload_payload.assert_called_once()
                        mock_detector._invoke_async_endpoint.assert_called_once()
                        mock_tile_table.update_tile_status.assert_called()
                        mock_tile_table.update_tile_inference_info.assert_called_once()

        finally:
            # Cleanup - file should be removed by worker
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def test_submission_worker_process_tile_failure(self):
        """Test AsyncSubmissionWorker process_tile_submission failure"""
        from aws.osml.model_runner.tile_worker.async_tile_submission_worker import AsyncSubmissionWorker
        import tempfile
        import os

        tile_queue = Queue()
        mock_detector = Mock()
        mock_detector.endpoint = "test-endpoint"
        mock_detector._invoke_async_endpoint.side_effect = Exception("Endpoint invocation failed")

        # Create a temporary file
        with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.tif') as tmp_file:
            tmp_file.write(b"fake image data")
            tmp_path = tmp_file.name

        try:
            tile_info = {
                "job_id": "test-job-123",
                "tile_id": "test-tile-456",
                "region_id": "test-region-789",
                "image_path": tmp_path,
                "region": [[0, 0], [512, 512]],
            }

            mock_tile_table = Mock()

            with patch("aws.osml.model_runner.tile_worker.async_tile_submission_worker.TileRequestTable", return_value=mock_tile_table):
                with patch("aws.osml.model_runner.tile_worker.async_tile_submission_worker.S3_MANAGER") as mock_s3:
                    with patch("aws.osml.model_runner.tile_worker.async_tile_submission_worker.ServiceConfig") as mock_config:
                        mock_s3.upload_payload.return_value = "s3://bucket/input/test.tif"
                        mock_config.async_input_prefix = "async-input/"

                        worker = AsyncSubmissionWorker(
                            worker_id=1, tile_queue=tile_queue, feature_detector=mock_detector, tile_request_table=mock_tile_table
                        )

                        result = worker.process_tile_submission(tile_info)

                        assert result is False
                        # Verify FAILED status was set
                        mock_tile_table.update_tile_status.assert_called()

        finally:
            # Cleanup
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def test_results_worker_name(self):
        """Test AsyncResultsWorker has correct name"""
        from aws.osml.model_runner.tile_worker.async_tile_results_worker import AsyncResultsWorker

        in_queue = Queue()
        mock_detector = Mock()
        mock_detector.endpoint = "test-endpoint"
        mock_feature_table = Mock()
        mock_region_request_table = Mock()

        with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.TileRequestTable"):
            with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.ImageRequestTable"):
                with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.ServiceConfig") as mock_config:
                    mock_config.tile_request_table = "test-tile-table"
                    mock_config.image_request_table = "test-job-table"

                    worker = AsyncResultsWorker(
                        worker_id=3,
                        feature_table=mock_feature_table,
                        geolocator=None,
                        region_request_table=mock_region_request_table,
                        in_queue=in_queue,
                        feature_detector=mock_detector,
                    )

                    assert worker.name == "AsyncResultsWorker-3"

    def test_get_or_create_geolocator_no_sensor_model(self):
        """Test _get_or_create_geolocator returns None when no sensor model"""
        from aws.osml.model_runner.tile_worker.async_tile_results_worker import AsyncResultsWorker

        in_queue = Queue()
        mock_detector = Mock()
        mock_detector.endpoint = "test-endpoint"
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

                    # Call with no sensor model
                    result = worker._get_or_create_geolocator("image1", None, None)

                    # Should return None and clear cache
                    assert result is None
                    assert worker._cached_geolocator is None
                    assert worker._cached_image_id is None

    def test_submission_worker_process_tile_submission_success(self):
        """Test AsyncSubmissionWorker process_tile_submission success path"""
        from aws.osml.model_runner.tile_worker.async_tile_submission_worker import AsyncSubmissionWorker
        from aws.osml.model_runner.common import RequestStatus
        import tempfile
        import os

        tile_queue = Queue()
        mock_detector = Mock()
        mock_detector.endpoint = "test-endpoint"
        mock_detector._invoke_async_endpoint.return_value = ("inference-123", "s3://output", "s3://failure")

        # Create a temporary file
        with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.tif') as tmp_file:
            tmp_file.write(b"fake image data")
            tmp_path = tmp_file.name

        try:
            tile_info = {
                "job_id": "test-job-123",
                "tile_id": "test-tile-456",
                "region_id": "test-region-789",
                "image_path": tmp_path,
                "region": [[0, 0], [512, 512]],
            }

            with patch("aws.osml.model_runner.tile_worker.async_tile_submission_worker.TileRequestTable") as mock_table_class:
                with patch("aws.osml.model_runner.tile_worker.async_tile_submission_worker.S3_MANAGER") as mock_s3:
                    with patch("aws.osml.model_runner.tile_worker.async_tile_submission_worker.ServiceConfig") as mock_config:
                        mock_config.async_input_prefix = "input/"
                        mock_s3.upload_payload.return_value = "s3://bucket/input/test.tif"
                        mock_table = Mock()
                        mock_table_class.return_value = mock_table

                        worker = AsyncSubmissionWorker(
                            worker_id=1, tile_queue=tile_queue, feature_detector=mock_detector, tile_request_table=mock_table
                        )

                        result = worker.process_tile_submission(tile_info)

                        assert result is True
                        mock_s3.upload_payload.assert_called_once()
                        mock_detector._invoke_async_endpoint.assert_called_once()
                        mock_table.update_tile_status.assert_called()
                        mock_table.update_tile_inference_info.assert_called_once()

        finally:
            # Cleanup - file should be removed by worker, but check anyway
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def test_submission_worker_process_tile_submission_failure(self):
        """Test AsyncSubmissionWorker process_tile_submission failure path"""
        from aws.osml.model_runner.tile_worker.async_tile_submission_worker import AsyncSubmissionWorker
        from aws.osml.model_runner.common import RequestStatus
        import tempfile
        import os

        tile_queue = Queue()
        mock_detector = Mock()
        mock_detector.endpoint = "test-endpoint"
        mock_detector._invoke_async_endpoint.side_effect = Exception("Endpoint invocation failed")

        # Create a temporary file
        with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.tif') as tmp_file:
            tmp_file.write(b"fake image data")
            tmp_path = tmp_file.name

        try:
            tile_info = {
                "job_id": "test-job-123",
                "tile_id": "test-tile-456",
                "region_id": "test-region-789",
                "image_path": tmp_path,
                "region": [[0, 0], [512, 512]],
            }

            with patch("aws.osml.model_runner.tile_worker.async_tile_submission_worker.TileRequestTable") as mock_table_class:
                with patch("aws.osml.model_runner.tile_worker.async_tile_submission_worker.S3_MANAGER") as mock_s3:
                    with patch("aws.osml.model_runner.tile_worker.async_tile_submission_worker.ServiceConfig") as mock_config:
                        mock_config.async_input_prefix = "input/"
                        mock_s3.upload_payload.return_value = "s3://bucket/input/test.tif"
                        mock_table = Mock()
                        mock_table_class.return_value = mock_table

                        worker = AsyncSubmissionWorker(
                            worker_id=1, tile_queue=tile_queue, feature_detector=mock_detector, tile_request_table=mock_table
                        )

                        result = worker.process_tile_submission(tile_info)

                        assert result is False
                        # Verify failure status was updated
                        mock_table.update_tile_status.assert_called()

        finally:
            # Cleanup
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def test_submission_worker_process_tile_submission_without_table(self):
        """Test AsyncSubmissionWorker process_tile_submission without tile request table"""
        from aws.osml.model_runner.tile_worker.async_tile_submission_worker import AsyncSubmissionWorker
        import tempfile
        import os

        tile_queue = Queue()
        mock_detector = Mock()
        mock_detector.endpoint = "test-endpoint"
        mock_detector._invoke_async_endpoint.return_value = ("inference-123", "s3://output", "s3://failure")

        # Create a temporary file
        with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.tif') as tmp_file:
            tmp_file.write(b"fake image data")
            tmp_path = tmp_file.name

        try:
            tile_info = {
                "job_id": "test-job-123",
                "tile_id": "test-tile-456",
                "region_id": "test-region-789",
                "image_path": tmp_path,
                "region": [[0, 0], [512, 512]],
            }

            with patch("aws.osml.model_runner.tile_worker.async_tile_submission_worker.TileRequestTable"):
                with patch("aws.osml.model_runner.tile_worker.async_tile_submission_worker.S3_MANAGER") as mock_s3:
                    with patch("aws.osml.model_runner.tile_worker.async_tile_submission_worker.ServiceConfig") as mock_config:
                        mock_config.async_input_prefix = "input/"
                        mock_s3.upload_payload.return_value = "s3://bucket/input/test.tif"

                        worker = AsyncSubmissionWorker(
                            worker_id=1, tile_queue=tile_queue, feature_detector=mock_detector, tile_request_table=None
                        )

                        result = worker.process_tile_submission(tile_info)

                        assert result is True
                        mock_s3.upload_payload.assert_called_once()

        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)


    def test_results_worker_get_or_create_geolocator_cached(self):
        """Test AsyncResultsWorker geolocator caching"""
        from aws.osml.model_runner.tile_worker.async_tile_results_worker import AsyncResultsWorker

        in_queue = Queue()
        mock_detector = Mock()
        mock_detector.endpoint = "test-endpoint"
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

                    mock_sensor_model = Mock()
                    mock_elevation_model = Mock()

                    # First call - creates geolocator
                    with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.Geolocator") as mock_geo_class:
                        mock_geolocator = Mock()
                        mock_geo_class.return_value = mock_geolocator

                        result1 = worker._get_or_create_geolocator("image-123", mock_sensor_model, mock_elevation_model)

                        assert result1 == mock_geolocator
                        assert worker._cached_image_id == "image-123"
                        mock_geo_class.assert_called_once()

                        # Second call with same image_id - uses cache
                        result2 = worker._get_or_create_geolocator("image-123", mock_sensor_model, mock_elevation_model)

                        assert result2 == mock_geolocator
                        # Should not create new geolocator
                        mock_geo_class.assert_called_once()

    def test_results_worker_get_or_create_geolocator_no_sensor_model(self):
        """Test AsyncResultsWorker geolocator with no sensor model"""
        from aws.osml.model_runner.tile_worker.async_tile_results_worker import AsyncResultsWorker

        in_queue = Queue()
        mock_detector = Mock()
        mock_detector.endpoint = "test-endpoint"
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

                    result = worker._get_or_create_geolocator("image-123", None, None)

                    assert result is None
                    assert worker._cached_geolocator is None
                    assert worker._cached_image_id is None

    def test_results_worker_clear_geolocator_cache(self):
        """Test AsyncResultsWorker clear_geolocator_cache"""
        from aws.osml.model_runner.tile_worker.async_tile_results_worker import AsyncResultsWorker

        in_queue = Queue()
        mock_detector = Mock()
        mock_detector.endpoint = "test-endpoint"
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

                    # Set cache
                    worker._cached_geolocator = Mock()
                    worker._cached_image_id = "image-123"

                    # Clear cache
                    worker.clear_geolocator_cache()

                    assert worker._cached_geolocator is None
                    assert worker._cached_image_id is None

    def test_results_worker_process_completed_job(self):
        """Test AsyncResultsWorker _process_completed_job"""
        from aws.osml.model_runner.tile_worker.async_tile_results_worker import AsyncResultsWorker
        from aws.osml.model_runner.common import TileState

        in_queue = Queue()
        mock_detector = Mock()
        mock_detector.endpoint = "test-endpoint"
        mock_feature_table = Mock()
        mock_region_request_table = Mock()

        with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.TileRequestTable") as mock_tile_table_class:
            with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.ImageRequestTable"):
                with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.ServiceConfig") as mock_config:
                    with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.S3_MANAGER") as mock_s3:
                        mock_config.tile_request_table = "test-tile-table"
                        mock_config.image_request_table = "test-job-table"

                        mock_tile_table = Mock()
                        mock_tile_table_class.return_value = mock_tile_table

                        # Mock S3 download
                        mock_features = {
                            "type": "FeatureCollection",
                            "features": [
                                {
                                    "type": "Feature",
                                    "geometry": {"type": "Point", "coordinates": [0, 0]},
                                    "properties": {},
                                }
                            ],
                        }
                        mock_s3._download_from_s3.return_value = mock_features

                        worker = AsyncResultsWorker(
                            worker_id=1,
                            feature_table=mock_feature_table,
                            geolocator=None,
                            region_request_table=mock_region_request_table,
                            in_queue=in_queue,
                            feature_detector=mock_detector,
                        )

                        # Mock _refine_features
                        mock_refined_features = [{"type": "Feature", "properties": {}}]
                        with patch.object(worker, "_refine_features", return_value=mock_refined_features):
                            image_info = {
                                "tile_id": "test-tile-123",
                                "region_id": "test-region-789",
                                "image_id": "test-image-456",
                                "job_id": "test-job-001",
                                "region": [[0, 0], [512, 512]],
                            }

                            worker._process_completed_job(image_info, "s3://bucket/output.json")

                            # Verify features were added
                            mock_feature_table.add_features.assert_called_once_with(mock_refined_features)
                            # Verify tile was marked as succeeded
                            mock_region_request_table.add_tile.assert_called_once_with(
                                "test-image-456", "test-region-789", [[0, 0], [512, 512]], TileState.SUCCEEDED
                            )
                            # Verify tile status was updated
                            mock_tile_table.update_tile_status.assert_called_once()

    def test_results_worker_process_completed_job_no_features(self):
        """Test AsyncResultsWorker _process_completed_job with no features"""
        from aws.osml.model_runner.tile_worker.async_tile_results_worker import AsyncResultsWorker
        from aws.osml.model_runner.common import TileState

        in_queue = Queue()
        mock_detector = Mock()
        mock_detector.endpoint = "test-endpoint"
        mock_feature_table = Mock()
        mock_region_request_table = Mock()

        with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.TileRequestTable") as mock_tile_table_class:
            with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.ImageRequestTable"):
                with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.ServiceConfig") as mock_config:
                    with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.S3_MANAGER") as mock_s3:
                        mock_config.tile_request_table = "test-tile-table"
                        mock_config.image_request_table = "test-job-table"

                        mock_tile_table = Mock()
                        mock_tile_table_class.return_value = mock_tile_table

                        # Mock S3 download with empty features
                        mock_features = {"type": "FeatureCollection", "features": []}
                        mock_s3._download_from_s3.return_value = mock_features

                        worker = AsyncResultsWorker(
                            worker_id=1,
                            feature_table=mock_feature_table,
                            geolocator=None,
                            region_request_table=mock_region_request_table,
                            in_queue=in_queue,
                            feature_detector=mock_detector,
                        )

                        with patch.object(worker, "_refine_features", return_value=[]):
                            image_info = {
                                "tile_id": "test-tile-123",
                                "region_id": "test-region-789",
                                "image_id": "test-image-456",
                                "job_id": "test-job-001",
                                "region": [[0, 0], [512, 512]],
                            }

                            worker._process_completed_job(image_info, "s3://bucket/output.json")

                            # Should not add features if empty
                            mock_feature_table.add_features.assert_not_called()
                            # But should still mark tile as succeeded
                            mock_region_request_table.add_tile.assert_called_once()

    def test_results_worker_process_completed_job_exception(self):
        """Test AsyncResultsWorker _process_completed_job with exception"""
        from aws.osml.model_runner.tile_worker.async_tile_results_worker import AsyncResultsWorker

        in_queue = Queue()
        mock_detector = Mock()
        mock_detector.endpoint = "test-endpoint"
        mock_feature_table = Mock()
        mock_region_request_table = Mock()

        with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.TileRequestTable") as mock_tile_table_class:
            with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.ImageRequestTable"):
                with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.ServiceConfig") as mock_config:
                    with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.S3_MANAGER") as mock_s3:
                        mock_config.tile_request_table = "test-tile-table"
                        mock_config.image_request_table = "test-job-table"

                        mock_tile_table = Mock()
                        mock_tile_table_class.return_value = mock_tile_table

                        # Mock S3 download to raise exception
                        mock_s3._download_from_s3.side_effect = Exception("Download failed")

                        worker = AsyncResultsWorker(
                            worker_id=1,
                            feature_table=mock_feature_table,
                            geolocator=None,
                            region_request_table=mock_region_request_table,
                            in_queue=in_queue,
                            feature_detector=mock_detector,
                        )

                        # Mock _handle_failed_job
                        with patch.object(worker, "_handle_failed_job") as mock_handle_failed:
                            image_info = {
                                "tile_id": "test-tile-123",
                                "region_id": "test-region-789",
                                "image_id": "test-image-456",
                                "job_id": "test-job-001",
                            }

                            worker._process_completed_job(image_info, "s3://bucket/output.json")

                            # Should call _handle_failed_job
                            mock_handle_failed.assert_called_once()

    def test_results_worker_handle_failed_job(self):
        """Test AsyncResultsWorker _handle_failed_job"""
        from aws.osml.model_runner.tile_worker.async_tile_results_worker import AsyncResultsWorker
        from aws.osml.model_runner.common import RequestStatus

        in_queue = Queue()
        mock_detector = Mock()
        mock_detector.endpoint = "test-endpoint"
        mock_feature_table = Mock()
        mock_region_request_table = Mock()

        with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.TileRequestTable") as mock_tile_table_class:
            with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.ImageRequestTable"):
                with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.ServiceConfig") as mock_config:
                    mock_config.tile_request_table = "test-tile-table"
                    mock_config.image_request_table = "test-job-table"

                    mock_tile_table = Mock()
                    mock_tile_table_class.return_value = mock_tile_table

                    worker = AsyncResultsWorker(
                        worker_id=1,
                        feature_table=mock_feature_table,
                        geolocator=None,
                        region_request_table=mock_region_request_table,
                        in_queue=in_queue,
                        feature_detector=mock_detector,
                    )

                    image_info = {
                        "tile_id": "test-tile-123",
                        "region_id": "test-region-789",
                    }

                    worker._handle_failed_job(image_info, "Test failure reason")

                    # Verify tile status was updated to FAILED
                    mock_tile_table.update_tile_status.assert_called_once_with(
                        "test-tile-123", "test-region-789", RequestStatus.FAILED, "Test failure reason"
                    )

    def test_results_worker_process_tile_with_geolocator(self):
        """Test AsyncResultsWorker process_tile_with_geolocator"""
        from aws.osml.model_runner.tile_worker.async_tile_results_worker import AsyncResultsWorker

        in_queue = Queue()
        mock_detector = Mock()
        mock_detector.endpoint = "test-endpoint"
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

                    mock_geolocator = Mock()
                    image_info = {"tile_id": "test-tile-123"}

                    # Mock process_tile
                    with patch.object(worker, "process_tile") as mock_process:
                        worker.process_tile_with_geolocator(image_info, mock_geolocator)

                        # Verify process_tile was called
                        mock_process.assert_called_once_with(image_info)
                        # Verify geolocator was restored
                        assert worker.geolocator is None

    def test_results_worker_process_tile_failed_status(self):
        """Test AsyncResultsWorker process_tile with FAILED status"""
        from aws.osml.model_runner.tile_worker.async_tile_results_worker import AsyncResultsWorker
        from aws.osml.model_runner.database import TileRequestItem
        from aws.osml.model_runner.common import RequestStatus

        in_queue = Queue()
        mock_detector = Mock()
        mock_detector.endpoint = "test-endpoint"
        mock_feature_table = Mock()
        mock_region_request_table = Mock()

        with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.TileRequestTable") as mock_tile_table_class:
            with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.ImageRequestTable"):
                with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.ServiceConfig") as mock_config:
                    mock_config.tile_request_table = "test-tile-table"
                    mock_config.image_request_table = "test-job-table"

                    mock_tile_table = Mock()
                    mock_tile_table_class.return_value = mock_tile_table

                    # Mock tile item with FAILED status
                    mock_tile_item = TileRequestItem(
                        tile_id="test-tile-123",
                        region_id="test-region-789",
                        tile_status=RequestStatus.FAILED,
                        error_message="Test error",
                    )
                    mock_tile_table.get_tile_request.return_value = mock_tile_item

                    worker = AsyncResultsWorker(
                        worker_id=1,
                        feature_table=mock_feature_table,
                        geolocator=None,
                        region_request_table=mock_region_request_table,
                        in_queue=in_queue,
                        feature_detector=mock_detector,
                    )

                    # Mock _handle_failed_job
                    with patch.object(worker, "_handle_failed_job") as mock_handle_failed:
                        image_info = {
                            "tile_id": "test-tile-123",
                            "region_id": "test-region-789",
                            "job_id": "test-job-001",
                        }

                        worker.process_tile(image_info)

                        # Should call _handle_failed_job
                        mock_handle_failed.assert_called_once_with(image_info, "Test error")

    def test_results_worker_process_tile_no_output_location(self):
        """Test AsyncResultsWorker process_tile with no output location"""
        from aws.osml.model_runner.tile_worker.async_tile_results_worker import AsyncResultsWorker
        from aws.osml.model_runner.database import TileRequestItem
        from aws.osml.model_runner.common import RequestStatus

        in_queue = Queue()
        mock_detector = Mock()
        mock_detector.endpoint = "test-endpoint"
        mock_feature_table = Mock()
        mock_region_request_table = Mock()

        with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.TileRequestTable") as mock_tile_table_class:
            with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.ImageRequestTable"):
                with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.ServiceConfig") as mock_config:
                    mock_config.tile_request_table = "test-tile-table"
                    mock_config.image_request_table = "test-job-table"

                    mock_tile_table = Mock()
                    mock_tile_table_class.return_value = mock_tile_table

                    # Mock tile item with IN_PROGRESS status but no output location
                    mock_tile_item = TileRequestItem(
                        tile_id="test-tile-123",
                        region_id="test-region-789",
                        tile_status=RequestStatus.IN_PROGRESS,
                    )
                    mock_tile_table.get_tile_request.return_value = mock_tile_item

                    worker = AsyncResultsWorker(
                        worker_id=1,
                        feature_table=mock_feature_table,
                        geolocator=None,
                        region_request_table=mock_region_request_table,
                        in_queue=in_queue,
                        feature_detector=mock_detector,
                    )

                    # Mock _handle_failed_job
                    with patch.object(worker, "_handle_failed_job") as mock_handle_failed:
                        image_info = {
                            "tile_id": "test-tile-123",
                            "region_id": "test-region-789",
                            "job_id": "test-job-001",
                            "output_location": None,  # No output location
                        }

                        worker.process_tile(image_info)

                        # Should call _handle_failed_job
                        mock_handle_failed.assert_called_once_with(image_info, "No output location")

    def test_results_worker_with_assumed_credentials(self):
        """Test AsyncResultsWorker initialization with assumed credentials"""
        from aws.osml.model_runner.tile_worker.async_tile_results_worker import AsyncResultsWorker

        in_queue = Queue()
        mock_detector = Mock()
        mock_feature_table = Mock()
        mock_region_request_table = Mock()

        assumed_creds = {
            "AccessKeyId": "test-key",
            "SecretAccessKey": "test-secret",
            "SessionToken": "test-token",
        }

        with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.TileRequestTable"):
            with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.ImageRequestTable"):
                with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.ServiceConfig") as mock_config:
                    with patch("aws.osml.model_runner.tile_worker.async_tile_results_worker.boto3") as mock_boto3:
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
                            assumed_credentials=assumed_creds,
                        )

                        # Verify boto3 client was created with credentials
                        mock_boto3.client.assert_called_once()
                        call_kwargs = mock_boto3.client.call_args[1]
                        assert call_kwargs["aws_access_key_id"] == "test-key"
                        assert call_kwargs["aws_secret_access_key"] == "test-secret"
                        assert call_kwargs["aws_session_token"] == "test-token"

if __name__ == "__main__":
    unittest.main()
