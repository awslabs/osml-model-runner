#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import unittest
from unittest import TestCase
from unittest.mock import Mock, patch, MagicMock

import pytest


class TestTileRequestHandler(TestCase):
    """Unit tests for TileRequestHandler class"""

    def test_handler_initialization(self):
        """Test TileRequestHandler initialization"""
        from aws.osml.model_runner.tile_request_handler import TileRequestHandler

        mock_tile_table = Mock()
        mock_image_request_table = Mock()
        mock_status_monitor = Mock()

        handler = TileRequestHandler(
            tile_request_table=mock_tile_table,
            image_request_table=mock_image_request_table,
            tile_status_monitor=mock_status_monitor,
        )

        assert handler is not None
        assert handler.tile_request_table == mock_tile_table
        assert handler.image_request_table == mock_image_request_table
        assert handler.tile_status_monitor == mock_status_monitor

    def test_process_tile_request_success(self):
        """Test processing a single tile request successfully"""
        from aws.osml.model_runner.tile_request_handler import TileRequestHandler
        from aws.osml.model_runner.api import TileRequest
        from aws.osml.model_runner.database import TileRequestItem

        mock_tile_table = Mock()
        mock_image_request_table = Mock()
        mock_status_monitor = Mock()

        handler = TileRequestHandler(
            tile_request_table=mock_tile_table,
            image_request_table=mock_image_request_table,
            tile_status_monitor=mock_status_monitor,
        )

        tile_request = TileRequest(
            tile_id="test-tile-123",
            image_id="test-image-456",
            region_id="test-region-789",
            job_id="test-job-001",
            image_url="s3://bucket/image.tif",
            image_path="s3://bucket/image.tif",
            tile_bounds=[[0, 0], [1024, 1024]],
            model_name="test-model",
            model_invocation_role="arn:aws:iam::123456789012:role/test-role",
        )

        tile_request_item = TileRequestItem.from_tile_request(tile_request)

        with patch("aws.osml.model_runner.tile_request_handler.load_gdal_dataset") as mock_load:
            with patch("aws.osml.model_runner.tile_request_handler.get_image_path") as mock_get_path:
                with patch("aws.osml.model_runner.tile_request_handler.setup_result_tile_workers") as mock_setup:
                    with patch("aws.osml.model_runner.tile_request_handler.ServiceConfig") as mock_config:
                        # Mock GDAL dataset loading
                        mock_dataset = Mock()
                        mock_sensor_model = Mock()
                        mock_load.return_value = (mock_dataset, mock_sensor_model)
                        mock_get_path.return_value = "/tmp/image.tif"
                        mock_config.elevation_model = None

                        # Mock worker setup
                        mock_queue = Mock()
                        mock_workers = [Mock()]
                        mock_setup.return_value = (mock_queue, mock_workers)

                        # Mock completion queue to return success
                        handler._completion_queue = Mock()
                        handler._completion_queue.get.return_value = {
                            "request_id": "test-request",
                            "status": "completed",
                            "timestamp": 123456789,
                        }

                        # Process the tile request
                        handler.process_tile_request(tile_request, tile_request_item)

                        # Verify worker queue was used
                        mock_queue.put.assert_called_once()

    def test_process_tile_request_failure(self):
        """Test processing a tile request that fails"""
        from aws.osml.model_runner.tile_request_handler import TileRequestHandler
        from aws.osml.model_runner.api import TileRequest
        from aws.osml.model_runner.database import TileRequestItem
        from aws.osml.model_runner.common import RequestStatus

        mock_tile_table = Mock()
        mock_image_request_table = Mock()
        mock_status_monitor = Mock()

        handler = TileRequestHandler(
            tile_request_table=mock_tile_table,
            image_request_table=mock_image_request_table,
            tile_status_monitor=mock_status_monitor,
        )

        tile_request = TileRequest(
            tile_id="test-tile-123",
            image_id="test-image-456",
            region_id="test-region-789",
            job_id="test-job-001",
            image_url="s3://bucket/image.tif",
            image_path="s3://bucket/image.tif",
            tile_bounds=[[0, 0], [1024, 1024]],
            model_name="test-model",
            model_invocation_role="arn:aws:iam::123456789012:role/test-role",
        )

        tile_request_item = TileRequestItem.from_tile_request(tile_request)

        with patch("aws.osml.model_runner.tile_request_handler.load_gdal_dataset") as mock_load:
            with patch("aws.osml.model_runner.tile_request_handler.get_image_path") as mock_get_path:
                # Simulate failure in loading dataset
                mock_load.side_effect = Exception("Failed to load dataset")
                mock_get_path.return_value = "/tmp/image.tif"

                # Process should handle the exception
                handler.process_tile_request(tile_request, tile_request_item)

                # Verify failure was recorded
                mock_tile_table.complete_tile_request.assert_called_once()
                call_args = mock_tile_table.complete_tile_request.call_args
                assert call_args[0][1] == RequestStatus.FAILED

    def test_fail_tile_request(self):
        """Test failing a tile request"""
        from aws.osml.model_runner.tile_request_handler import TileRequestHandler
        from aws.osml.model_runner.database import TileRequestItem
        from aws.osml.model_runner.common import RequestStatus

        mock_tile_table = Mock()
        mock_image_request_table = Mock()
        mock_status_monitor = Mock()

        handler = TileRequestHandler(
            tile_request_table=mock_tile_table,
            image_request_table=mock_image_request_table,
            tile_status_monitor=mock_status_monitor,
        )

        tile_request_item = TileRequestItem(
            tile_id="test-tile-123",
            region_id="test-region-789",
            job_id="test-job-001",
        )

        # Mock the complete_tile_request to return the item
        mock_tile_table.complete_tile_request.return_value = tile_request_item

        result = handler.fail_tile_request(tile_request_item)

        # Verify the tile was marked as failed
        mock_tile_table.complete_tile_request.assert_called_once_with(tile_request_item, RequestStatus.FAILED)
        mock_status_monitor.process_event.assert_called_once()

    def test_shutdown(self):
        """Test shutting down the handler"""
        from aws.osml.model_runner.tile_request_handler import TileRequestHandler

        mock_tile_table = Mock()
        mock_image_request_table = Mock()
        mock_status_monitor = Mock()

        handler = TileRequestHandler(
            tile_request_table=mock_tile_table,
            image_request_table=mock_image_request_table,
            tile_status_monitor=mock_status_monitor,
        )

        # Set up mock worker pool
        mock_worker1 = Mock()
        mock_worker2 = Mock()
        handler._worker_pool = [mock_worker1, mock_worker2]
        handler._work_queue = Mock()

        # Shutdown
        handler.shutdown()

        # Verify shutdown signals were sent
        assert handler._work_queue.put.call_count == 2
        mock_worker1.join.assert_called_once()
        mock_worker2.join.assert_called_once()

    def test_shutdown_without_worker_pool(self):
        """Test shutting down when no worker pool exists"""
        from aws.osml.model_runner.tile_request_handler import TileRequestHandler

        mock_tile_table = Mock()
        mock_image_request_table = Mock()
        mock_status_monitor = Mock()

        handler = TileRequestHandler(
            tile_request_table=mock_tile_table,
            image_request_table=mock_image_request_table,
            tile_status_monitor=mock_status_monitor,
        )

        # Shutdown should not raise exception when no worker pool
        handler.shutdown()


if __name__ == "__main__":
    unittest.main()
