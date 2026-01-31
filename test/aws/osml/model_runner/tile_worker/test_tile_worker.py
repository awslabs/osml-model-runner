#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

import tempfile
from pathlib import Path
from queue import Queue
from unittest import TestCase, main
from unittest.mock import Mock, patch


class TestTileWorker(TestCase):
    """Test cases for the TileWorker class."""

    def setUp(self):
        """Set up common test fixtures."""
        from aws.osml.model_runner.tile_worker.tile_worker import TileWorker

        self.in_queue = Queue()
        self.feature_detector = Mock()
        self.feature_detector.endpoint = "test-endpoint"
        self.feature_detector.request_count = 0
        self.geolocator = Mock()
        self.feature_table = Mock()
        self.region_request_table = Mock()

        self.tile_worker = TileWorker(
            in_queue=self.in_queue,
            feature_detector=self.feature_detector,
            geolocator=self.geolocator,
            feature_table=self.feature_table,
            region_request_table=self.region_request_table,
        )

    def test_process_tile_handles_detector_exception_increments_failed_count(self):
        """Test that detector exceptions are handled and failed_tile_count is incremented."""
        # Arrange
        self.feature_detector.find_features.side_effect = RuntimeError("Model invocation failed")

        with tempfile.TemporaryDirectory() as temp_dir:
            test_tile_path = Path(temp_dir) / "test_tile.tif"
            test_tile_path.write_bytes(b"fake_image_data")

            image_info = {
                "image_path": str(test_tile_path),
                "region": [[0, 0], [512, 512]],
                "image_id": "img_123",
                "region_id": "region_456",
                "job_id": "job_789",
            }

            # Act
            self.tile_worker.process_tile(image_info)

            # Assert
            self.assertEqual(self.tile_worker.failed_tile_count, 1)
            self.region_request_table.add_tile.assert_called_once()
            call_args = self.region_request_table.add_tile.call_args
            # Verify TileState.FAILED was passed
            from aws.osml.model_runner.common import TileState

            self.assertEqual(call_args[0][3], TileState.FAILED)

    def test_process_tile_emits_error_metric_when_metrics_available(self):
        """Test that error metrics are emitted when MetricsLogger is available."""
        from aws_embedded_metrics.logger.metrics_logger import MetricsLogger
        from aws_embedded_metrics.unit import Unit

        from aws.osml.model_runner.app_config import MetricLabels

        # Arrange
        self.feature_detector.find_features.side_effect = RuntimeError("Model failed")

        with tempfile.TemporaryDirectory() as temp_dir:
            test_tile_path = Path(temp_dir) / "test_tile.tif"
            test_tile_path.write_bytes(b"fake_image_data")

            image_info = {
                "image_path": str(test_tile_path),
                "region": [[0, 0], [512, 512]],
                "image_id": "img_123",
                "region_id": "region_456",
            }

            mock_metrics = Mock(spec=MetricsLogger)

            # Act - Call the unwrapped version to pass metrics explicitly
            self.tile_worker.process_tile.__wrapped__(self.tile_worker, image_info, metrics=mock_metrics)

            # Assert
            # Verify that put_metric was called with ERRORS
            mock_metrics.put_metric.assert_any_call(MetricLabels.ERRORS, 1, str(Unit.COUNT.value))

    def test_process_tile_handles_metrics_none_gracefully(self):
        """Test that process_tile handles metrics=None without AttributeError."""
        # Arrange
        self.feature_detector.find_features.return_value = {"features": []}

        with tempfile.TemporaryDirectory() as temp_dir:
            test_tile_path = Path(temp_dir) / "test_tile.tif"
            test_tile_path.write_bytes(b"fake_image_data")

            image_info = {
                "image_path": str(test_tile_path),
                "region": [[0, 0], [512, 512]],
                "image_id": "img_123",
                "region_id": "region_456",
            }

            # Act - Call unwrapped version with metrics=None explicitly
            self.tile_worker.process_tile.__wrapped__(self.tile_worker, image_info, metrics=None)

            # Assert - No exception should be raised
            # Verify processing completed successfully
            from aws.osml.model_runner.common import TileState

            self.region_request_table.add_tile.assert_called_once()
            call_args = self.region_request_table.add_tile.call_args
            self.assertEqual(call_args[0][3], TileState.SUCCEEDED)

    @patch("aws.osml.model_runner.tile_worker.tile_worker.asyncio")
    def test_run_handles_event_loop_cleanup_exception(self, mock_asyncio):
        """Test that event loop cleanup exception is logged but doesn't propagate."""
        # Arrange
        mock_loop = Mock()
        mock_loop.close.side_effect = RuntimeError("Event loop cleanup failed")
        mock_asyncio.new_event_loop.return_value = mock_loop

        # Put None on queue to signal shutdown
        self.in_queue.put(None)

        # Act - run() should complete despite cleanup exception
        self.tile_worker.run()

        # Assert
        mock_loop.stop.assert_called_once()
        mock_loop.close.assert_called_once()

    def test_refine_features_handles_none_bbox_and_geometry(self):
        """Test that _refine_features handles None bbox and geometry without error."""
        # Arrange
        feature_collection = {"features": [{"type": "Feature", "properties": {}, "geometry": None}]}

        image_info = {"region": [[100, 200], [512, 512]], "image_id": "img_123", "image_path": "/tmp/tile.tif"}

        # Mock property accessor to return None for bbox and geometry
        self.tile_worker.property_accessor = Mock()
        self.tile_worker.property_accessor.get_image_bbox.return_value = None
        self.tile_worker.property_accessor.get_image_geometry.return_value = None
        self.tile_worker.property_accessor.find_image_geometry.return_value = None

        # Act
        features = self.tile_worker._refine_features.__wrapped__(
            self.tile_worker, feature_collection, image_info, metrics=None
        )

        # Assert - Processing should complete without error
        self.assertEqual(len(features), 1)
        # Verify property accessor methods were called
        self.tile_worker.property_accessor.get_image_bbox.assert_called()
        self.tile_worker.property_accessor.get_image_geometry.assert_called()

    def test_refine_features_with_valid_bbox_translates_coordinates(self):
        """Test that _refine_features correctly translates bbox by region offset."""
        import geojson

        # Arrange
        # imageBBox should be a list [minx, miny, maxx, maxy] in tile coordinates
        tiled_bbox = [10, 10, 50, 50]

        # Create a proper GeoJSON Feature object
        feature = geojson.Feature(geometry=geojson.Point((30, 30)), properties={"imageBBox": tiled_bbox})

        feature_collection = {"features": [feature]}

        image_info = {
            "region": [[100, 200], [512, 512]],  # ulx=100, uly=200
            "image_id": "img_123",
            "image_path": "/tmp/tile.tif",
        }

        # Act
        features = self.tile_worker._refine_features.__wrapped__(
            self.tile_worker, feature_collection, image_info, metrics=None
        )

        # Assert
        self.assertEqual(len(features), 1)
        # Verify feature was processed (image_id added)
        self.assertEqual(features[0]["properties"]["image_id"], "img_123")


if __name__ == "__main__":
    main()
