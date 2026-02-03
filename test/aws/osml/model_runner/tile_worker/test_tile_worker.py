#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

import tempfile
from pathlib import Path
from queue import Queue

import pytest


@pytest.fixture
def tile_worker_setup(mocker):
    """Set up common test fixtures."""
    from aws.osml.model_runner.tile_worker.tile_worker import TileWorker

    in_queue = Queue()
    feature_detector = mocker.Mock()
    feature_detector.endpoint = "test-endpoint"
    feature_detector.request_count = 0
    geolocator = mocker.Mock()
    feature_table = mocker.Mock()
    region_request_table = mocker.Mock()

    tile_worker = TileWorker(
        in_queue=in_queue,
        feature_detector=feature_detector,
        geolocator=geolocator,
        feature_table=feature_table,
        region_request_table=region_request_table,
    )

    return tile_worker, feature_detector, region_request_table


def test_process_tile_handles_detector_exception_increments_failed_count(tile_worker_setup):
    """Test that detector exceptions are handled and failed_tile_count is incremented."""
    from aws.osml.model_runner.common import TileState

    # Arrange
    tile_worker, feature_detector, region_request_table = tile_worker_setup
    feature_detector.find_features.side_effect = RuntimeError("Model invocation failed")

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
        tile_worker.process_tile(image_info)

        # Assert
        assert tile_worker.failed_tile_count == 1
        region_request_table.add_tile.assert_called_once()
        call_args = region_request_table.add_tile.call_args
        # Verify TileState.FAILED was passed
        assert call_args[0][3] == TileState.FAILED


def test_process_tile_emits_error_metric_when_metrics_available(tile_worker_setup, mocker):
    """Test that error metrics are emitted when MetricsLogger is available."""
    from aws_embedded_metrics.logger.metrics_logger import MetricsLogger
    from aws_embedded_metrics.unit import Unit

    from aws.osml.model_runner.app_config import MetricLabels

    # Arrange
    tile_worker, feature_detector, region_request_table = tile_worker_setup
    feature_detector.find_features.side_effect = RuntimeError("Model failed")

    with tempfile.TemporaryDirectory() as temp_dir:
        test_tile_path = Path(temp_dir) / "test_tile.tif"
        test_tile_path.write_bytes(b"fake_image_data")

        image_info = {
            "image_path": str(test_tile_path),
            "region": [[0, 0], [512, 512]],
            "image_id": "img_123",
            "region_id": "region_456",
        }

        mock_metrics = mocker.Mock(spec=MetricsLogger)

        # Act - Call the unwrapped version to pass metrics explicitly
        tile_worker.process_tile.__wrapped__(tile_worker, image_info, metrics=mock_metrics)

        # Assert
        # Verify that put_metric was called with ERRORS
        mock_metrics.put_metric.assert_any_call(MetricLabels.ERRORS, 1, str(Unit.COUNT.value))


def test_process_tile_handles_metrics_none_gracefully(tile_worker_setup):
    """Test that process_tile handles metrics=None without AttributeError."""
    from aws.osml.model_runner.common import TileState

    # Arrange
    tile_worker, feature_detector, region_request_table = tile_worker_setup
    feature_detector.find_features.return_value = {"features": []}

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
        tile_worker.process_tile.__wrapped__(tile_worker, image_info, metrics=None)

        # Assert - No exception should be raised
        # Verify processing completed successfully
        region_request_table.add_tile.assert_called_once()
        call_args = region_request_table.add_tile.call_args
        assert call_args[0][3] == TileState.SUCCEEDED


def test_run_handles_event_loop_cleanup_exception(tile_worker_setup, mocker):
    """Test that event loop cleanup exception is logged but doesn't propagate."""
    # Arrange
    tile_worker, feature_detector, region_request_table = tile_worker_setup

    mock_loop = mocker.Mock()
    mock_loop.close.side_effect = RuntimeError("Event loop cleanup failed")
    mock_asyncio = mocker.patch("aws.osml.model_runner.tile_worker.tile_worker.asyncio")
    mock_asyncio.new_event_loop.return_value = mock_loop

    # Put None on queue to signal shutdown
    tile_worker.in_queue.put(None)

    # Act - run() should complete despite cleanup exception
    tile_worker.run()

    # Assert
    mock_loop.stop.assert_called_once()
    mock_loop.close.assert_called_once()


def test_refine_features_handles_none_bbox_and_geometry(tile_worker_setup, mocker):
    """Test that _refine_features handles None bbox and geometry without error."""
    # Arrange
    tile_worker, feature_detector, region_request_table = tile_worker_setup
    feature_collection = {"features": [{"type": "Feature", "properties": {}, "geometry": None}]}

    image_info = {"region": [[100, 200], [512, 512]], "image_id": "img_123", "image_path": "/tmp/tile.tif"}

    # Mock property accessor to return None for bbox and geometry
    tile_worker.property_accessor = mocker.Mock()
    tile_worker.property_accessor.get_image_bbox.return_value = None
    tile_worker.property_accessor.get_image_geometry.return_value = None
    tile_worker.property_accessor.find_image_geometry.return_value = None

    # Act
    features = tile_worker._refine_features.__wrapped__(tile_worker, feature_collection, image_info, metrics=None)

    # Assert - Processing should complete without error
    assert len(features) == 1
    # Verify property accessor methods were called
    tile_worker.property_accessor.get_image_bbox.assert_called()
    tile_worker.property_accessor.get_image_geometry.assert_called()


def test_refine_features_with_valid_bbox_translates_coordinates(tile_worker_setup):
    """Test that _refine_features correctly translates bbox by region offset."""
    import geojson

    # Arrange
    tile_worker, feature_detector, region_request_table = tile_worker_setup

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
    features = tile_worker._refine_features.__wrapped__(tile_worker, feature_collection, image_info, metrics=None)

    # Assert
    assert len(features) == 1
    # Verify feature was processed (image_id added)
    assert features[0]["properties"]["image_id"] == "img_123"
