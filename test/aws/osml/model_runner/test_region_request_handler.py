#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

from unittest.mock import MagicMock, Mock, patch

import pytest
from osgeo import gdal

from aws.osml.model_runner.api import RegionRequest
from aws.osml.model_runner.app_config import ServiceConfig
from aws.osml.model_runner.common import RequestStatus
from aws.osml.model_runner.database import (
    ImageRequestItem,
    ImageRequestTable,
    RegionRequestItem,
    RegionRequestTable,
)
from aws.osml.model_runner.region_request_handler import RegionRequestHandler
from aws.osml.model_runner.status import RegionStatusMonitor
from aws.osml.model_runner.tile_worker import TilingStrategy
from aws.osml.photogrammetry import SensorModel


@pytest.fixture
def region_request_handler_setup():
    """Set up test fixtures for RegionRequestHandler tests."""
    # Set up mock dependencies
    mock_region_request_table = MagicMock(spec=RegionRequestTable)
    mock_image_request_table = MagicMock(spec=ImageRequestTable)
    mock_region_status_monitor = MagicMock(spec=RegionStatusMonitor)
    mock_tiling_strategy = MagicMock(spec=TilingStrategy)
    mock_config = MagicMock(spec=ServiceConfig)

    # Instantiate the handler with mocked dependencies
    handler = RegionRequestHandler(
        region_request_table=mock_region_request_table,
        image_request_table=mock_image_request_table,
        region_status_monitor=mock_region_status_monitor,
        tiling_strategy=mock_tiling_strategy,
        config=mock_config,
    )

    # Mock the region request and dataset
    mock_raster_dataset = MagicMock(spec=gdal.Dataset)
    mock_sensor_model = MagicMock(spec=SensorModel)

    # Add necessary attributes to mock region request
    mock_region_request = RegionRequest(
        {
            "tile_size": (10, 10),
            "tile_overlap": (1, 1),
            "tile_format": "NITF",
            "image_id": "test-image-d",
            "image_url": "./test/data/small.ntf",
            "region_bounds": ((0, 0), (50, 50)),
            "model_name": "test-model",
            "model_invoke_mode": "SM_ENDPOINT",
            "image_extension": "NITF",
        }
    )

    # Create a mock item from the request
    mock_region_request_item = RegionRequestItem.from_region_request(mock_region_request)

    # Mock the is_valid function and set to true, so we can reverse for failure testing
    mock_region_request.is_valid = MagicMock(return_value=True)

    # Mock the tile workers and queue
    mock_tile_queue = MagicMock()
    mock_tile_workers = [MagicMock()]

    yield (
        handler,
        mock_region_request_table,
        mock_image_request_table,
        mock_region_status_monitor,
        mock_tiling_strategy,
        mock_config,
        mock_raster_dataset,
        mock_sensor_model,
        mock_region_request,
        mock_region_request_item,
        mock_tile_queue,
        mock_tile_workers,
    )


@patch("aws.osml.model_runner.region_request_handler.setup_tile_workers")
@patch("aws.osml.model_runner.region_request_handler.process_tiles")
def test_process_region_request_success(mock_process_tiles, mock_setup_workers, region_request_handler_setup):
    """
    Test successful region processing.
    """
    (
        handler,
        mock_region_request_table,
        mock_image_request_table,
        mock_region_status_monitor,
        mock_tiling_strategy,
        mock_config,
        mock_raster_dataset,
        mock_sensor_model,
        mock_region_request,
        mock_region_request_item,
        mock_tile_queue,
        mock_tile_workers,
    ) = region_request_handler_setup

    # Mock tile processing behavior
    mock_setup_workers.return_value = (mock_tile_queue, mock_tile_workers)
    mock_process_tiles.return_value = (10, 0)  # total_tiles, failed_tiles
    mock_region_request_table.start_region_request.return_value = mock_region_request_item
    mock_region_request_table.update_region_request.return_value = mock_region_request_item
    mock_image_request_table.complete_region_request.return_value = MagicMock(spec=ImageRequestItem)

    # Call process_region_request
    result = handler.process_region_request(
        region_request=mock_region_request,
        region_request_item=mock_region_request_item,
        raster_dataset=mock_raster_dataset,
        sensor_model=mock_sensor_model,
    )

    # Assert that the region request was started and updated correctly
    mock_region_request_table.start_region_request.assert_called_once_with(mock_region_request_item)
    mock_region_request_table.update_region_request.assert_called_once()
    mock_image_request_table.complete_region_request.assert_called_once()
    mock_region_status_monitor.process_event.assert_called()
    assert isinstance(result, ImageRequestItem)


def test_process_region_request_invalid_request(region_request_handler_setup):
    """
    Test processing with an invalid RegionRequest.
    """
    (
        handler,
        mock_region_request_table,
        mock_image_request_table,
        mock_region_status_monitor,
        mock_tiling_strategy,
        mock_config,
        mock_raster_dataset,
        mock_sensor_model,
        mock_region_request,
        mock_region_request_item,
        mock_tile_queue,
        mock_tile_workers,
    ) = region_request_handler_setup

    # Simulate an invalid region request
    mock_region_request.is_valid.return_value = False

    # Assert that ValueError is raised for invalid region request
    with pytest.raises(ValueError):
        handler.process_region_request(
            region_request=mock_region_request,
            region_request_item=mock_region_request_item,
            raster_dataset=mock_raster_dataset,
            sensor_model=mock_sensor_model,
        )


@patch("aws.osml.model_runner.region_request_handler.setup_tile_workers")
@patch("aws.osml.model_runner.region_request_handler.process_tiles")
def test_process_region_request_exception(mock_process_tiles, mock_setup_workers, region_request_handler_setup):
    """
    Test region processing failure scenario.
    """
    (
        handler,
        mock_region_request_table,
        mock_image_request_table,
        mock_region_status_monitor,
        mock_tiling_strategy,
        mock_config,
        mock_raster_dataset,
        mock_sensor_model,
        mock_region_request,
        mock_region_request_item,
        mock_tile_queue,
        mock_tile_workers,
    ) = region_request_handler_setup

    mock_setup_workers.return_value = (mock_tile_queue, mock_tile_workers)
    mock_process_tiles.side_effect = Exception("Tile processing failed")

    mock_region_request_table.start_region_request.return_value = mock_region_request_item
    mock_image_request_table.complete_region_request.return_value = MagicMock(spec=ImageRequestItem)

    # Call process_region_request and expect failure
    result = handler.process_region_request(
        region_request=mock_region_request,
        region_request_item=mock_region_request_item,
        raster_dataset=mock_raster_dataset,
        sensor_model=mock_sensor_model,
    )

    # Assert that fail_region_request was called due to failure
    mock_region_request_table.start_region_request.assert_called_once()
    mock_region_status_monitor.process_event.assert_called()
    assert mock_region_request_item.message == "Failed to process image region: Tile processing failed"
    assert isinstance(result, ImageRequestItem)


def test_fail_region_request(region_request_handler_setup):
    """
    Test fail_region_request method behavior.
    """
    (
        handler,
        mock_region_request_table,
        mock_image_request_table,
        mock_region_status_monitor,
        mock_tiling_strategy,
        mock_config,
        mock_raster_dataset,
        mock_sensor_model,
        mock_region_request,
        mock_region_request_item,
        mock_tile_queue,
        mock_tile_workers,
    ) = region_request_handler_setup

    mock_image_request_table.complete_region_request.return_value = MagicMock(spec=ImageRequestItem)
    result = handler.fail_region_request(mock_region_request_item)

    # Assert that the region request was updated with FAILED status
    mock_region_request_table.complete_region_request.assert_called_once_with(mock_region_request_item, RequestStatus.FAILED)
    mock_region_status_monitor.process_event.assert_called_once()
    mock_image_request_table.complete_region_request.assert_called_once()
    assert isinstance(result, ImageRequestItem)


@patch("aws.osml.model_runner.region_request_handler.setup_tile_workers")
@patch("aws.osml.model_runner.region_request_handler.process_tiles")
def test_process_region_request_with_metrics_logger(mock_process_tiles, mock_setup_workers, region_request_handler_setup):
    """Test process_region_request with MetricsLogger sets dimensions"""
    from aws_embedded_metrics.logger.metrics_logger import MetricsLogger

    (
        handler,
        mock_region_request_table,
        mock_image_request_table,
        mock_region_status_monitor,
        mock_tiling_strategy,
        mock_config,
        mock_raster_dataset,
        mock_sensor_model,
        mock_region_request,
        mock_region_request_item,
        mock_tile_queue,
        mock_tile_workers,
    ) = region_request_handler_setup

    # Mock tile processing
    mock_setup_workers.return_value = (mock_tile_queue, mock_tile_workers)
    mock_process_tiles.return_value = (10, 0)
    mock_region_request_table.start_region_request.return_value = mock_region_request_item
    mock_region_request_table.update_region_request.return_value = mock_region_request_item
    mock_image_request_table.complete_region_request.return_value = MagicMock(spec=ImageRequestItem)

    # Create mock metrics logger
    mock_metrics = Mock(spec=MetricsLogger)

    # Call with metrics
    handler.process_region_request.__wrapped__(
        handler,
        region_request=mock_region_request,
        region_request_item=mock_region_request_item,
        raster_dataset=mock_raster_dataset,
        sensor_model=mock_sensor_model,
        metrics=mock_metrics,
    )

    # Assert metrics methods were called
    mock_metrics.set_dimensions.assert_called()
    mock_metrics.put_dimensions.assert_called()
    mock_metrics.put_metric.assert_called()


def test_fail_region_request_exception_handling(region_request_handler_setup):
    """Test fail_region_request handles exception in status update"""
    from aws.osml.model_runner.exceptions import ProcessRegionException

    (
        handler,
        mock_region_request_table,
        mock_image_request_table,
        mock_region_status_monitor,
        mock_tiling_strategy,
        mock_config,
        mock_raster_dataset,
        mock_sensor_model,
        mock_region_request,
        mock_region_request_item,
        mock_tile_queue,
        mock_tile_workers,
    ) = region_request_handler_setup

    # Mock to raise exception during complete_region_request
    mock_region_request_table.complete_region_request.side_effect = Exception("DDB error")

    # Act / Assert - should raise ProcessRegionException
    with pytest.raises(ProcessRegionException):
        handler.fail_region_request(mock_region_request_item)
