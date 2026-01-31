#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

from unittest import TestCase, main
from unittest.mock import MagicMock, patch

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


class TestRegionRequestHandler(TestCase):
    def setUp(self):
        # Set up mock dependencies
        self.mock_region_request_table = MagicMock(spec=RegionRequestTable)
        self.mock_image_request_table = MagicMock(spec=ImageRequestTable)
        self.mock_region_status_monitor = MagicMock(spec=RegionStatusMonitor)
        self.mock_tiling_strategy = MagicMock(spec=TilingStrategy)
        self.mock_config = MagicMock(spec=ServiceConfig)

        # Example config properties

        # Instantiate the handler with mocked dependencies
        self.handler = RegionRequestHandler(
            region_request_table=self.mock_region_request_table,
            image_request_table=self.mock_image_request_table,
            region_status_monitor=self.mock_region_status_monitor,
            tiling_strategy=self.mock_tiling_strategy,
            config=self.mock_config,
        )

        # Mock the region request and dataset
        self.mock_raster_dataset = MagicMock(spec=gdal.Dataset)
        self.mock_sensor_model = MagicMock(spec=SensorModel)

        # Mock the region request and dataset
        self.mock_raster_dataset = MagicMock(spec=gdal.Dataset)
        self.mock_sensor_model = MagicMock(spec=SensorModel)

        # Add necessary attributes to mock region request
        self.mock_region_request = RegionRequest(
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
        self.mock_region_request_item = RegionRequestItem.from_region_request(self.mock_region_request)

        # Mock the is_valid function and set to true, so we can reverse for failure testing
        self.mock_region_request.is_valid = MagicMock(return_value=True)

        # Mock the tile workers and queue
        self.mock_tile_queue = MagicMock()
        self.mock_tile_workers = [MagicMock()]

    @patch("aws.osml.model_runner.region_request_handler.setup_tile_workers")
    @patch("aws.osml.model_runner.region_request_handler.process_tiles")
    def test_process_region_request_success(self, mock_process_tiles, mock_setup_workers):
        """
        Test successful region processing.
        """
        # Mock tile processing behavior
        mock_setup_workers.return_value = (self.mock_tile_queue, self.mock_tile_workers)
        mock_process_tiles.return_value = (10, 0)  # total_tiles, failed_tiles
        self.mock_region_request_table.start_region_request.return_value = self.mock_region_request_item
        self.mock_region_request_table.update_region_request.return_value = self.mock_region_request_item
        self.mock_image_request_table.complete_region_request.return_value = MagicMock(spec=ImageRequestItem)

        # Call process_region_request
        result = self.handler.process_region_request(
            region_request=self.mock_region_request,
            region_request_item=self.mock_region_request_item,
            raster_dataset=self.mock_raster_dataset,
            sensor_model=self.mock_sensor_model,
        )

        # Assert that the region request was started and updated correctly
        self.mock_region_request_table.start_region_request.assert_called_once_with(self.mock_region_request_item)
        self.mock_region_request_table.update_region_request.assert_called_once()
        self.mock_image_request_table.complete_region_request.assert_called_once()
        self.mock_region_status_monitor.process_event.assert_called()
        assert isinstance(result, ImageRequestItem)

    def test_process_region_request_invalid_request(self):
        """
        Test processing with an invalid RegionRequest.
        """
        # Simulate an invalid region request
        self.mock_region_request.is_valid.return_value = False

        # Assert that ValueError is raised for invalid region request
        with self.assertRaises(ValueError):
            self.handler.process_region_request(
                region_request=self.mock_region_request,
                region_request_item=self.mock_region_request_item,
                raster_dataset=self.mock_raster_dataset,
                sensor_model=self.mock_sensor_model,
            )

    @patch("aws.osml.model_runner.region_request_handler.setup_tile_workers")
    @patch("aws.osml.model_runner.region_request_handler.process_tiles")
    def test_process_region_request_exception(self, mock_process_tiles, mock_setup_workers):
        """
        Test region processing failure scenario.
        """
        mock_setup_workers.return_value = (self.mock_tile_queue, self.mock_tile_workers)
        mock_process_tiles.side_effect = Exception("Tile processing failed")

        self.mock_region_request_table.start_region_request.return_value = self.mock_region_request_item
        self.mock_image_request_table.complete_region_request.return_value = MagicMock(spec=ImageRequestItem)

        # Call process_region_request and expect failure
        result = self.handler.process_region_request(
            region_request=self.mock_region_request,
            region_request_item=self.mock_region_request_item,
            raster_dataset=self.mock_raster_dataset,
            sensor_model=self.mock_sensor_model,
        )

        # Assert that fail_region_request was called due to failure
        self.mock_region_request_table.start_region_request.assert_called_once()
        self.mock_region_status_monitor.process_event.assert_called()
        assert self.mock_region_request_item.message == "Failed to process image region: Tile processing failed"
        assert isinstance(result, ImageRequestItem)

    def test_fail_region_request(self):
        """
        Test fail_region_request method behavior.
        """
        self.mock_image_request_table.complete_region_request.return_value = MagicMock(spec=ImageRequestItem)
        result = self.handler.fail_region_request(self.mock_region_request_item)

        # Assert that the region request was updated with FAILED status
        self.mock_region_request_table.complete_region_request.assert_called_once_with(
            self.mock_region_request_item, RequestStatus.FAILED
        )
        self.mock_region_status_monitor.process_event.assert_called_once()
        self.mock_image_request_table.complete_region_request.assert_called_once()
        assert isinstance(result, ImageRequestItem)

    @patch("aws.osml.model_runner.region_request_handler.setup_tile_workers")
    @patch("aws.osml.model_runner.region_request_handler.process_tiles")
    def test_process_region_request_with_metrics_logger(self, mock_process_tiles, mock_setup_workers):
        """Test process_region_request with MetricsLogger sets dimensions"""
        from unittest.mock import Mock

        from aws_embedded_metrics.logger.metrics_logger import MetricsLogger

        # Mock tile processing
        mock_setup_workers.return_value = (self.mock_tile_queue, self.mock_tile_workers)
        mock_process_tiles.return_value = (10, 0)
        self.mock_region_request_table.start_region_request.return_value = self.mock_region_request_item
        self.mock_region_request_table.update_region_request.return_value = self.mock_region_request_item
        self.mock_image_request_table.complete_region_request.return_value = MagicMock(spec=ImageRequestItem)

        # Create mock metrics logger
        mock_metrics = Mock(spec=MetricsLogger)

        # Call with metrics
        self.handler.process_region_request.__wrapped__(
            self.handler,
            region_request=self.mock_region_request,
            region_request_item=self.mock_region_request_item,
            raster_dataset=self.mock_raster_dataset,
            sensor_model=self.mock_sensor_model,
            metrics=mock_metrics,
        )

        # Assert metrics methods were called
        mock_metrics.set_dimensions.assert_called()
        mock_metrics.put_dimensions.assert_called()
        mock_metrics.put_metric.assert_called()

    def test_fail_region_request_exception_handling(self):
        """Test fail_region_request handles exception in status update"""
        from aws.osml.model_runner.exceptions import ProcessRegionException

        # Mock to raise exception during complete_region_request
        self.mock_region_request_table.complete_region_request.side_effect = Exception("DDB error")

        # Act / Assert - should raise ProcessRegionException
        with self.assertRaises(ProcessRegionException):
            self.handler.fail_region_request(self.mock_region_request_item)


if __name__ == "__main__":
    main()
