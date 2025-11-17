#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

import unittest
from unittest import TestCase
from unittest.mock import Mock, MagicMock, patch, PropertyMock

import pytest
from osgeo import gdal

from aws.osml.model_runner.common import RequestStatus


class TestRegionRequestHandler(TestCase):
    """Unit tests for RegionRequestHandler class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_region_request_table = Mock()
        self.mock_image_request_table = Mock()
        self.mock_region_status_monitor = Mock()
        self.mock_endpoint_statistics_table = Mock()
        self.mock_tiling_strategy = Mock()
        self.mock_endpoint_utils = Mock()
        self.mock_config = Mock()
        self.mock_tile_request_table = Mock()
        self.mock_tile_request_queue = Mock()

    def test_handler_initialization(self):
        """Test RegionRequestHandler initialization"""
        from aws.osml.model_runner.region_request_handler import RegionRequestHandler

        handler = RegionRequestHandler(
            region_request_table=self.mock_region_request_table,
            image_request_table=self.mock_image_request_table,
            region_status_monitor=self.mock_region_status_monitor,
            endpoint_statistics_table=self.mock_endpoint_statistics_table,
            tiling_strategy=self.mock_tiling_strategy,
            endpoint_utils=self.mock_endpoint_utils,
            config=self.mock_config,
            tile_request_table=self.mock_tile_request_table,
            tile_request_queue=self.mock_tile_request_queue,
        )

        assert handler.region_request_table == self.mock_region_request_table
        assert handler.image_request_table == self.mock_image_request_table
        assert handler.region_status_monitor == self.mock_region_status_monitor
        assert handler.endpoint_statistics_table == self.mock_endpoint_statistics_table
        assert handler.tiling_strategy == self.mock_tiling_strategy
        assert handler.endpoint_utils == self.mock_endpoint_utils
        assert handler.config == self.mock_config

    def test_process_region_request_realtime(self):
        """Test process_region_request with realtime mode"""
        from aws.osml.model_runner.region_request_handler import RegionRequestHandler
        from aws.osml.model_runner.api import RegionRequest, ModelInvokeMode
        from aws.osml.model_runner.database import RegionRequestItem, ImageRequestItem

        handler = RegionRequestHandler(
            region_request_table=self.mock_region_request_table,
            image_request_table=self.mock_image_request_table,
            region_status_monitor=self.mock_region_status_monitor,
            endpoint_statistics_table=self.mock_endpoint_statistics_table,
            tiling_strategy=self.mock_tiling_strategy,
            endpoint_utils=self.mock_endpoint_utils,
            config=self.mock_config,
        )

        # Create mock region request
        region_request = RegionRequest(
            job_id="test-job-123",
            region_id="test-region-456",
            image_id="test-image-789",
            image_url="s3://bucket/image.tif",
            model_name="test-model",
            model_invoke_mode=ModelInvokeMode.SM_ENDPOINT,
            region_bounds=[[0, 0], [512, 512]],
        )

        region_request_item = RegionRequestItem(
            region_id="test-region-456",
            image_id="test-image-789",
            job_id="test-job-123",
        )

        # Mock raster dataset
        mock_raster = Mock(spec=gdal.Dataset)
        mock_driver = Mock()
        mock_driver.ShortName = "GTiff"
        mock_raster.GetDriver.return_value = mock_driver

        # Mock image request item
        mock_image_item = ImageRequestItem(
            image_id="test-image-789",
            job_id="test-job-123",
            image_url="s3://bucket/image.tif",
        )

        self.mock_image_request_table.complete_region_request.return_value = mock_image_item
        self.mock_region_request_table.complete_region_request.return_value = region_request_item
        self.mock_region_request_table.update_region_request.return_value = region_request_item
        self.mock_region_status_monitor.get_status.return_value = RequestStatus.SUCCESS
        self.mock_config.self_throttling = False

        with patch("aws.osml.model_runner.region_request_handler.setup_tile_workers") as mock_setup:
            with patch("aws.osml.model_runner.region_request_handler.TileProcessor") as mock_processor_class:
                mock_queue = Mock()
                mock_workers = [Mock()]
                mock_setup.return_value = (mock_queue, mock_workers)

                mock_processor = Mock()
                mock_processor.process_tiles.return_value = (10, 0)  # 10 total, 0 failed
                mock_processor_class.return_value = mock_processor

                result = handler.process_region_request(
                    region_request=region_request,
                    region_request_item=region_request_item,
                    raster_dataset=mock_raster,
                )

                assert result == mock_image_item
                self.mock_region_request_table.start_region_request.assert_called_once()
                self.mock_image_request_table.complete_region_request.assert_called_once()

    def test_process_region_request_async(self):
        """Test process_region_request with async mode"""
        from aws.osml.model_runner.region_request_handler import RegionRequestHandler
        from aws.osml.model_runner.api import RegionRequest, ModelInvokeMode
        from aws.osml.model_runner.database import RegionRequestItem, ImageRequestItem

        handler = RegionRequestHandler(
            region_request_table=self.mock_region_request_table,
            image_request_table=self.mock_image_request_table,
            region_status_monitor=self.mock_region_status_monitor,
            endpoint_statistics_table=self.mock_endpoint_statistics_table,
            tiling_strategy=self.mock_tiling_strategy,
            endpoint_utils=self.mock_endpoint_utils,
            config=self.mock_config,
            tile_request_table=self.mock_tile_request_table,
        )

        region_request = RegionRequest(
            job_id="test-job-123",
            region_id="test-region-456",
            image_id="test-image-789",
            image_url="s3://bucket/image.tif",
            model_name="test-model",
            model_invoke_mode=ModelInvokeMode.SM_ENDPOINT_ASYNC,
            region_bounds=[[0, 0], [512, 512]],
        )

        region_request_item = RegionRequestItem(
            region_id="test-region-456",
            image_id="test-image-789",
            job_id="test-job-123",
        )

        mock_raster = Mock(spec=gdal.Dataset)
        mock_driver = Mock()
        mock_driver.ShortName = "GTiff"
        mock_raster.GetDriver.return_value = mock_driver

        mock_image_item = ImageRequestItem(
            image_id="test-image-789",
            job_id="test-job-123",
            image_url="s3://bucket/image.tif",
        )

        self.mock_image_request_table.get_image_request.return_value = mock_image_item
        self.mock_region_request_table.update_region_request.return_value = region_request_item

        with patch("aws.osml.model_runner.region_request_handler.setup_submission_tile_workers") as mock_setup:
            with patch("aws.osml.model_runner.region_request_handler.AsyncTileProcessor") as mock_processor_class:
                with patch("aws.osml.model_runner.region_request_handler.ServiceConfig") as mock_service_config:
                    mock_service_config.self_throttling = False
                    mock_service_config.elevation_model = None

                    mock_queue = Mock()
                    mock_workers = [Mock()]
                    mock_setup.return_value = (mock_queue, mock_workers)

                    mock_processor = Mock()
                    mock_processor.process_tiles.return_value = (10, 0)
                    mock_processor_class.return_value = mock_processor

                    result = handler.process_region_request(
                        region_request=region_request,
                        region_request_item=region_request_item,
                        raster_dataset=mock_raster,
                    )

                    assert result == mock_image_item
                    self.mock_region_request_table.start_region_request.assert_called_once()

    def test_process_region_request_batch_not_implemented(self):
        """Test process_region_request with batch mode raises NotImplementedError"""
        from aws.osml.model_runner.region_request_handler import RegionRequestHandler
        from aws.osml.model_runner.api import RegionRequest, ModelInvokeMode
        from aws.osml.model_runner.database import RegionRequestItem

        handler = RegionRequestHandler(
            region_request_table=self.mock_region_request_table,
            image_request_table=self.mock_image_request_table,
            region_status_monitor=self.mock_region_status_monitor,
            endpoint_statistics_table=self.mock_endpoint_statistics_table,
            tiling_strategy=self.mock_tiling_strategy,
            endpoint_utils=self.mock_endpoint_utils,
            config=self.mock_config,
        )

        region_request = RegionRequest(
            job_id="test-job-123",
            region_id="test-region-456",
            image_id="test-image-789",
            image_url="s3://bucket/image.tif",
            model_name="test-model",
            model_invoke_mode=ModelInvokeMode.SM_BATCH,
            region_bounds=[[0, 0], [512, 512]],
        )

        region_request_item = RegionRequestItem(
            region_id="test-region-456",
            image_id="test-image-789",
            job_id="test-job-123",
        )

        mock_raster = Mock(spec=gdal.Dataset)

        with pytest.raises(NotImplementedError):
            handler.process_region_request(
                region_request=region_request,
                region_request_item=region_request_item,
                raster_dataset=mock_raster,
            )

    def test_fail_region_request(self):
        """Test fail_region_request method"""
        from aws.osml.model_runner.region_request_handler import RegionRequestHandler
        from aws.osml.model_runner.database import RegionRequestItem, ImageRequestItem

        handler = RegionRequestHandler(
            region_request_table=self.mock_region_request_table,
            image_request_table=self.mock_image_request_table,
            region_status_monitor=self.mock_region_status_monitor,
            endpoint_statistics_table=self.mock_endpoint_statistics_table,
            tiling_strategy=self.mock_tiling_strategy,
            endpoint_utils=self.mock_endpoint_utils,
            config=self.mock_config,
        )

        region_request_item = RegionRequestItem(
            region_id="test-region-456",
            image_id="test-image-789",
            job_id="test-job-123",
        )

        mock_image_item = ImageRequestItem(
            image_id="test-image-789",
            job_id="test-job-123",
            image_url="s3://bucket/image.tif",
        )

        self.mock_region_request_table.complete_region_request.return_value = region_request_item
        self.mock_image_request_table.complete_region_request.return_value = mock_image_item

        result = handler.fail_region_request(region_request_item)

        assert result == mock_image_item
        self.mock_region_request_table.complete_region_request.assert_called_once_with(
            region_request_item, RequestStatus.FAILED
        )
        self.mock_image_request_table.complete_region_request.assert_called_once_with(
            region_request_item.image_id, error=True
        )

    def test_fail_region_request_with_exception(self):
        """Test fail_region_request when table update fails"""
        from aws.osml.model_runner.region_request_handler import RegionRequestHandler
        from aws.osml.model_runner.database import RegionRequestItem
        from aws.osml.model_runner.exceptions import ProcessRegionException

        handler = RegionRequestHandler(
            region_request_table=self.mock_region_request_table,
            image_request_table=self.mock_image_request_table,
            region_status_monitor=self.mock_region_status_monitor,
            endpoint_statistics_table=self.mock_endpoint_statistics_table,
            tiling_strategy=self.mock_tiling_strategy,
            endpoint_utils=self.mock_endpoint_utils,
            config=self.mock_config,
        )

        region_request_item = RegionRequestItem(
            region_id="test-region-456",
            image_id="test-image-789",
            job_id="test-job-123",
        )

        self.mock_region_request_table.complete_region_request.side_effect = Exception("Table update failed")

        with pytest.raises(ProcessRegionException):
            handler.fail_region_request(region_request_item)

    def test_process_region_request_realtime_with_self_throttling(self):
        """Test process_region_request_realtime with self-throttling enabled"""
        from aws.osml.model_runner.region_request_handler import RegionRequestHandler
        from aws.osml.model_runner.api import RegionRequest, ModelInvokeMode
        from aws.osml.model_runner.database import RegionRequestItem, ImageRequestItem

        self.mock_config.self_throttling = True
        self.mock_config.elevation_model = None

        handler = RegionRequestHandler(
            region_request_table=self.mock_region_request_table,
            image_request_table=self.mock_image_request_table,
            region_status_monitor=self.mock_region_status_monitor,
            endpoint_statistics_table=self.mock_endpoint_statistics_table,
            tiling_strategy=self.mock_tiling_strategy,
            endpoint_utils=self.mock_endpoint_utils,
            config=self.mock_config,
        )

        region_request = RegionRequest(
            job_id="test-job-123",
            region_id="test-region-456",
            image_id="test-image-789",
            image_url="s3://bucket/image.tif",
            model_name="test-model",
            model_invoke_mode=ModelInvokeMode.SM_ENDPOINT,
            region_bounds=[[0, 0], [512, 512]],
        )

        region_request_item = RegionRequestItem(
            region_id="test-region-456",
            image_id="test-image-789",
            job_id="test-job-123",
        )

        mock_raster = Mock(spec=gdal.Dataset)
        mock_driver = Mock()
        mock_driver.ShortName = "GTiff"
        mock_raster.GetDriver.return_value = mock_driver

        mock_image_item = ImageRequestItem(
            image_id="test-image-789",
            job_id="test-job-123",
            image_url="s3://bucket/image.tif",
        )

        # Mock throttling checks
        self.mock_endpoint_utils.calculate_max_regions.return_value = 10
        self.mock_endpoint_statistics_table.current_in_progress_regions.return_value = 5

        self.mock_image_request_table.complete_region_request.return_value = mock_image_item
        self.mock_region_request_table.complete_region_request.return_value = region_request_item
        self.mock_region_request_table.update_region_request.return_value = region_request_item
        self.mock_region_status_monitor.get_status.return_value = RequestStatus.SUCCESS

        with patch("aws.osml.model_runner.region_request_handler.setup_tile_workers") as mock_setup:
            with patch("aws.osml.model_runner.region_request_handler.TileProcessor") as mock_processor_class:
                mock_queue = Mock()
                mock_workers = [Mock()]
                mock_setup.return_value = (mock_queue, mock_workers)

                mock_processor = Mock()
                mock_processor.process_tiles.return_value = (10, 0)
                mock_processor_class.return_value = mock_processor

                result = handler.process_region_request_realtime(
                    region_request=region_request,
                    region_request_item=region_request_item,
                    raster_dataset=mock_raster,
                )

                assert result == mock_image_item
                self.mock_endpoint_statistics_table.upsert_endpoint.assert_called_once()
                self.mock_endpoint_statistics_table.increment_region_count.assert_called_once()
                self.mock_endpoint_statistics_table.decrement_region_count.assert_called_once()

    def test_process_region_request_realtime_throttled(self):
        """Test process_region_request_realtime when throttled"""
        from aws.osml.model_runner.region_request_handler import RegionRequestHandler
        from aws.osml.model_runner.api import RegionRequest, ModelInvokeMode
        from aws.osml.model_runner.database import RegionRequestItem
        from aws.osml.model_runner.exceptions import SelfThrottledRegionException

        self.mock_config.self_throttling = True

        handler = RegionRequestHandler(
            region_request_table=self.mock_region_request_table,
            image_request_table=self.mock_image_request_table,
            region_status_monitor=self.mock_region_status_monitor,
            endpoint_statistics_table=self.mock_endpoint_statistics_table,
            tiling_strategy=self.mock_tiling_strategy,
            endpoint_utils=self.mock_endpoint_utils,
            config=self.mock_config,
        )

        region_request = RegionRequest(
            job_id="test-job-123",
            region_id="test-region-456",
            image_id="test-image-789",
            image_url="s3://bucket/image.tif",
            model_name="test-model",
            model_invoke_mode=ModelInvokeMode.SM_ENDPOINT,
            region_bounds=[[0, 0], [512, 512]],
        )

        region_request_item = RegionRequestItem(
            region_id="test-region-456",
            image_id="test-image-789",
            job_id="test-job-123",
        )

        mock_raster = Mock(spec=gdal.Dataset)
        mock_driver = Mock()
        mock_driver.ShortName = "GTiff"
        mock_raster.GetDriver.return_value = mock_driver

        # Mock throttling - at capacity
        self.mock_endpoint_utils.calculate_max_regions.return_value = 10
        self.mock_endpoint_statistics_table.current_in_progress_regions.return_value = 10

        with pytest.raises(SelfThrottledRegionException):
            handler.process_region_request_realtime(
                region_request=region_request,
                region_request_item=region_request_item,
                raster_dataset=mock_raster,
            )

    def test_process_region_request_realtime_invalid_request(self):
        """Test process_region_request_realtime with invalid request"""
        from aws.osml.model_runner.region_request_handler import RegionRequestHandler
        from aws.osml.model_runner.api import RegionRequest, ModelInvokeMode
        from aws.osml.model_runner.database import RegionRequestItem

        self.mock_config.self_throttling = False

        handler = RegionRequestHandler(
            region_request_table=self.mock_region_request_table,
            image_request_table=self.mock_image_request_table,
            region_status_monitor=self.mock_region_status_monitor,
            endpoint_statistics_table=self.mock_endpoint_statistics_table,
            tiling_strategy=self.mock_tiling_strategy,
            endpoint_utils=self.mock_endpoint_utils,
            config=self.mock_config,
        )

        # Create invalid region request (missing required fields)
        region_request = RegionRequest(
            job_id="test-job-123",
            region_id="test-region-456",
            image_id="test-image-789",
            image_url="",  # Invalid empty URL
            model_name="test-model",
            model_invoke_mode=ModelInvokeMode.SM_ENDPOINT,
            region_bounds=[[0, 0], [512, 512]],
        )

        region_request_item = RegionRequestItem(
            region_id="test-region-456",
            image_id="test-image-789",
            job_id="test-job-123",
        )

        mock_raster = Mock(spec=gdal.Dataset)

        with pytest.raises(ValueError):
            handler.process_region_request_realtime(
                region_request=region_request,
                region_request_item=region_request_item,
                raster_dataset=mock_raster,
            )

    def test_process_region_request_realtime_with_failed_tiles(self):
        """Test process_region_request_realtime with some failed tiles"""
        from aws.osml.model_runner.region_request_handler import RegionRequestHandler
        from aws.osml.model_runner.api import RegionRequest, ModelInvokeMode
        from aws.osml.model_runner.database import RegionRequestItem, ImageRequestItem

        self.mock_config.self_throttling = False
        self.mock_config.elevation_model = None

        handler = RegionRequestHandler(
            region_request_table=self.mock_region_request_table,
            image_request_table=self.mock_image_request_table,
            region_status_monitor=self.mock_region_status_monitor,
            endpoint_statistics_table=self.mock_endpoint_statistics_table,
            tiling_strategy=self.mock_tiling_strategy,
            endpoint_utils=self.mock_endpoint_utils,
            config=self.mock_config,
        )

        region_request = RegionRequest(
            job_id="test-job-123",
            region_id="test-region-456",
            image_id="test-image-789",
            image_url="s3://bucket/image.tif",
            model_name="test-model",
            model_invoke_mode=ModelInvokeMode.SM_ENDPOINT,
            region_bounds=[[0, 0], [512, 512]],
        )

        region_request_item = RegionRequestItem(
            region_id="test-region-456",
            image_id="test-image-789",
            job_id="test-job-123",
        )

        mock_raster = Mock(spec=gdal.Dataset)
        mock_driver = Mock()
        mock_driver.ShortName = "GTiff"
        mock_raster.GetDriver.return_value = mock_driver

        mock_image_item = ImageRequestItem(
            image_id="test-image-789",
            job_id="test-job-123",
            image_url="s3://bucket/image.tif",
        )

        self.mock_image_request_table.complete_region_request.return_value = mock_image_item
        self.mock_region_request_table.complete_region_request.return_value = region_request_item
        self.mock_region_request_table.update_region_request.return_value = region_request_item
        self.mock_region_status_monitor.get_status.return_value = RequestStatus.PARTIAL

        with patch("aws.osml.model_runner.region_request_handler.setup_tile_workers") as mock_setup:
            with patch("aws.osml.model_runner.region_request_handler.TileProcessor") as mock_processor_class:
                mock_queue = Mock()
                mock_workers = [Mock()]
                mock_setup.return_value = (mock_queue, mock_workers)

                mock_processor = Mock()
                mock_processor.process_tiles.return_value = (10, 3)  # 10 total, 3 failed
                mock_processor_class.return_value = mock_processor

                result = handler.process_region_request_realtime(
                    region_request=region_request,
                    region_request_item=region_request_item,
                    raster_dataset=mock_raster,
                )

                assert result == mock_image_item
                # Verify error flag was set due to failed tiles
                self.mock_image_request_table.complete_region_request.assert_called_once_with(
                    region_request.image_id, True
                )

    def test_process_region_request_realtime_exception_handling(self):
        """Test process_region_request_realtime exception handling"""
        from aws.osml.model_runner.region_request_handler import RegionRequestHandler
        from aws.osml.model_runner.api import RegionRequest, ModelInvokeMode
        from aws.osml.model_runner.database import RegionRequestItem, ImageRequestItem

        self.mock_config.self_throttling = False
        self.mock_config.elevation_model = None

        handler = RegionRequestHandler(
            region_request_table=self.mock_region_request_table,
            image_request_table=self.mock_image_request_table,
            region_status_monitor=self.mock_region_status_monitor,
            endpoint_statistics_table=self.mock_endpoint_statistics_table,
            tiling_strategy=self.mock_tiling_strategy,
            endpoint_utils=self.mock_endpoint_utils,
            config=self.mock_config,
        )

        region_request = RegionRequest(
            job_id="test-job-123",
            region_id="test-region-456",
            image_id="test-image-789",
            image_url="s3://bucket/image.tif",
            model_name="test-model",
            model_invoke_mode=ModelInvokeMode.SM_ENDPOINT,
            region_bounds=[[0, 0], [512, 512]],
        )

        region_request_item = RegionRequestItem(
            region_id="test-region-456",
            image_id="test-image-789",
            job_id="test-job-123",
        )

        mock_raster = Mock(spec=gdal.Dataset)
        mock_driver = Mock()
        mock_driver.ShortName = "GTiff"
        mock_raster.GetDriver.return_value = mock_driver

        mock_image_item = ImageRequestItem(
            image_id="test-image-789",
            job_id="test-job-123",
            image_url="s3://bucket/image.tif",
        )

        # Mock fail_region_request
        with patch.object(handler, "fail_region_request", return_value=mock_image_item):
            with patch("aws.osml.model_runner.region_request_handler.setup_tile_workers") as mock_setup:
                # Simulate exception during tile processing
                mock_setup.side_effect = Exception("Tile processing failed")

                result = handler.process_region_request_realtime(
                    region_request=region_request,
                    region_request_item=region_request_item,
                    raster_dataset=mock_raster,
                )

                assert result == mock_image_item
                # Verify fail_region_request was called
                handler.fail_region_request.assert_called_once()


if __name__ == "__main__":
    unittest.main()
