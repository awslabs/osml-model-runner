#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

from pathlib import Path
from queue import Queue
from unittest import TestCase, main
from unittest.mock import Mock, MagicMock, patch


class TestTileProcessor(TestCase):

    @staticmethod
    def get_dataset_and_camera():
        from aws.osml.gdal.gdal_utils import load_gdal_dataset

        ds, sensor_model = load_gdal_dataset("./test/data/GeogToWGS84GeoKey5.tif")
        return ds, sensor_model

    @patch("aws.osml.model_runner.tile_worker.tile_worker_utils.FeatureDetectorFactory", autospec=True)
    @patch("aws.osml.model_runner.tile_worker.tile_worker_utils.FeatureTable", autospec=True)
    @patch("aws.osml.model_runner.tile_worker.tile_worker_utils.RegionRequestTable", autospec=True)
    def test_process_tiles(self, mock_region_request_table, mock_feature_table, mock_feature_detector_factory):
        """
        Test processing of image tiles using a tiling strategy, ensuring all expected tiles are processed
        without errors. The test also validates successful integration with GDAL datasets.
        """
        from aws.osml.model_runner.api import RegionRequest
        from aws.osml.model_runner.database import RegionRequestItem
        from aws.osml.model_runner.tile_worker import VariableTileTilingStrategy
        from aws.osml.model_runner.tile_worker.tile_worker_utils import setup_tile_workers
        from aws.osml.model_runner.tile_worker.tile_processors import TileProcessor

        mock_feature_detector = Mock()
        mock_feature_detector.endpoint = "test-model-endpoint"
        mock_feature_detector.find_features.return_value = {"features": []}
        mock_feature_detector_factory.return_value.build.return_value = mock_feature_detector

        # Mock the database tables
        mock_feature_table.return_value = Mock()
        mock_region_request_table.return_value = Mock()

        # Mock the RegionRequest and RegionRequestItem
        mock_region_request = RegionRequest(
            {
                "tile_size": (10, 10),
                "tile_overlap": (0, 0),
                "tile_format": "NITF",
                "image_id": "1",
                "image_url": "/mock/path",
                "region_bounds": ((0, 0), (50, 50)),
                "model_invoke_mode": "SM_ENDPOINT",
                "image_extension": "fake",
                "failed_tiles": [],
            }
        )
        region_request_item = RegionRequestItem.from_region_request(mock_region_request)

        # Load the testing Dataset and SensorModel
        ds, sensor_model = self.get_dataset_and_camera()

        # Setup tile workers
        work_queue, tile_worker_list = setup_tile_workers(mock_region_request, sensor_model, None)

        # Execute process_tiles
        total_tile_count, tile_error_count = TileProcessor().process_tiles(
            tiling_strategy=VariableTileTilingStrategy(),
            region_request=mock_region_request,
            region_request_item=region_request_item,
            tile_queue=work_queue,
            tile_workers=tile_worker_list,
            raster_dataset=ds,
            sensor_model=sensor_model,
        )

        # Verify expected results
        assert total_tile_count == 25
        assert tile_error_count == 0

    @patch("aws.osml.model_runner.tile_worker.tile_processors.RequestQueue")
    @patch("aws.osml.model_runner.tile_worker.tile_processors.ServiceConfig")
    def test_tile_processor_initialization(self, mock_service_config, mock_request_queue):
        """Test TileProcessor initialization"""
        from aws.osml.model_runner.tile_worker.tile_processors import TileProcessor

        mock_service_config.tile_queue = "test-queue"
        processor = TileProcessor()

        assert processor.shutdown_workers is True
        mock_request_queue.assert_called_once_with("test-queue", wait_seconds=0)

    @patch("aws.osml.model_runner.tile_worker.tile_processors.RequestQueue")
    @patch("aws.osml.model_runner.tile_worker.tile_processors.ServiceConfig")
    def test_handle_tile(self, mock_service_config, mock_request_queue):
        """Test handle_tile method"""
        from aws.osml.model_runner.tile_worker.tile_processors import TileProcessor
        from aws.osml.model_runner.api import RegionRequest
        from aws.osml.model_runner.database import RegionRequestItem

        mock_service_config.tile_queue = "test-queue"
        processor = TileProcessor()

        tile_queue = Queue()
        region_request = RegionRequest(
            {
                "tile_size": (512, 512),
                "tile_overlap": (50, 50),
                "tile_format": "NITF",
                "image_id": "test-image-1",
                "image_url": "s3://bucket/image.tif",
                "region_bounds": ((0, 0), (2048, 2048)),
                "model_invoke_mode": "SM_ENDPOINT",
                "model_name": "test-model",
                "image_extension": "tif",
            }
        )
        region_request_item = RegionRequestItem.from_region_request(region_request)
        tmp_image_path = Path("/tmp/test_image.tif")
        tile_bounds = ((0, 0), (512, 512))

        processor.handle_tile(tile_queue, region_request, region_request_item, tmp_image_path, tile_bounds)

        # Verify item was added to queue
        assert tile_queue.qsize() == 1
        image_info = tile_queue.get()
        assert image_info["image_path"] == tmp_image_path
        assert image_info["region"] == tile_bounds
        assert image_info["image_id"] == region_request_item.image_id

    @patch("aws.osml.model_runner.tile_worker.tile_processors.RequestQueue")
    @patch("aws.osml.model_runner.tile_worker.tile_processors.ServiceConfig")
    def test_get_tile_array(self, mock_service_config, mock_request_queue):
        """Test get_tile_array method"""
        from aws.osml.model_runner.tile_worker.tile_processors import TileProcessor
        from aws.osml.model_runner.api import RegionRequest
        from aws.osml.model_runner.database import RegionRequestItem
        from aws.osml.model_runner.tile_worker import VariableTileTilingStrategy

        mock_service_config.tile_queue = "test-queue"
        processor = TileProcessor()

        region_request = RegionRequest(
            {
                "tile_size": (512, 512),
                "tile_overlap": (0, 0),
                "tile_format": "NITF",
                "image_id": "test-image-1",
                "image_url": "s3://bucket/image.tif",
                "region_bounds": ((0, 0), (1024, 1024)),
                "model_invoke_mode": "SM_ENDPOINT",
                "model_name": "test-model",
                "image_extension": "tif",
            }
        )
        region_request_item = RegionRequestItem.from_region_request(region_request)

        tiling_strategy = VariableTileTilingStrategy()
        tile_array = processor.get_tile_array(tiling_strategy, region_request_item)

        # Should have 4 tiles (2x2 grid)
        assert len(tile_array) == 4

    @patch("aws.osml.model_runner.tile_worker.tile_processors.RequestQueue")
    @patch("aws.osml.model_runner.tile_worker.tile_processors.ServiceConfig")
    def test_get_tile_array_with_succeeded_tiles(self, mock_service_config, mock_request_queue):
        """Test get_tile_array filters out succeeded tiles"""
        from aws.osml.model_runner.tile_worker.tile_processors import TileProcessor
        from aws.osml.model_runner.api import RegionRequest
        from aws.osml.model_runner.database import RegionRequestItem
        from aws.osml.model_runner.tile_worker import VariableTileTilingStrategy

        mock_service_config.tile_queue = "test-queue"
        processor = TileProcessor()

        region_request = RegionRequest(
            {
                "tile_size": (512, 512),
                "tile_overlap": (0, 0),
                "tile_format": "NITF",
                "image_id": "test-image-1",
                "image_url": "s3://bucket/image.tif",
                "region_bounds": ((0, 0), (1024, 1024)),
                "model_invoke_mode": "SM_ENDPOINT",
                "model_name": "test-model",
                "image_extension": "tif",
            }
        )
        region_request_item = RegionRequestItem.from_region_request(region_request)
        # Mark first tile as succeeded
        region_request_item.succeeded_tiles = [[[0, 0], [512, 512]]]

        tiling_strategy = VariableTileTilingStrategy()
        tile_array = processor.get_tile_array(tiling_strategy, region_request_item)

        # Should have 3 tiles (one already succeeded)
        assert len(tile_array) == 3

    @patch("aws.osml.model_runner.tile_worker.tile_processors.RequestQueue")
    @patch("aws.osml.model_runner.tile_worker.tile_processors.ServiceConfig")
    def test_shut_down_workers(self, mock_service_config, mock_request_queue):
        """Test shut_down_workers method"""
        from aws.osml.model_runner.tile_worker.tile_processors import TileProcessor

        mock_service_config.tile_queue = "test-queue"
        processor = TileProcessor()

        tile_queue = Queue()
        mock_worker1 = Mock()
        mock_worker1.failed_tile_count = 1
        mock_worker2 = Mock()
        mock_worker2.failed_tile_count = 2
        tile_workers = [mock_worker1, mock_worker2]

        tile_error_count = processor.shut_down_workers(tile_workers, tile_queue)

        # Verify shutdown signals sent
        assert tile_queue.qsize() == 2
        # Verify workers joined
        mock_worker1.join.assert_called_once()
        mock_worker2.join.assert_called_once()
        # Verify error count
        assert tile_error_count == 3

    @patch("aws.osml.model_runner.tile_worker.tile_processors.RequestQueue")
    @patch("aws.osml.model_runner.tile_worker.tile_processors.ServiceConfig")
    def test_shut_down_workers_disabled(self, mock_service_config, mock_request_queue):
        """Test shut_down_workers when shutdown is disabled"""
        from aws.osml.model_runner.tile_worker.tile_processors import TileProcessor

        mock_service_config.tile_queue = "test-queue"
        processor = TileProcessor()
        processor.shutdown_workers = False

        tile_queue = Queue()
        mock_worker = Mock()
        tile_workers = [mock_worker]

        tile_error_count = processor.shut_down_workers(tile_workers, tile_queue)

        # Verify no shutdown signals sent
        assert tile_queue.qsize() == 0
        # Verify workers not joined
        mock_worker.join.assert_not_called()
        # Verify error count is 0
        assert tile_error_count == 0


class TestAsyncTileProcessor(TestCase):
    @patch("aws.osml.model_runner.tile_worker.tile_processors.RequestQueue")
    @patch("aws.osml.model_runner.tile_worker.tile_processors.ServiceConfig")
    def test_async_tile_processor_initialization(self, mock_service_config, mock_request_queue):
        """Test AsyncTileProcessor initialization"""
        from aws.osml.model_runner.tile_worker.tile_processors import AsyncTileProcessor

        mock_service_config.tile_queue = "test-queue"
        mock_tile_request_table = Mock()

        processor = AsyncTileProcessor(mock_tile_request_table)

        assert processor.tiles_submitted == 0
        assert processor.tile_request_table == mock_tile_request_table

    @patch("aws.osml.model_runner.tile_worker.tile_processors.RequestQueue")
    @patch("aws.osml.model_runner.tile_worker.tile_processors.ServiceConfig")
    def test_async_handle_tile(self, mock_service_config, mock_request_queue):
        """Test AsyncTileProcessor handle_tile method"""
        from aws.osml.model_runner.tile_worker.tile_processors import AsyncTileProcessor
        from aws.osml.model_runner.api import RegionRequest
        from aws.osml.model_runner.database import RegionRequestItem, TileRequestItem
        from aws.osml.model_runner.common import RequestStatus

        mock_service_config.tile_queue = "test-queue"
        mock_service_config.use_tile_poller = False
        mock_tile_request_table = Mock()

        # Mock tile request item
        mock_tile_item = TileRequestItem(
            tile_id="test-tile-123",
            region_id="test-region-789",
            tile_status=RequestStatus.PENDING,
        )
        mock_tile_request_table.get_or_create_tile_request_item.return_value = mock_tile_item

        processor = AsyncTileProcessor(mock_tile_request_table)

        tile_queue = Queue()
        region_request = RegionRequest(
            {
                "tile_size": (512, 512),
                "tile_overlap": (50, 50),
                "tile_format": "NITF",
                "image_id": "test-image-1",
                "image_url": "s3://bucket/image.tif",
                "region_bounds": ((0, 0), (2048, 2048)),
                "model_invoke_mode": "ASYNC",
                "model_name": "test-model",
                "image_extension": "tif",
            }
        )
        region_request_item = RegionRequestItem.from_region_request(region_request)
        tmp_image_path = Path("/tmp/test_image.tif")
        tile_bounds = ((0, 0), (512, 512))

        processor.handle_tile(tile_queue, region_request, region_request_item, tmp_image_path, tile_bounds)

        # Verify tile was submitted
        assert processor.tiles_submitted == 1
        assert tile_queue.qsize() == 1

    @patch("aws.osml.model_runner.tile_worker.tile_processors.RequestQueue")
    @patch("aws.osml.model_runner.tile_worker.tile_processors.ServiceConfig")
    def test_async_handle_tile_already_succeeded(self, mock_service_config, mock_request_queue):
        """Test AsyncTileProcessor skips already succeeded tiles"""
        from aws.osml.model_runner.tile_worker.tile_processors import AsyncTileProcessor
        from aws.osml.model_runner.api import RegionRequest
        from aws.osml.model_runner.database import RegionRequestItem, TileRequestItem
        from aws.osml.model_runner.common import RequestStatus

        mock_service_config.tile_queue = "test-queue"
        mock_tile_request_table = Mock()

        # Mock tile request item that's already succeeded
        mock_tile_item = TileRequestItem(
            tile_id="test-tile-123",
            region_id="test-region-789",
            tile_status=RequestStatus.SUCCESS,
        )
        mock_tile_request_table.get_or_create_tile_request_item.return_value = mock_tile_item

        processor = AsyncTileProcessor(mock_tile_request_table)

        tile_queue = Queue()
        region_request = RegionRequest(
            {
                "tile_size": (512, 512),
                "tile_overlap": (50, 50),
                "tile_format": "NITF",
                "image_id": "test-image-1",
                "image_url": "s3://bucket/image.tif",
                "region_bounds": ((0, 0), (2048, 2048)),
                "model_invoke_mode": "ASYNC",
                "model_name": "test-model",
                "image_extension": "tif",
            }
        )
        region_request_item = RegionRequestItem.from_region_request(region_request)
        tmp_image_path = Path("/tmp/test_image.tif")
        tile_bounds = ((0, 0), (512, 512))

        processor.handle_tile(tile_queue, region_request, region_request_item, tmp_image_path, tile_bounds)

        # Verify tile was NOT submitted (already succeeded)
        assert processor.tiles_submitted == 0
        assert tile_queue.qsize() == 0

    @patch("aws.osml.model_runner.tile_worker.tile_processors.RequestQueue")
    @patch("aws.osml.model_runner.tile_worker.tile_processors.ServiceConfig")
    def test_async_handle_tile_with_poller(self, mock_service_config, mock_request_queue_class):
        """Test AsyncTileProcessor handle_tile with poller enabled"""
        from aws.osml.model_runner.tile_worker.tile_processors import AsyncTileProcessor
        from aws.osml.model_runner.api import RegionRequest
        from aws.osml.model_runner.database import RegionRequestItem, TileRequestItem
        from aws.osml.model_runner.common import RequestStatus

        mock_service_config.tile_queue = "test-queue"
        mock_service_config.use_tile_poller = True
        mock_service_config.tile_poller_delay = 60
        mock_tile_request_table = Mock()

        # Mock tile request item
        mock_tile_item = TileRequestItem(
            tile_id="test-tile-123",
            region_id="test-region-789",
            tile_status=RequestStatus.PENDING,
        )
        mock_tile_request_table.get_or_create_tile_request_item.return_value = mock_tile_item

        processor = AsyncTileProcessor(mock_tile_request_table)

        tile_queue = Queue()
        region_request = RegionRequest(
            {
                "tile_size": (512, 512),
                "tile_overlap": (50, 50),
                "tile_format": "NITF",
                "image_id": "test-image-1",
                "image_url": "s3://bucket/image.tif",
                "region_bounds": ((0, 0), (2048, 2048)),
                "model_invoke_mode": "ASYNC",
                "model_name": "test-model",
                "image_extension": "tif",
            }
        )
        region_request_item = RegionRequestItem.from_region_request(region_request)
        tmp_image_path = Path("/tmp/test_image.tif")
        tile_bounds = ((0, 0), (512, 512))

        processor.handle_tile(tile_queue, region_request, region_request_item, tmp_image_path, tile_bounds)

        # Verify tile was submitted
        assert processor.tiles_submitted == 1
        # Verify poller message was sent
        processor.tile_request_queue.send_request.assert_called_once()


class TestBatchTileProcessor(TestCase):
    @patch("aws.osml.model_runner.tile_worker.tile_processors.RequestQueue")
    @patch("aws.osml.model_runner.tile_worker.tile_processors.ServiceConfig")
    def test_batch_tile_processor_initialization(self, mock_service_config, mock_request_queue):
        """Test BatchTileProcessor initialization"""
        from aws.osml.model_runner.tile_worker.tile_processors import BatchTileProcessor

        mock_service_config.tile_queue = "test-queue"
        mock_tile_request_table = Mock()

        processor = BatchTileProcessor(mock_tile_request_table)

        assert processor.shutdown_workers is False
        assert processor.tiles_submitted == 0
        assert processor.tile_request_table == mock_tile_request_table


if __name__ == "__main__":
    main()
