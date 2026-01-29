#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

import tempfile
from pathlib import Path
from unittest import TestCase, main
from unittest.mock import Mock, patch


class TestTileWorkerUtils(TestCase):
    @patch("aws.osml.model_runner.tile_worker.tile_worker_utils.FeatureDetectorFactory")
    @patch("aws.osml.model_runner.tile_worker.tile_worker_utils.TileWorker", autospec=True)
    @patch("aws.osml.model_runner.tile_worker.tile_worker_utils.ServiceConfig", autospec=True)
    def test_setup_tile_workers(self, mock_service_config, mock_tile_worker, mock_feature_detector_factory):
        """
        Test the setup of tile workers, ensuring the correct number of workers is initialized
        based on the configuration and that workers are started correctly.
        """
        from aws.osml.model_runner.api import RegionRequest
        from aws.osml.model_runner.tile_worker.tile_worker_utils import setup_tile_workers

        mock_feature_detector = Mock()
        mock_feature_detector.endpoint = "test-model-endpoint"
        mock_feature_detector.find_features.return_value = {"features": []}
        mock_feature_detector_factory.return_value.build.return_value = mock_feature_detector
        mock_tile_worker.start = Mock()
        mock_num_tile_workers = 4
        mock_service_config.workers = mock_num_tile_workers
        mock_region_request = RegionRequest(
            {
                "tile_size": (10, 10),
                "tile_overlap": (1, 1),
                "tile_format": "NITF",
                "image_id": "1",
                "image_url": "/mock/path",
                "region_bounds": ((0, 0), (50, 50)),
                "model_invoke_mode": "SM_ENDPOINT",
                "image_extension": "fake",
            }
        )
        mock_sensor_model = None
        mock_elevation_model = None
        work_queue, tile_worker_list = setup_tile_workers(mock_region_request, mock_sensor_model, mock_elevation_model)

        # Assert that the correct number of tile workers are created
        assert len(tile_worker_list) == mock_num_tile_workers
        # Verify that each worker's start method is called
        for worker in tile_worker_list:
            # Assert that the mock function was called exactly 4 times
            self.assertEqual(worker.start.call_count, 4)

    @patch("aws.osml.model_runner.tile_worker.tile_worker_utils.FeatureTable", autospec=True)
    @patch("aws.osml.model_runner.tile_worker.tile_worker_utils.TileWorker", autospec=True)
    @patch("aws.osml.model_runner.tile_worker.tile_worker_utils.ServiceConfig", autospec=True)
    def test_setup_tile_workers_exception(self, mock_service_config, mock_tile_worker, mock_feature_table):
        """
        Test that an exception during tile worker setup raises a SetupTileWorkersException.
        """
        from aws.osml.model_runner.api import RegionRequest
        from aws.osml.model_runner.tile_worker.exceptions import SetupTileWorkersException
        from aws.osml.model_runner.tile_worker.tile_worker_utils import setup_tile_workers

        mock_tile_worker.start = Mock()
        mock_num_tile_workers = 4
        mock_service_config.workers = mock_num_tile_workers
        mock_feature_table.side_effect = Exception("Mock processing exception")
        mock_region_request = RegionRequest(
            {
                "tile_size": (10, 10),
                "tile_overlap": (1, 1),
                "tile_format": "NITF",
                "image_id": "1",
                "image_url": "/mock/path",
                "region_bounds": ((0, 0), (50, 50)),
                "model_invoke_mode": "SM_ENDPOINT",
                "image_extension": "fake",
            }
        )
        mock_sensor_model = None
        mock_elevation_model = None
        with self.assertRaises(SetupTileWorkersException):
            # Attempt to set up workers should fail and raise the specified exception
            setup_tile_workers(mock_region_request, mock_sensor_model, mock_elevation_model)

    @patch("aws.osml.model_runner.tile_worker.tile_worker_utils.FeatureDetectorFactory", autospec=True)
    @patch("aws.osml.model_runner.tile_worker.tile_worker_utils.TileWorker", autospec=True)
    @patch("aws.osml.model_runner.tile_worker.tile_worker_utils.ServiceConfig", autospec=True)
    def test_setup_tile_workers_with_endpoint_parameters(
        self, mock_service_config, mock_tile_worker, mock_feature_detector_factory
    ):
        """
        Test that when model_endpoint_parameters are provided in a region request the feature detector is called with
         the endpoint parameters.
        """
        from aws.osml.model_runner.api import RegionRequest
        from aws.osml.model_runner.tile_worker.tile_worker_utils import setup_tile_workers

        start_mock = Mock()
        mock_tile_worker.return_value.start = start_mock
        mock_num_tile_workers = 1
        mock_service_config.workers = mock_num_tile_workers

        # Create a mock feature detector
        mock_feature_detector = Mock()
        mock_feature_detector.endpoint = "test-model-endpoint"
        mock_feature_detector.find_features.return_value = {"features": []}
        mock_feature_detector_factory.return_value.build.return_value = mock_feature_detector

        mock_region_request = RegionRequest(
            {
                "tile_size": (10, 10),
                "tile_overlap": (1, 1),
                "tile_format": "NITF",
                "image_id": "1",
                "image_url": "/mock/path",
                "region_bounds": ((0, 0), (50, 50)),
                "model_invoke_mode": "SM_ENDPOINT",
                "image_extension": "fake",
                "model_name": "test-model",
                "model_endpoint_parameters": {"TargetVariant": "model-variant-1"},
            }
        )

        work_queue, tile_worker_list = setup_tile_workers(mock_region_request)

        # Assert that FeatureDetectorFactory was initialized with the correct parameters
        mock_feature_detector_factory.assert_called_once_with(
            endpoint="test-model",
            endpoint_mode="SM_ENDPOINT",
            endpoint_parameters={"TargetVariant": "model-variant-1"},
            assumed_credentials=None,
        )

        # Additional assertions to ensure the rest of the setup process worked as expected
        assert len(tile_worker_list) == mock_num_tile_workers
        assert start_mock.call_count == mock_num_tile_workers

    @patch("aws.osml.model_runner.tile_worker.tile_worker_utils.FeatureTable", autospec=True)
    @patch("aws.osml.model_runner.tile_worker.tile_worker_utils.RegionRequestTable", autospec=True)
    @patch("aws.osml.model_runner.tile_worker.tile_worker_utils.Geolocator", autospec=True)
    @patch("aws.osml.model_runner.tile_worker.tile_worker_utils.FeatureDetectorFactory", autospec=True)
    @patch("aws.osml.model_runner.tile_worker.tile_worker_utils.TileWorker", autospec=True)
    @patch("aws.osml.model_runner.tile_worker.tile_worker_utils.ServiceConfig", autospec=True)
    @patch("aws.osml.model_runner.tile_worker.tile_worker_utils.get_credentials_for_assumed_role", autospec=True)
    def test_setup_tile_workers_with_invocation_role_and_geolocator(
        self,
        mock_get_credentials,
        mock_service_config,
        mock_tile_worker,
        mock_feature_detector_factory,
        mock_geolocator,
        mock_region_request_table,
        mock_feature_table,
    ):
        """
        Test that model invocation credentials and geolocator are wired when provided.
        """
        from aws.osml.model_runner.api import RegionRequest
        from aws.osml.model_runner.tile_worker.tile_worker_utils import setup_tile_workers

        mock_service_config.workers = 1
        mock_service_config.feature_table = "feature-table"
        mock_service_config.region_request_table = "region-table"
        mock_get_credentials.return_value = {"AccessKeyId": "key", "SecretAccessKey": "secret"}
        mock_feature_detector = Mock()
        mock_feature_detector_factory.return_value.build.return_value = mock_feature_detector
        mock_tile_worker.return_value.start = Mock()

        mock_region_request = RegionRequest(
            {
                "tile_size": (10, 10),
                "tile_overlap": (1, 1),
                "tile_format": "NITF",
                "image_id": "1",
                "image_url": "/mock/path",
                "region_bounds": ((0, 0), (50, 50)),
                "model_invoke_mode": "SM_ENDPOINT",
                "image_extension": "fake",
                "model_invocation_role": "arn:aws:iam::123456789012:role/test-role",
            }
        )

        sensor_model = Mock()
        elevation_model = Mock()
        work_queue, tile_worker_list = setup_tile_workers(mock_region_request, sensor_model, elevation_model)

        mock_get_credentials.assert_called_once_with(mock_region_request.model_invocation_role)
        mock_feature_detector_factory.assert_called_once_with(
            endpoint=mock_region_request.model_name,
            endpoint_mode=mock_region_request.model_invoke_mode,
            endpoint_parameters=mock_region_request.model_endpoint_parameters,
            assumed_credentials=mock_get_credentials.return_value,
        )
        mock_geolocator.assert_called_once()
        assert work_queue is not None
        assert len(tile_worker_list) == 1

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
        from aws.osml.model_runner.tile_worker.tile_worker_utils import process_tiles, setup_tile_workers

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
        total_tile_count, tile_error_count = process_tiles(
            tiling_strategy=VariableTileTilingStrategy(),
            region_request_item=region_request_item,
            tile_queue=work_queue,
            tile_workers=tile_worker_list,
            raster_dataset=ds,
            sensor_model=sensor_model,
        )

        # Verify expected results
        assert total_tile_count == 25
        assert tile_error_count == 0

    @patch("aws.osml.model_runner.tile_worker.tile_worker_utils.GDALTileFactory", autospec=True)
    @patch("aws.osml.model_runner.tile_worker.tile_worker_utils.GDALConfigEnv", autospec=True)
    @patch("aws.osml.model_runner.tile_worker.tile_worker_utils.get_credentials_for_assumed_role", autospec=True)
    @patch("aws.osml.model_runner.tile_worker.tile_worker_utils._create_tile", autospec=True)
    def test_process_tiles_skips_succeeded_tiles_and_uses_role(
        self, mock_create_tile, mock_get_credentials, mock_gdal_config_env, mock_gdal_tile_factory
    ):
        """
        Test that succeeded tiles are filtered and image read credentials are used.
        """
        from types import SimpleNamespace

        from aws.osml.model_runner.tile_worker.tile_worker_utils import process_tiles

        mock_get_credentials.return_value = {"AccessKeyId": "key", "SecretAccessKey": "secret"}
        mock_gdal_config_env.return_value.with_aws_credentials.return_value.__enter__ = Mock()
        mock_gdal_config_env.return_value.with_aws_credentials.return_value.__exit__ = Mock(return_value=False)

        tiling_strategy = Mock()
        tiling_strategy.compute_tiles.return_value = [
            ((0, 0), (10, 10)),
            ((10, 0), (10, 10)),
            ((20, 0), (10, 10)),
        ]
        mock_create_tile.side_effect = [None, "/tmp/tile.ntf"]

        region_request_item = SimpleNamespace(
            region_bounds=((0, 0), (20, 20)),
            tile_size=(10, 10),
            tile_overlap=(0, 0),
            succeeded_tiles=[[[0, 0], [10, 10]]],
            image_read_role="arn:aws:iam::123456789012:role/read-role",
            tile_format="NITF",
            tile_compression="NONE",
            image_id="image-1",
            job_id="job-1",
            region_id="region-1",
        )

        tile_queue = Mock()
        tile_workers = [Mock(failed_tile_count=0), Mock(failed_tile_count=1)]

        total_tile_count, tile_error_count = process_tiles(
            tiling_strategy=tiling_strategy,
            region_request_item=region_request_item,
            tile_queue=tile_queue,
            tile_workers=tile_workers,
            raster_dataset=Mock(),
            sensor_model=None,
        )

        assert total_tile_count == 2
        assert tile_error_count == 1
        mock_get_credentials.assert_called_once_with(region_request_item.image_read_role)
        assert tile_queue.put.call_count == 3

    @patch("aws.osml.model_runner.tile_worker.tile_worker_utils.GDALTileFactory", autospec=True)
    @patch("aws.osml.model_runner.tile_worker.tile_worker_utils.GDALConfigEnv", autospec=True)
    def test_process_tiles_exception(self, mock_gdal_config_env, mock_gdal_tile_factory):
        """
        Test that process_tiles wraps exceptions in ProcessTilesException.
        """
        from types import SimpleNamespace

        from aws.osml.model_runner.tile_worker.exceptions import ProcessTilesException
        from aws.osml.model_runner.tile_worker.tile_worker_utils import process_tiles

        mock_gdal_tile_factory.side_effect = RuntimeError("boom")
        mock_gdal_config_env.return_value.with_aws_credentials.return_value.__enter__ = Mock()
        mock_gdal_config_env.return_value.with_aws_credentials.return_value.__exit__ = Mock(return_value=False)

        tiling_strategy = Mock()
        tiling_strategy.compute_tiles.return_value = [((0, 0), (10, 10))]

        region_request_item = SimpleNamespace(
            region_bounds=((0, 0), (10, 10)),
            tile_size=(10, 10),
            tile_overlap=(0, 0),
            succeeded_tiles=None,
            image_read_role=None,
            tile_format="NITF",
            tile_compression="NONE",
            image_id="image-1",
            job_id="job-1",
            region_id="region-1",
        )

        with self.assertRaises(ProcessTilesException):
            process_tiles(
                tiling_strategy=tiling_strategy,
                region_request_item=region_request_item,
                tile_queue=Mock(),
                tile_workers=[Mock(failed_tile_count=0)],
                raster_dataset=Mock(),
                sensor_model=None,
            )

    def test_next_greater_multiple(self):
        """
        Test finding the next greater multiple of a number.
        """
        assert 16 == self.next_greater_multiple(1, 16)
        assert 16 == self.next_greater_multiple(15, 16)
        assert 16 == self.next_greater_multiple(16, 16)
        assert 32 == self.next_greater_multiple(17, 16)
        assert 48 == self.next_greater_multiple(42, 16)
        assert 64 == self.next_greater_multiple(50, 16)
        assert 528 == self.next_greater_multiple(513, 16)

    def test_next_greater_power_of_two(self):
        """
        Test finding the next greater power of two for a given number.
        """
        assert 1 == self.next_greater_power_of_two(1)
        assert 2 == self.next_greater_power_of_two(2)
        assert 4 == self.next_greater_power_of_two(3)
        assert 8 == self.next_greater_power_of_two(8)
        assert 64 == self.next_greater_power_of_two(42)
        assert 128 == self.next_greater_power_of_two(100)
        assert 256 == self.next_greater_power_of_two(255)
        assert 512 == self.next_greater_power_of_two(400)

    def test_sizeof_fmt(self):
        """
        Test the human-readable size formatting function.
        """
        from aws.osml.model_runner.tile_worker.tile_worker_utils import sizeof_fmt

        assert sizeof_fmt(250) == "250.0B"
        assert sizeof_fmt(1024**3) == "1.0GiB"
        assert sizeof_fmt(1024**8) == "1.0YiB"

    def test_create_tile_success_with_metrics(self):
        """
        Test creating a tile successfully and recording metrics.
        """
        from aws_embedded_metrics import MetricsLogger

        from aws.osml.model_runner.tile_worker.tile_worker_utils import _create_tile

        class FakeDriver:
            ShortName = "NITF"

        class FakeDataset:
            def GetDriver(self):
                return FakeDriver()

        gdal_tile_factory = Mock()
        gdal_tile_factory.raster_dataset = FakeDataset()
        gdal_tile_factory.create_encoded_tile.return_value = b"data"

        metrics = MetricsLogger(resolve_environment=Mock())
        metrics.set_dimensions = Mock()
        metrics.put_dimensions = Mock()
        metrics.put_metric = Mock()
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_image_path = Path(tmp_dir, "tile.ntf")
            tile_path = _create_tile.__wrapped__(  # type: ignore[attr-defined]
                gdal_tile_factory, ((0, 0), (10, 10)), tmp_image_path, metrics
            )

        assert tile_path is not None
        metrics.set_dimensions.assert_called_once()
        metrics.put_dimensions.assert_called_once()
        metrics.put_metric.assert_called()

    def test_create_tile_handles_none_data(self):
        """
        Test handling of missing tile data.
        """
        from aws_embedded_metrics import MetricsLogger

        from aws.osml.model_runner.tile_worker.tile_worker_utils import _create_tile

        class FakeDriver:
            ShortName = "NITF"

        class FakeDataset:
            def GetDriver(self):
                return FakeDriver()

        gdal_tile_factory = Mock()
        gdal_tile_factory.raster_dataset = FakeDataset()
        gdal_tile_factory.create_encoded_tile.return_value = None

        metrics = MetricsLogger(resolve_environment=Mock())
        metrics.set_dimensions = Mock()
        metrics.put_dimensions = Mock()
        metrics.put_metric = Mock()
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_image_path = Path(tmp_dir, "tile.ntf")
            tile_path = _create_tile.__wrapped__(  # type: ignore[attr-defined]
                gdal_tile_factory, ((0, 0), (10, 10)), tmp_image_path, metrics
            )

        assert tile_path is None
        metrics.put_metric.assert_called()

    @patch("pathlib.Path.is_file", return_value=False)
    def test_create_tile_missing_file(self, mock_is_file):
        """
        Test handling when tile file is not created.
        """
        from aws_embedded_metrics import MetricsLogger

        from aws.osml.model_runner.tile_worker.tile_worker_utils import _create_tile

        class FakeDriver:
            ShortName = "NITF"

        class FakeDataset:
            def GetDriver(self):
                return FakeDriver()

        gdal_tile_factory = Mock()
        gdal_tile_factory.raster_dataset = FakeDataset()
        gdal_tile_factory.create_encoded_tile.return_value = b"data"

        metrics = MetricsLogger(resolve_environment=Mock())
        metrics.set_dimensions = Mock()
        metrics.put_dimensions = Mock()
        metrics.put_metric = Mock()
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_image_path = Path(tmp_dir, "tile.ntf")
            tile_path = _create_tile.__wrapped__(  # type: ignore[attr-defined]
                gdal_tile_factory, ((0, 0), (10, 10)), tmp_image_path, metrics
            )

        assert tile_path is None
        mock_is_file.assert_called()

    @patch("aws.osml.model_runner.tile_worker.tile_worker_utils.FeatureSelector", autospec=True)
    @patch("aws.osml.model_runner.tile_worker.tile_worker_utils.FeatureDistillationDeserializer", autospec=True)
    def test_select_features(self, mock_distillation_deserializer, mock_feature_selector):
        """
        Test that select_features uses the deserializer and tiling strategy.
        """
        from aws.osml.model_runner.tile_worker.tile_worker_utils import select_features

        mock_distillation_deserializer.return_value.deserialize.return_value = "option"
        feature_selector = Mock()
        mock_feature_selector.return_value = feature_selector
        tiling_strategy = Mock()
        tiling_strategy.cleanup_duplicate_features.return_value = ["feature"]

        features = [Mock()]
        processing_bounds = ((0, 0), (10, 10))
        result = select_features(
            feature_distillation_option='{"type": "nms"}',
            features=features,
            processing_bounds=processing_bounds,
            region_size="(10, 10)",
            tile_size="(5, 5)",
            tile_overlap="(1, 1)",
            tiling_strategy=tiling_strategy,
        )

        tiling_strategy.cleanup_duplicate_features.assert_called_once_with(
            processing_bounds,
            (10, 10),
            (5, 5),
            (1, 1),
            features,
            feature_selector,
        )
        assert result == ["feature"]

    @staticmethod
    def get_dataset_and_camera():
        """
        Utility method to load a dataset and associated sensor model from a test file.
        """
        from aws.osml.gdal.gdal_utils import load_gdal_dataset

        return load_gdal_dataset("./test/data/GeogToWGS84GeoKey5.tif")

    @staticmethod
    def next_greater_multiple(n: int, m: int) -> int:
        """
        Return the minimum value that is greater than or equal to n that is evenly divisible by m.
        """
        if n % m == 0:
            return n
        return n + (m - n % m)

    @staticmethod
    def next_greater_power_of_two(n: int) -> int:
        """
        Returns the smallest power of 2 that is greater than or equal to the input parameter.
        """
        count = 0
        if n and not (n & (n - 1)):
            return n
        while n != 0:
            n >>= 1
            count += 1
        return 1 << count


if __name__ == "__main__":
    main()
