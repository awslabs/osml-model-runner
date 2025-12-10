#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

from unittest import TestCase, main
from unittest.mock import MagicMock, patch

import pytest
import shapely.geometry

from aws.osml.model_runner.common import ImageDimensions
from aws.osml.model_runner.exceptions import LoadImageException
from aws.osml.model_runner.tile_worker import TilingStrategy, ToolkitRegionCalculator


class TestToolkitRegionCalculator(TestCase):
    def setUp(self):
        """Set up test fixtures."""
        self.mock_tiling_strategy = MagicMock(spec=TilingStrategy)
        self.region_size: ImageDimensions = (10240, 10240)
        self.calculator = ToolkitRegionCalculator(tiling_strategy=self.mock_tiling_strategy, region_size=self.region_size)

    @patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.load_gdal_dataset")
    @patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.get_image_path")
    @patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.calculate_processing_bounds")
    @patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.GDALConfigEnv")
    def test_small_image_returns_one_region(self, mock_gdal_config, mock_calc_bounds, mock_get_path, mock_load_dataset):
        """
        Test that a small image (1024×1024) returns 1 region.
        """
        # Mock GDAL dataset
        mock_dataset = MagicMock()
        mock_dataset.RasterXSize = 1024
        mock_dataset.RasterYSize = 1024
        mock_sensor_model = MagicMock()

        # Set up mocks
        mock_get_path.return_value = "test/path/image.tif"
        mock_load_dataset.return_value = (mock_dataset, mock_sensor_model)
        mock_calc_bounds.return_value = ((0, 0), (1024, 1024))

        # Mock tiling strategy to return 1 region for small image
        expected_regions = [((0, 0), (1024, 1024))]
        self.mock_tiling_strategy.compute_regions.return_value = expected_regions

        # Mock GDAL config context manager
        mock_context = MagicMock()
        mock_gdal_config.return_value = mock_context
        mock_context.with_aws_credentials.return_value.__enter__.return_value = None

        # Calculate regions
        tile_size: ImageDimensions = (1024, 1024)
        tile_overlap: ImageDimensions = (50, 50)
        regions = self.calculator.calculate_regions(
            image_url="s3://bucket/image.tif", tile_size=tile_size, tile_overlap=tile_overlap
        )

        # Verify
        assert len(regions) == 1
        assert regions[0] == ((0, 0), (1024, 1024))
        self.mock_tiling_strategy.compute_regions.assert_called_once()

    @patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.load_gdal_dataset")
    @patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.get_image_path")
    @patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.calculate_processing_bounds")
    @patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.GDALConfigEnv")
    def test_large_image_returns_multiple_regions(
        self, mock_gdal_config, mock_calc_bounds, mock_get_path, mock_load_dataset
    ):
        """
        Test that a large image (20480×20480) returns 4 regions.
        """
        # Mock GDAL dataset
        mock_dataset = MagicMock()
        mock_dataset.RasterXSize = 20480
        mock_dataset.RasterYSize = 20480
        mock_sensor_model = MagicMock()

        # Set up mocks
        mock_get_path.return_value = "test/path/large_image.tif"
        mock_load_dataset.return_value = (mock_dataset, mock_sensor_model)
        mock_calc_bounds.return_value = ((0, 0), (20480, 20480))

        # Mock tiling strategy to return 4 regions for large image
        expected_regions = [
            ((0, 0), (10240, 10240)),
            ((0, 10240), (10240, 10240)),
            ((10240, 0), (10240, 10240)),
            ((10240, 10240), (10240, 10240)),
        ]
        self.mock_tiling_strategy.compute_regions.return_value = expected_regions

        # Mock GDAL config context manager
        mock_context = MagicMock()
        mock_gdal_config.return_value = mock_context
        mock_context.with_aws_credentials.return_value.__enter__.return_value = None

        # Calculate regions
        tile_size: ImageDimensions = (2048, 2048)
        tile_overlap: ImageDimensions = (100, 100)
        regions = self.calculator.calculate_regions(
            image_url="s3://bucket/large_image.tif", tile_size=tile_size, tile_overlap=tile_overlap
        )

        # Verify
        assert len(regions) == 4
        self.mock_tiling_strategy.compute_regions.assert_called_once()

    @patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.load_gdal_dataset")
    @patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.get_image_path")
    @patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.calculate_processing_bounds")
    @patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.GDALConfigEnv")
    def test_image_with_roi_returns_fewer_regions(
        self, mock_gdal_config, mock_calc_bounds, mock_get_path, mock_load_dataset
    ):
        """
        Test that an image with ROI returns fewer regions than without ROI.
        """
        # Mock GDAL dataset
        mock_dataset = MagicMock()
        mock_dataset.RasterXSize = 20480
        mock_dataset.RasterYSize = 20480
        mock_sensor_model = MagicMock()

        # Set up mocks
        mock_get_path.return_value = "test/path/image.tif"
        mock_load_dataset.return_value = (mock_dataset, mock_sensor_model)

        # ROI restricts processing to smaller area
        mock_calc_bounds.return_value = ((0, 0), (10240, 10240))

        # Mock tiling strategy to return 1 region for ROI-restricted area
        expected_regions = [((0, 0), (10240, 10240))]
        self.mock_tiling_strategy.compute_regions.return_value = expected_regions

        # Mock GDAL config context manager
        mock_context = MagicMock()
        mock_gdal_config.return_value = mock_context
        mock_context.with_aws_credentials.return_value.__enter__.return_value = None

        # Create ROI geometry
        roi = shapely.geometry.box(0, 0, 10240, 10240)

        # Calculate regions with ROI
        tile_size: ImageDimensions = (2048, 2048)
        tile_overlap: ImageDimensions = (100, 100)
        regions = self.calculator.calculate_regions(
            image_url="s3://bucket/image.tif", tile_size=tile_size, tile_overlap=tile_overlap, roi=roi
        )

        # Verify fewer regions due to ROI
        assert len(regions) == 1
        mock_calc_bounds.assert_called_once_with(mock_dataset, roi, mock_sensor_model)

    @patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.load_gdal_dataset")
    @patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.get_image_path")
    @patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.GDALConfigEnv")
    def test_inaccessible_image_raises_exception(self, mock_gdal_config, mock_get_path, mock_load_dataset):
        """
        Test that an inaccessible image raises LoadImageException.
        """
        # Set up mocks to simulate image access failure
        mock_get_path.return_value = "test/path/missing_image.tif"
        mock_load_dataset.side_effect = Exception("Failed to load image")

        # Mock GDAL config context manager
        mock_context = MagicMock()
        mock_gdal_config.return_value = mock_context
        mock_context.with_aws_credentials.return_value.__enter__.return_value = None

        # Attempt to calculate regions
        tile_size: ImageDimensions = (2048, 2048)
        tile_overlap: ImageDimensions = (100, 100)

        with pytest.raises(LoadImageException):
            self.calculator.calculate_regions(
                image_url="s3://bucket/missing_image.tif", tile_size=tile_size, tile_overlap=tile_overlap
            )

    @patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.load_gdal_dataset")
    @patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.get_image_path")
    @patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.calculate_processing_bounds")
    @patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.get_credentials_for_assumed_role")
    @patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.GDALConfigEnv")
    def test_iam_role_assumption_works(
        self, mock_gdal_config, mock_get_creds, mock_calc_bounds, mock_get_path, mock_load_dataset
    ):
        """
        Test that IAM role assumption works correctly.
        """
        # Mock GDAL dataset
        mock_dataset = MagicMock()
        mock_dataset.RasterXSize = 1024
        mock_dataset.RasterYSize = 1024
        mock_sensor_model = MagicMock()

        # Set up mocks
        mock_get_path.return_value = "test/path/image.tif"
        mock_load_dataset.return_value = (mock_dataset, mock_sensor_model)
        mock_calc_bounds.return_value = ((0, 0), (1024, 1024))

        # Mock credentials
        mock_credentials = {"AccessKeyId": "test-key", "SecretAccessKey": "test-secret", "SessionToken": "test-token"}
        mock_get_creds.return_value = mock_credentials

        # Mock tiling strategy
        expected_regions = [((0, 0), (1024, 1024))]
        self.mock_tiling_strategy.compute_regions.return_value = expected_regions

        # Mock GDAL config context manager
        mock_context = MagicMock()
        mock_gdal_config.return_value = mock_context
        mock_context.with_aws_credentials.return_value.__enter__.return_value = None

        # Calculate regions with IAM role
        tile_size: ImageDimensions = (1024, 1024)
        tile_overlap: ImageDimensions = (50, 50)
        image_read_role = "arn:aws:iam::123456789012:role/TestRole"

        regions = self.calculator.calculate_regions(
            image_url="s3://bucket/image.tif",
            tile_size=tile_size,
            tile_overlap=tile_overlap,
            image_read_role=image_read_role,
        )

        # Verify credentials were retrieved
        mock_get_creds.assert_called_once_with(image_read_role)

        # Verify GDAL config was set up with credentials
        mock_context.with_aws_credentials.assert_called_once_with(mock_credentials)

        # Verify regions were calculated
        assert len(regions) == 1

    @patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.load_gdal_dataset")
    @patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.get_image_path")
    @patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.calculate_processing_bounds")
    @patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.GDALConfigEnv")
    def test_gdal_configuration_setup(self, mock_gdal_config, mock_calc_bounds, mock_get_path, mock_load_dataset):
        """
        Test that GDAL configuration is set up properly.
        """
        # Mock GDAL dataset
        mock_dataset = MagicMock()
        mock_dataset.RasterXSize = 1024
        mock_dataset.RasterYSize = 1024
        mock_sensor_model = MagicMock()

        # Set up mocks
        mock_get_path.return_value = "test/path/image.tif"
        mock_load_dataset.return_value = (mock_dataset, mock_sensor_model)
        mock_calc_bounds.return_value = ((0, 0), (1024, 1024))

        # Mock tiling strategy
        expected_regions = [((0, 0), (1024, 1024))]
        self.mock_tiling_strategy.compute_regions.return_value = expected_regions

        # Mock GDAL config context manager
        mock_context = MagicMock()
        mock_gdal_config.return_value = mock_context
        mock_context.with_aws_credentials.return_value.__enter__.return_value = None

        # Calculate regions
        tile_size: ImageDimensions = (1024, 1024)
        tile_overlap: ImageDimensions = (50, 50)

        regions = self.calculator.calculate_regions(
            image_url="s3://bucket/image.tif", tile_size=tile_size, tile_overlap=tile_overlap
        )

        # Verify GDAL config was instantiated and used
        mock_gdal_config.assert_called_once()
        mock_context.with_aws_credentials.assert_called_once()

        # Verify regions were calculated
        assert len(regions) == 1


if __name__ == "__main__":
    main()
