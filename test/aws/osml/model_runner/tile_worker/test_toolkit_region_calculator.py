#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

import pytest
import shapely.geometry

from aws.osml.model_runner.common import ImageDimensions
from aws.osml.model_runner.exceptions import LoadImageException
from aws.osml.model_runner.tile_worker import TilingStrategy, ToolkitRegionCalculator


@pytest.fixture
def toolkit_region_calculator_setup(mocker):
    """Set up test fixtures for ToolkitRegionCalculator tests."""
    mock_tiling_strategy = mocker.MagicMock(spec=TilingStrategy)
    region_size: ImageDimensions = (10240, 10240)
    calculator = ToolkitRegionCalculator(tiling_strategy=mock_tiling_strategy, region_size=region_size)
    return calculator, mock_tiling_strategy


def test_small_image_returns_one_region(toolkit_region_calculator_setup, mocker):
    """
    Test that a small image (1024×1024) returns 1 region.
    """
    calculator, mock_tiling_strategy = toolkit_region_calculator_setup

    # Mock GDAL dataset
    mock_dataset = mocker.MagicMock()
    mock_dataset.RasterXSize = 1024
    mock_dataset.RasterYSize = 1024
    mock_sensor_model = mocker.MagicMock()

    # Set up mocks
    mock_get_path = mocker.patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.get_image_path")
    mock_get_path.return_value = "test/path/image.tif"

    mock_load_dataset = mocker.patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.load_gdal_dataset")
    mock_load_dataset.return_value = (mock_dataset, mock_sensor_model)

    mock_calc_bounds = mocker.patch(
        "aws.osml.model_runner.tile_worker.toolkit_region_calculator.calculate_processing_bounds"
    )
    mock_calc_bounds.return_value = ((0, 0), (1024, 1024))

    # Mock tiling strategy to return 1 region for small image
    expected_regions = [((0, 0), (1024, 1024))]
    mock_tiling_strategy.compute_regions.return_value = expected_regions

    # Mock GDAL config context manager
    mock_gdal_config = mocker.patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.GDALConfigEnv")
    mock_context = mocker.MagicMock()
    mock_gdal_config.return_value = mock_context
    mock_context.with_aws_credentials.return_value.__enter__.return_value = None

    # Calculate regions
    tile_size: ImageDimensions = (1024, 1024)
    tile_overlap: ImageDimensions = (50, 50)
    regions = calculator.calculate_regions(image_url="s3://bucket/image.tif", tile_size=tile_size, tile_overlap=tile_overlap)

    # Verify
    assert len(regions) == 1
    assert regions[0] == ((0, 0), (1024, 1024))
    mock_tiling_strategy.compute_regions.assert_called_once()


def test_large_image_returns_multiple_regions(toolkit_region_calculator_setup, mocker):
    """
    Test that a large image (20480×20480) returns 4 regions.
    """
    calculator, mock_tiling_strategy = toolkit_region_calculator_setup

    # Mock GDAL dataset
    mock_dataset = mocker.MagicMock()
    mock_dataset.RasterXSize = 20480
    mock_dataset.RasterYSize = 20480
    mock_sensor_model = mocker.MagicMock()

    # Set up mocks
    mock_get_path = mocker.patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.get_image_path")
    mock_get_path.return_value = "test/path/large_image.tif"

    mock_load_dataset = mocker.patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.load_gdal_dataset")
    mock_load_dataset.return_value = (mock_dataset, mock_sensor_model)

    mock_calc_bounds = mocker.patch(
        "aws.osml.model_runner.tile_worker.toolkit_region_calculator.calculate_processing_bounds"
    )
    mock_calc_bounds.return_value = ((0, 0), (20480, 20480))

    # Mock tiling strategy to return 4 regions for large image
    expected_regions = [
        ((0, 0), (10240, 10240)),
        ((0, 10240), (10240, 10240)),
        ((10240, 0), (10240, 10240)),
        ((10240, 10240), (10240, 10240)),
    ]
    mock_tiling_strategy.compute_regions.return_value = expected_regions

    # Mock GDAL config context manager
    mock_gdal_config = mocker.patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.GDALConfigEnv")
    mock_context = mocker.MagicMock()
    mock_gdal_config.return_value = mock_context
    mock_context.with_aws_credentials.return_value.__enter__.return_value = None

    # Calculate regions
    tile_size: ImageDimensions = (2048, 2048)
    tile_overlap: ImageDimensions = (100, 100)
    regions = calculator.calculate_regions(
        image_url="s3://bucket/large_image.tif", tile_size=tile_size, tile_overlap=tile_overlap
    )

    # Verify
    assert len(regions) == 4
    mock_tiling_strategy.compute_regions.assert_called_once()


def test_image_with_roi_returns_fewer_regions(toolkit_region_calculator_setup, mocker):
    """
    Test that an image with ROI returns fewer regions than without ROI.
    """
    calculator, mock_tiling_strategy = toolkit_region_calculator_setup

    # Mock GDAL dataset
    mock_dataset = mocker.MagicMock()
    mock_dataset.RasterXSize = 20480
    mock_dataset.RasterYSize = 20480
    mock_sensor_model = mocker.MagicMock()

    # Set up mocks
    mock_get_path = mocker.patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.get_image_path")
    mock_get_path.return_value = "test/path/image.tif"

    mock_load_dataset = mocker.patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.load_gdal_dataset")
    mock_load_dataset.return_value = (mock_dataset, mock_sensor_model)

    mock_calc_bounds = mocker.patch(
        "aws.osml.model_runner.tile_worker.toolkit_region_calculator.calculate_processing_bounds"
    )
    # ROI restricts processing to smaller area
    mock_calc_bounds.return_value = ((0, 0), (10240, 10240))

    # Mock tiling strategy to return 1 region for ROI-restricted area
    expected_regions = [((0, 0), (10240, 10240))]
    mock_tiling_strategy.compute_regions.return_value = expected_regions

    # Mock GDAL config context manager
    mock_gdal_config = mocker.patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.GDALConfigEnv")
    mock_context = mocker.MagicMock()
    mock_gdal_config.return_value = mock_context
    mock_context.with_aws_credentials.return_value.__enter__.return_value = None

    # Create ROI geometry
    roi = shapely.geometry.box(0, 0, 10240, 10240)

    # Calculate regions with ROI
    tile_size: ImageDimensions = (2048, 2048)
    tile_overlap: ImageDimensions = (100, 100)
    regions = calculator.calculate_regions(
        image_url="s3://bucket/image.tif", tile_size=tile_size, tile_overlap=tile_overlap, roi=roi
    )

    # Verify fewer regions due to ROI
    assert len(regions) == 1
    mock_calc_bounds.assert_called_once_with(mock_dataset, roi, mock_sensor_model)


def test_inaccessible_image_raises_exception(toolkit_region_calculator_setup, mocker):
    """
    Test that an inaccessible image raises LoadImageException.
    """
    calculator, _ = toolkit_region_calculator_setup

    # Set up mocks to simulate image access failure
    mock_get_path = mocker.patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.get_image_path")
    mock_get_path.return_value = "test/path/missing_image.tif"

    mock_load_dataset = mocker.patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.load_gdal_dataset")
    mock_load_dataset.side_effect = Exception("Failed to load image")

    # Mock GDAL config context manager
    mock_gdal_config = mocker.patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.GDALConfigEnv")
    mock_context = mocker.MagicMock()
    mock_gdal_config.return_value = mock_context
    mock_context.with_aws_credentials.return_value.__enter__.return_value = None

    # Attempt to calculate regions
    tile_size: ImageDimensions = (2048, 2048)
    tile_overlap: ImageDimensions = (100, 100)

    with pytest.raises(LoadImageException):
        calculator.calculate_regions(
            image_url="s3://bucket/missing_image.tif", tile_size=tile_size, tile_overlap=tile_overlap
        )


def test_iam_role_assumption_works(toolkit_region_calculator_setup, mocker):
    """
    Test that IAM role assumption works correctly.
    """
    calculator, mock_tiling_strategy = toolkit_region_calculator_setup

    # Mock GDAL dataset
    mock_dataset = mocker.MagicMock()
    mock_dataset.RasterXSize = 1024
    mock_dataset.RasterYSize = 1024
    mock_sensor_model = mocker.MagicMock()

    # Set up mocks
    mock_get_path = mocker.patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.get_image_path")
    mock_get_path.return_value = "test/path/image.tif"

    mock_load_dataset = mocker.patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.load_gdal_dataset")
    mock_load_dataset.return_value = (mock_dataset, mock_sensor_model)

    mock_calc_bounds = mocker.patch(
        "aws.osml.model_runner.tile_worker.toolkit_region_calculator.calculate_processing_bounds"
    )
    mock_calc_bounds.return_value = ((0, 0), (1024, 1024))

    # Mock credentials
    mock_get_creds = mocker.patch(
        "aws.osml.model_runner.tile_worker.toolkit_region_calculator.get_credentials_for_assumed_role"
    )
    mock_credentials = {"AccessKeyId": "test-key", "SecretAccessKey": "test-secret", "SessionToken": "test-token"}
    mock_get_creds.return_value = mock_credentials

    # Mock tiling strategy
    expected_regions = [((0, 0), (1024, 1024))]
    mock_tiling_strategy.compute_regions.return_value = expected_regions

    # Mock GDAL config context manager
    mock_gdal_config = mocker.patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.GDALConfigEnv")
    mock_context = mocker.MagicMock()
    mock_gdal_config.return_value = mock_context
    mock_context.with_aws_credentials.return_value.__enter__.return_value = None

    # Calculate regions with IAM role
    tile_size: ImageDimensions = (1024, 1024)
    tile_overlap: ImageDimensions = (50, 50)
    image_read_role = "arn:aws:iam::123456789012:role/TestRole"

    regions = calculator.calculate_regions(
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


def test_gdal_configuration_setup(toolkit_region_calculator_setup, mocker):
    """
    Test that GDAL configuration is set up properly.
    """
    calculator, mock_tiling_strategy = toolkit_region_calculator_setup

    # Mock GDAL dataset
    mock_dataset = mocker.MagicMock()
    mock_dataset.RasterXSize = 1024
    mock_dataset.RasterYSize = 1024
    mock_sensor_model = mocker.MagicMock()

    # Set up mocks
    mock_get_path = mocker.patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.get_image_path")
    mock_get_path.return_value = "test/path/image.tif"

    mock_load_dataset = mocker.patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.load_gdal_dataset")
    mock_load_dataset.return_value = (mock_dataset, mock_sensor_model)

    mock_calc_bounds = mocker.patch(
        "aws.osml.model_runner.tile_worker.toolkit_region_calculator.calculate_processing_bounds"
    )
    mock_calc_bounds.return_value = ((0, 0), (1024, 1024))

    # Mock tiling strategy
    expected_regions = [((0, 0), (1024, 1024))]
    mock_tiling_strategy.compute_regions.return_value = expected_regions

    # Mock GDAL config context manager
    mock_gdal_config = mocker.patch("aws.osml.model_runner.tile_worker.toolkit_region_calculator.GDALConfigEnv")
    mock_context = mocker.MagicMock()
    mock_gdal_config.return_value = mock_context
    mock_context.with_aws_credentials.return_value.__enter__.return_value = None

    # Calculate regions
    tile_size: ImageDimensions = (1024, 1024)
    tile_overlap: ImageDimensions = (50, 50)

    regions = calculator.calculate_regions(image_url="s3://bucket/image.tif", tile_size=tile_size, tile_overlap=tile_overlap)

    # Verify GDAL config was instantiated and used
    mock_gdal_config.assert_called_once()
    mock_context.with_aws_credentials.assert_called_once()

    # Verify regions were calculated
    assert len(regions) == 1
