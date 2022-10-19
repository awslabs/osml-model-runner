import json
from unittest.mock import MagicMock, patch

import pytest

from aws_oversightml_model_runner.gdal.gdal_utils import (
    get_extensions_from_driver,
    get_gdal_driver_extensions,
    get_image_extension,
    load_gdal_dataset,
    normalize_extension,
    select_extension,
)
from aws_oversightml_model_runner.photogrammetry import GDALAffineSensorModel


@pytest.fixture
def test_dataset_and_sensor_model():
    ds, sensor_model = load_gdal_dataset("./test/data/GeogToWGS84GeoKey5.tif")
    return ds, sensor_model


def test_gdal_load_success(test_dataset_and_sensor_model):
    ds = test_dataset_and_sensor_model[0]
    sensor_model = test_dataset_and_sensor_model[1]

    assert ds is not None
    assert ds.RasterXSize == 101
    assert ds.RasterYSize == 101

    assert sensor_model is not None
    assert isinstance(sensor_model, GDALAffineSensorModel)


@patch("aws_oversightml_model_runner.gdal.gdal_utils.get_gdal_driver_extensions")
@patch("aws_oversightml_model_runner.gdal.gdal_utils.gdal")
def test_get_extensions_from_driver(mock_gdal, mock_get_drivers):
    with open("./test/data/mock_gdal_info.json") as mock_data:
        mock_gdal_info = json.load(mock_data)
    with open("./test/data/mock_driver_lookup.json") as mock_data:
        mock_driver_lookup = json.load(mock_data)
    mock_gdal_info["driverLongName"] = "GeoTIFF"
    mock_gdal.Info = MagicMock(return_value=mock_gdal_info)
    mock_get_drivers.return_value = mock_driver_lookup

    possible_extensions = get_extensions_from_driver("dummy")

    expected_extensons = ["tif", "tiff"]
    assert possible_extensions == expected_extensons


def test_get_gdal_driver_extensions():
    driver_lookup = get_gdal_driver_extensions()
    assert "GeoTIFF" in driver_lookup
    assert "National Imagery Transmission Format" in driver_lookup
    assert "Portable Network Graphics" in driver_lookup
    assert "JPEG JFIF" in driver_lookup


def test_select_extension():
    assert select_extension("some/file_name.dt1", ["dt0", "dt1", "dt2"]) == "DT1"
    assert select_extension("some/tricky_tif.tiff", ["tif", "tiff"]) == "TIFF"
    assert select_extension("some/file_name_without_extension", ["dt0", "dt1", "dt2"]) == "DT0"
    assert select_extension("some/file_name.odd", []) == "UNKNOWN"


def test_normalize_extension():
    assert normalize_extension("ntf") == "NITF"
    assert normalize_extension("nTf") == "NITF"
    assert normalize_extension("nitf") == "NITF"
    assert normalize_extension("Tif") == "TIFF"
    assert normalize_extension("tiff") == "TIFF"
    assert normalize_extension("jpg") == "JPEG"
    assert normalize_extension("jpEg") == "JPEG"
    assert normalize_extension("JPEG") == "JPEG"
    assert normalize_extension("png") == "PNG"
    assert normalize_extension("DT2") == "DT2"


def test_get_image_extension():
    assert get_image_extension("./test/data/GeogToWGS84GeoKey5.tif") == "TIFF"


def test_gdal_load_invalid():
    with pytest.raises(ValueError):
        load_gdal_dataset("./test/data/does-not-exist.tif")
