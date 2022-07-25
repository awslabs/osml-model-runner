import pytest
from osgeo import gdal

from aws_model_runner.gdal_utils import GDALConfigEnv, load_gdal_dataset
from aws_model_runner.georeference import GDALAffineCameraModel


def test_gdal_config_env():
    gdal.SetConfigOption("AWS_SECRET_ACCESS_KEY", "DEFAULT_AWS_SECRET_ACCESS_KEY")
    gdal.SetConfigOption("AWS_ACCESS_KEY_ID", "DEFAULT_AWS_ACCESS_KEY_ID")
    gdal.SetConfigOption("AWS_SESSION_TOKEN", "DEFAULT_AWS_SESSION_TOKEN")

    with GDALConfigEnv().with_aws_credentials(
        {
            "SecretAccessKey": "TEMP_SECRET_ACCESS_KEY",
            "AccessKeyId": "TEMP_ACCESS_KEY_ID",
            "SessionToken": "TEMP_SESSION_TOKEN",
        }
    ):
        # Verify that the temporary credentials were set for the scope of this "with" statement
        assert gdal.GetConfigOption("AWS_SECRET_ACCESS_KEY") == "TEMP_SECRET_ACCESS_KEY"
        assert gdal.GetConfigOption("AWS_ACCESS_KEY_ID") == "TEMP_ACCESS_KEY_ID"
        assert gdal.GetConfigOption("AWS_SESSION_TOKEN") == "TEMP_SESSION_TOKEN"

    # Verify that the original default values are restored correctly
    assert gdal.GetConfigOption("AWS_SECRET_ACCESS_KEY") == "DEFAULT_AWS_SECRET_ACCESS_KEY"
    assert gdal.GetConfigOption("AWS_ACCESS_KEY_ID") == "DEFAULT_AWS_ACCESS_KEY_ID"
    assert gdal.GetConfigOption("AWS_SESSION_TOKEN") == "DEFAULT_AWS_SESSION_TOKEN"


@pytest.fixture
def test_dataset_and_camera():
    ds, camera_model = load_gdal_dataset("./test/data/GeogToWGS84GeoKey5.tif")
    return ds, camera_model


def test_gdal_load_success(test_dataset_and_camera):
    ds = test_dataset_and_camera[0]
    camera_model = test_dataset_and_camera[1]

    assert ds is not None
    assert ds.RasterXSize == 101
    assert ds.RasterYSize == 101

    assert camera_model is not None
    assert isinstance(camera_model, GDALAffineCameraModel)


def test_gdal_load_invalid():
    with pytest.raises(ValueError):
        ds, camera_model = load_gdal_dataset("./test/data/does-not-exist.tif")
