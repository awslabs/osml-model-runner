import geojson
import pytest

from aws_oversightml_model_runner.classes.camera_model import GDALAffineCameraModel
from aws_oversightml_model_runner.utils.gdal_helper import load_gdal_dataset


@pytest.fixture
def test_dataset_and_camera():
    ds, camera_model = load_gdal_dataset("./test/data/GeogToWGS84GeoKey5.tif")
    return ds, camera_model


@pytest.fixture
def sample_gdal_cameramodel():

    # Test coordinate calculations using geotransform matrix from sample SpaceNet RIO image
    transform = (
        -43.681640625,
        4.487879136029412e-06,
        0.0,
        -22.939453125,
        0.0,
        -4.487879136029412e-06,
    )
    return GDALAffineCameraModel(transform)


@pytest.fixture
def sample_image_bounds():
    return [(0, 0), (19584, 0), (19584, 19584), (0, 19584)]


@pytest.fixture
def sample_geo_bounds():
    return [
        (-43.681640625, -22.939453125),
        (-43.59375, -22.939453125),
        (-43.59375, -23.02734375),
        (-43.681640625, -23.02734375),
    ]


@pytest.fixture()
def sample_geojson_detections():
    with open("./test/data/detections.geojson", "r") as geojson_file:
        return geojson.load(geojson_file)


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
        load_gdal_dataset("./test/data/does-not-exist.tif")


def test_gdal_cameramodel(sample_gdal_cameramodel, sample_image_bounds, sample_geo_bounds):
    assert pytest.approx(
        sample_geo_bounds[0], rel=1e-6, abs=1e-6
    ) == sample_gdal_cameramodel.image_to_world(sample_image_bounds[0])
    assert pytest.approx(
        sample_geo_bounds[1], rel=1e-6, abs=1e-6
    ) == sample_gdal_cameramodel.image_to_world(sample_image_bounds[1])
    assert pytest.approx(
        sample_image_bounds[0], rel=1e-6, abs=1e-6
    ) == sample_gdal_cameramodel.world_to_image(sample_geo_bounds[0])
    assert pytest.approx(
        sample_image_bounds[1], rel=1e-6, abs=1e-6
    ) == sample_gdal_cameramodel.world_to_image(sample_geo_bounds[1])


def test_geolocate_features(sample_gdal_cameramodel, sample_geojson_detections):
    print(sample_geojson_detections)
    sample_features = sample_geojson_detections["features"]
    assert len(sample_features) == 4

    sample_gdal_cameramodel.geolocate_detections(sample_features)

    assert len(sample_features) == 4
    for feature in sample_features:
        assert "bbox" in feature
        assert len(feature["bbox"]) == 4
        assert "center_latitude" in feature["properties"]
        assert "center_longitude" in feature["properties"]
        assert isinstance(feature["geometry"], geojson.Polygon)
