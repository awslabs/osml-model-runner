import geojson
import pytest
import shapely

from aws_oversightml_model_runner.georeference import GDALAffineCameraModel


@pytest.fixture
def sample_gdal_cameramodel():
    # Test coordinate calculations using geotransform matrix from sample SpaceNet RIO image
    transform = (-43.681640625, 4.487879136029412e-06, 0.0, -22.939453125, 0.0, -4.487879136029412e-06)
    return GDALAffineCameraModel(transform)


@pytest.fixture
def sample_image_bounds():
    return [(0, 0),
            (19584, 0),
            (19584, 19584),
            (0, 19584)]


@pytest.fixture
def sample_geo_bounds():
    return [(-43.681640625, -22.939453125),
            (-43.59375, -22.939453125),
            (-43.59375, -23.02734375),
            (-43.681640625, -23.02734375)]


@pytest.fixture()
def sample_geojson_detections():
    with open("./test/data/detections.geojson", "r") as geojson_file:
        return geojson.load(geojson_file)


def test_gdal_cameramodel(sample_gdal_cameramodel, sample_image_bounds, sample_geo_bounds):
    assert pytest.approx(sample_geo_bounds[0], rel=1e-6, abs=1e-6) == sample_gdal_cameramodel.image_to_world(
        sample_image_bounds[0])
    assert pytest.approx(sample_geo_bounds[1], rel=1e-6, abs=1e-6) == sample_gdal_cameramodel.image_to_world(
        sample_image_bounds[1])
    assert pytest.approx(sample_image_bounds[0], rel=1e-6, abs=1e-6) == sample_gdal_cameramodel.world_to_image(
        sample_geo_bounds[0])
    assert pytest.approx(sample_image_bounds[1], rel=1e-6, abs=1e-6) == sample_gdal_cameramodel.world_to_image(
        sample_geo_bounds[1])


def test_point_feature_conversion(sample_gdal_cameramodel, sample_image_bounds, sample_geo_bounds):
    point_feature: geojson.Feature = geojson.Feature(geometry=geojson.Point(sample_geo_bounds[0]))

    shape = sample_gdal_cameramodel.feature_to_image_shape(point_feature)

    assert isinstance(shape, shapely.geometry.Point)
    assert pytest.approx(sample_image_bounds[0], rel=0.49, abs=0.49) == shape.coords[0]


def test_polygon_feature_conversion(sample_gdal_cameramodel, sample_image_bounds, sample_geo_bounds):
    polygon_feature: geojson.Feature = geojson.Feature(geometry=geojson.Polygon(sample_geo_bounds))

    shape = sample_gdal_cameramodel.feature_to_image_shape(polygon_feature)

    assert isinstance(shape, shapely.geometry.Polygon)
    for i in range(0, len(sample_image_bounds)):
        print("TEST: " + str(i))
        print("SIB: " + str(sample_image_bounds[i]))
        print("SEC: " + str(shape.exterior.coords[i]))
        assert pytest.approx(sample_image_bounds[i], rel=0.49, abs=0.49) == shape.exterior.coords[i]


def test_geolocate_features(sample_gdal_cameramodel, sample_geojson_detections):
    print(sample_geojson_detections)
    sample_features = sample_geojson_detections['features']
    assert len(sample_features) == 4

    sample_gdal_cameramodel.geolocate_detections(sample_features)

    assert len(sample_features) == 4
    for feature in sample_features:
        assert 'bbox' in feature
        assert len(feature['bbox']) == 4
        assert 'center_latitude' in feature['properties']
        assert 'center_longitude' in feature['properties']
        assert isinstance(feature['geometry'], geojson.Polygon)

