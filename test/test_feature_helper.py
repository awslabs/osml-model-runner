import geojson
import pytest
import shapely

from aws_oversightml_model_runner.utils.feature_helper import feature_nms, features_to_image_shapes



@pytest.fixture
def sample_gdal_cameramodel():
    from aws_oversightml_model_runner.classes.camera_model import GDALAffineCameraModel
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


def test_feature_nms_empty_list():
    features = feature_nms([])
    assert len(features) == 0


def test_feature_nms_none_list():
    features = feature_nms(None)
    assert len(features) == 0


def test_feature_nms_no_overlap():
    with open("./test/data/detections.geojson", "r") as geojson_file:
        sample_features = geojson.load(geojson_file)["features"]
    assert len(sample_features) > 0

    processed_features = feature_nms(sample_features)
    assert len(sample_features) == len(processed_features)


def test_feature_nms_overlaps():
    # The actual geojson isn't used by nms. The only thing we care about for determining
    # duplicates is the bounds_imcoords property so these shapes are all just simple
    # points
    original_features = [
        geojson.Feature(
            id="feature_a",
            geometry=geojson.Point(0, 0),
            properties={"bounds_imcoords": [50, 50, 100, 100]},
        ),
        geojson.Feature(
            id="feature_b",
            geometry=geojson.Point(0, 0),
            properties={"bounds_imcoords": [45, 45, 105, 105]},
        ),
        geojson.Feature(
            id="feature_c",
            geometry=geojson.Point(0, 0),
            properties={"bounds_imcoords": [250, 250, 300, 275]},
        ),
    ]
    processed_features = feature_nms(original_features)
    assert len(processed_features) == 2


def test_feature_nms_overlaps_custom_threshold():
    # These features have an overlap of ~0.65
    original_features = [
        geojson.Feature(
            id="feature_a",
            geometry=geojson.Point(0, 0),
            properties={"bounds_imcoords": [40, 40, 90, 90]},
        ),
        geojson.Feature(
            id="feature_b",
            geometry=geojson.Point(0, 0),
            properties={"bounds_imcoords": [50, 50, 100, 100]},
        ),
    ]
    assert len(feature_nms(original_features[:], 0.6)) == 1
    assert len(feature_nms(original_features[:], 0.7)) == 2


def test_features_conversion_none(sample_gdal_cameramodel):
    shapes = features_to_image_shapes(sample_gdal_cameramodel, None)
    assert len(shapes) == 0


def test_features_conversion_no_geometry(sample_gdal_cameramodel):
    malformed_feature = {"id": "test_feature"}
    with pytest.raises(ValueError) as e_info:
        features_to_image_shapes(sample_gdal_cameramodel, [malformed_feature])
    assert str(e_info.value) == "Feature does not contain a valid geometry"


def test_features_conversion_unsupported_type(sample_gdal_cameramodel):
    malformed_feature = {
        "id": "test_feature",
        "geometry": {"type": "NewType", "coordinates": [-77.0364, 38.8976]},
    }
    with pytest.raises(ValueError) as e_info:
        features_to_image_shapes(sample_gdal_cameramodel, [malformed_feature])
    assert str(e_info.value) == "Unable to convert feature due to unrecognized or invalid geometry"


def test_features_conversion(sample_gdal_cameramodel):
    with open("./test/data/feature_examples.geojson", "r") as geojson_file:
        sample_features = geojson.load(geojson_file)["features"]
    # We should have 1 feature for each of the 6 geojson types
    assert len(sample_features) == 6

    shapes = features_to_image_shapes(sample_gdal_cameramodel, sample_features)
    assert len(shapes) == len(sample_features)
    assert isinstance(shapes[0], shapely.geometry.Point)
    assert isinstance(shapes[1], shapely.geometry.MultiPoint)
    assert isinstance(shapes[2], shapely.geometry.LineString)
    assert isinstance(shapes[3], shapely.geometry.MultiLineString)
    assert isinstance(shapes[4], shapely.geometry.Polygon)
    assert isinstance(shapes[5], shapely.geometry.MultiPolygon)


def test_polygon_feature_conversion(sample_gdal_cameramodel):
    sample_image_bounds = [(0, 0), (19584, 0), (19584, 19584), (0, 19584)]
    polygon_feature: geojson.Feature = geojson.Feature(
        geometry=geojson.Polygon(
            [
                (-43.681640625, -22.939453125),
                (-43.59375, -22.939453125),
                (-43.59375, -23.02734375),
                (-43.681640625, -23.02734375),
            ]
        )
    )

    shape = features_to_image_shapes(sample_gdal_cameramodel, [polygon_feature])[0]

    assert isinstance(shape, shapely.geometry.Polygon)
    for i in range(0, len(sample_image_bounds)):
        print("TEST: " + str(i))
        print("SIB: " + str(sample_image_bounds[i]))
        print("SEC: " + str(shape.exterior.coords[i]))
        assert pytest.approx(sample_image_bounds[i], rel=0.49, abs=0.49) == shape.exterior.coords[i]
