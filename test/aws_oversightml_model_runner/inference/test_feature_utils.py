import unittest
from unittest import mock

import geojson
import pytest
import shapely
from configuration import TEST_ENV_CONFIG


@mock.patch.dict("os.environ", TEST_ENV_CONFIG, clear=True)
class TestFeatureUtils(unittest.TestCase):
    def test_feature_nms_empty_list(self):
        from aws_oversightml_model_runner.inference.feature_utils import feature_nms

        features = feature_nms([])
        assert len(features) == 0

    def test_feature_nms_none_list(self):
        from aws_oversightml_model_runner.inference.feature_utils import feature_nms

        features = feature_nms(None)
        assert len(features) == 0

    def test_feature_nms_no_overlap(self):
        from aws_oversightml_model_runner.inference.feature_utils import feature_nms

        with open("./test/data/detections.geojson", "r") as geojson_file:
            sample_features = geojson.load(geojson_file)["features"]
        assert len(sample_features) > 0

        processed_features = feature_nms(sample_features)
        assert len(sample_features) == len(processed_features)

    def test_feature_nms_overlaps(self):
        from aws_oversightml_model_runner.inference.feature_utils import feature_nms

        # The actual geojson isn't used by nms. The only thing we care about for determining
        # duplicates is the bounds_imcoords property so these shapes are all just simple
        # points
        original_features = [
            geojson.Feature(
                id="feature_a",
                geometry=geojson.Point((0, 0)),
                properties={"bounds_imcoords": [50, 50, 100, 100]},
            ),
            geojson.Feature(
                id="feature_b",
                geometry=geojson.Point((0, 0)),
                properties={"bounds_imcoords": [45, 45, 105, 105]},
            ),
            geojson.Feature(
                id="feature_c",
                geometry=geojson.Point((0, 0)),
                properties={"bounds_imcoords": [250, 250, 300, 275]},
            ),
        ]
        processed_features = feature_nms(original_features)
        assert len(processed_features) == 2

    def test_feature_nms_overlaps_custom_threshold(self):
        from aws_oversightml_model_runner.inference.feature_utils import feature_nms

        # These features have an overlap of ~0.65
        original_features = [
            geojson.Feature(
                id="feature_a",
                geometry=geojson.Point((0, 0)),
                properties={"bounds_imcoords": [40, 40, 90, 90]},
            ),
            geojson.Feature(
                id="feature_b",
                geometry=geojson.Point((0, 0)),
                properties={"bounds_imcoords": [50, 50, 100, 100]},
            ),
        ]
        assert len(feature_nms(original_features[:], 0.6)) == 1
        assert len(feature_nms(original_features[:], 0.7)) == 2

    def test_features_conversion_none(self):
        from aws_oversightml_model_runner.inference.feature_utils import features_to_image_shapes

        shapes = features_to_image_shapes(self.build_gdal_sensor_model(), None)
        assert len(shapes) == 0

    def test_features_conversion_no_geometry(self):
        from aws_oversightml_model_runner.inference.feature_utils import features_to_image_shapes

        malformed_feature = {"id": "test_feature"}
        with pytest.raises(ValueError) as e_info:
            features_to_image_shapes(self.build_gdal_sensor_model(), [malformed_feature])
        assert str(e_info.value) == "Feature does not contain a valid geometry"

    def test_features_conversion_unsupported_type(self):
        from aws_oversightml_model_runner.inference.feature_utils import features_to_image_shapes

        malformed_feature = {
            "id": "test_feature",
            "geometry": {"type": "NewType", "coordinates": [-77.0364, 38.8976, 0.0]},
        }
        with pytest.raises(ValueError) as e_info:
            features_to_image_shapes(self.build_gdal_sensor_model(), [malformed_feature])
        assert (
            str(e_info.value) == "Unable to convert feature due to unrecognized or invalid geometry"
        )

    def test_features_conversion(self):
        from aws_oversightml_model_runner.inference.feature_utils import features_to_image_shapes

        with open("./test/data/feature_examples.geojson", "r") as geojson_file:
            sample_features = geojson.load(geojson_file)["features"]
        # We should have 1 feature for each of the 6 geojson types
        assert len(sample_features) == 6

        shapes = features_to_image_shapes(self.build_gdal_sensor_model(), sample_features)
        assert len(shapes) == len(sample_features)
        assert isinstance(shapes[0], shapely.geometry.Point)
        assert isinstance(shapes[1], shapely.geometry.MultiPoint)
        assert isinstance(shapes[2], shapely.geometry.LineString)
        assert isinstance(shapes[3], shapely.geometry.MultiLineString)
        assert isinstance(shapes[4], shapely.geometry.Polygon)
        assert isinstance(shapes[5], shapely.geometry.MultiPolygon)

    def test_polygon_feature_conversion(self):
        from aws_oversightml_model_runner.inference.feature_utils import features_to_image_shapes

        sample_image_bounds = [(0, 0), (19584, 0), (19584, 19584), (0, 19584)]
        polygon_feature: geojson.Feature = geojson.Feature(
            geometry=geojson.Polygon(
                [
                    (-43.681640625, -22.939453125, 0.0),
                    (-43.59375, -22.939453125, 0.0),
                    (-43.59375, -23.02734375, 0.0),
                    (-43.681640625, -23.02734375, 0.0),
                ]
            )
        )

        shape = features_to_image_shapes(self.build_gdal_sensor_model(), [polygon_feature])[0]

        assert isinstance(shape, shapely.geometry.Polygon)
        for i in range(0, len(sample_image_bounds)):
            print("TEST: " + str(i))
            print("SIB: " + str(sample_image_bounds[i]))
            print("SEC: " + str(shape.exterior.coords[i]))
            assert (
                pytest.approx(sample_image_bounds[i], rel=0.49, abs=0.49)
                == shape.exterior.coords[i]
            )

    def test_calculate_processing_bounds_no_roi(self):
        from aws_oversightml_model_runner.inference.feature_utils import calculate_processing_bounds

        ds, sensor_model = self.get_dataset_and_camera()

        processing_bounds = calculate_processing_bounds(ds, None, sensor_model)

        assert processing_bounds == ((0, 0), (101, 101))

    def test_calculate_processing_bounds_full_image(self):
        from aws_oversightml_model_runner.inference.feature_utils import calculate_processing_bounds

        ds, sensor_model = self.get_dataset_and_camera()

        roi = shapely.wkt.loads("POLYGON ((8 50, 10 50, 10 60, 8 60, 8 50))")

        processing_bounds = calculate_processing_bounds(ds, roi, sensor_model)

        assert processing_bounds == ((0, 0), (101, 101))

    def test_calculate_processing_bounds_intersect(self):
        from aws_oversightml_model_runner.inference.feature_utils import calculate_processing_bounds

        ds, sensor_model = self.get_dataset_and_camera()

        roi = shapely.wkt.loads(
            "POLYGON ((8 52, 9.001043490711101 52.0013898967889, 9 54, 8 54, 8 52))"
        )

        # Manually verify the lon/lat coordinates of the image positions used in this test with these
        # print statements
        # print(sensor_model.image_to_world((0, 0)))
        # print(sensor_model.image_to_world((50, 50)))
        # print(sensor_model.image_to_world((101, 101)))
        processing_bounds = calculate_processing_bounds(ds, roi, sensor_model)

        # Processing bounds is in ((r, c), (w, h))
        assert processing_bounds == ((0, 0), (50, 50))

    def test_calculate_processing_bounds_chip(self):
        from aws_oversightml_model_runner.inference.feature_utils import calculate_processing_bounds

        ds, sensor_model = self.get_dataset_and_camera()
        roi = shapely.wkt.loads(
            "POLYGON (("
            "8.999932379599102 52.0023621190119, 8.999932379599102 52.0002787856769, "
            "9.001599046267101 52.0002787856769, 9.001599046267101 52.0023621190119, "
            "8.999932379599102 52.0023621190119"
            "))"
        )

        # Manually verify the lon/lat coordinates of the image positions used in this test with these
        # print statements
        # print(sensor_model.image_to_world((10, 15)))
        # print(sensor_model.image_to_world((70, 90)))
        processing_bounds = calculate_processing_bounds(ds, roi, sensor_model)

        # Processing bounds is in ((r, c), (w, h))
        assert processing_bounds == ((15, 10), (60, 75))

    @staticmethod
    def build_gdal_sensor_model():
        from aws_oversightml_model_runner.photogrammetry import GDALAffineSensorModel

        # Test coordinate calculations using geotransform matrix from sample SpaceNet RIO image
        transform = [
            -43.681640625,
            4.487879136029412e-06,
            0.0,
            -22.939453125,
            0.0,
            -4.487879136029412e-06,
        ]
        return GDALAffineSensorModel(transform)

    @staticmethod
    def get_dataset_and_camera():
        from aws_oversightml_model_runner.gdal.gdal_utils import load_gdal_dataset

        ds, sensor_model = load_gdal_dataset("./test/data/GeogToWGS84GeoKey5.tif")
        return ds, sensor_model


if __name__ == "__main__":
    unittest.main()
