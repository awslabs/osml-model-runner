from math import degrees

import geojson
import pytest

from aws_oversightml_model_runner.photogrammetry import (
    GeodeticWorldCoordinate,
    ImageCoordinate,
    SensorModel,
)


@pytest.fixture()
def sample_geojson_detections():
    with open("./test/data/detections.geojson", "r") as geojson_file:
        return geojson.load(geojson_file)


def test_geolocate_features(sample_geojson_detections):
    class StubSensorModel(SensorModel):
        def image_to_world(self, image_coordinate: ImageCoordinate) -> GeodeticWorldCoordinate:
            return GeodeticWorldCoordinate([1.0, 2.0, 3.0])

        def world_to_image(self, world_coordinate: GeodeticWorldCoordinate) -> ImageCoordinate:
            return ImageCoordinate([1.0, 2.0])

    stub_sensor_model = StubSensorModel()

    print(sample_geojson_detections)
    sample_features = sample_geojson_detections["features"]
    assert len(sample_features) == 4

    stub_sensor_model.geolocate_detections(sample_features)

    assert len(sample_features) == 4
    for feature in sample_features:
        assert "bbox" in feature
        assert len(feature["bbox"]) == 4
        assert "center_latitude" in feature["properties"]
        assert degrees(2.0) == pytest.approx(feature["properties"]["center_latitude"])
        assert "center_longitude" in feature["properties"]
        assert degrees(1.0) == pytest.approx(feature["properties"]["center_longitude"])
        assert isinstance(feature["geometry"], geojson.Polygon)
        for coord in feature["geometry"]["coordinates"][0]:
            assert degrees(1.0) == pytest.approx(coord[0])
            assert degrees(2.0) == pytest.approx(coord[1])
            assert 3.0 == pytest.approx(coord[2])
