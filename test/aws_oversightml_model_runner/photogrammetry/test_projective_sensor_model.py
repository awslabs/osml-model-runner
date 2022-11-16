from math import radians

import numpy as np

from aws_oversightml_model_runner.photogrammetry import (
    ConstantElevationModel,
    GeodeticWorldCoordinate,
    ImageCoordinate,
    ProjectiveSensorModel,
)


def test_projective_sensor_model():
    world_coordinates = [
        GeodeticWorldCoordinate([radians(10.0), radians(10.0), 0.0]),
        GeodeticWorldCoordinate([radians(10.0), radians(30.0), 0.0]),
        GeodeticWorldCoordinate([radians(20.0), radians(30.0), 0.0]),
        GeodeticWorldCoordinate([radians(20.0), radians(10.0), 0.0]),
    ]

    chipped_image_coordinates = [
        ImageCoordinate([0.0, 200.0]),
        ImageCoordinate([0.0, 0.0]),
        ImageCoordinate([100.0, 0.0]),
        ImageCoordinate([100.0, 200.0]),
    ]
    sensor_model = ProjectiveSensorModel(world_coordinates, chipped_image_coordinates)
    elevation_model = ConstantElevationModel(42.0)

    image_coordinate = ImageCoordinate([50.0, 0.0])

    # Test with an external elevation model
    world_coordinate = sensor_model.image_to_world(
        image_coordinate, elevation_model=elevation_model
    )
    assert np.allclose(world_coordinate.coordinate, np.array([radians(15.0), radians(30.0), 42.0]))

    # Test without an external elevation model
    world_coordinate = sensor_model.image_to_world(image_coordinate)
    assert np.allclose(world_coordinate.coordinate, np.array([radians(15.0), radians(30.0), 0.0]))
    new_image_coordinate = sensor_model.world_to_image(world_coordinate)
    assert np.allclose(image_coordinate.coordinate, new_image_coordinate.coordinate)

    image_coordinate = ImageCoordinate([50.0, 100.0])
    world_coordinate = sensor_model.image_to_world(image_coordinate)
    assert np.allclose(world_coordinate.coordinate, np.array([radians(15.0), radians(20.0), 0.0]))
