import numpy as np

from aws_oversightml_model_runner.photogrammetry import (
    ChippedImageSensorModel,
    GeodeticWorldCoordinate,
    ImageCoordinate,
    SensorModel,
)


class FakeSensorModel(SensorModel):
    def __init__(self):
        super().__init__()

    def image_to_world(self, image_coordinate: ImageCoordinate) -> GeodeticWorldCoordinate:
        return GeodeticWorldCoordinate([image_coordinate.x, image_coordinate.y, 0.0])

    def world_to_image(self, world_coordinate: GeodeticWorldCoordinate) -> ImageCoordinate:
        return ImageCoordinate([world_coordinate.x, world_coordinate.y])


def test_chipped_image_sensor_model():
    original_image_coordinates = [
        ImageCoordinate([10.0, 10.0]),
        ImageCoordinate([10.0, 20.0]),
        ImageCoordinate([20.0, 20.0]),
        ImageCoordinate([20.0, 10.0]),
    ]

    chipped_image_coordinates = [
        ImageCoordinate([0.0, 0.0]),
        ImageCoordinate([0.0, 5.0]),
        ImageCoordinate([5.0, 5.0]),
        ImageCoordinate([5.0, 0.0]),
    ]
    sensor_model = ChippedImageSensorModel(
        original_image_coordinates, chipped_image_coordinates, FakeSensorModel()
    )

    image_coordinate = ImageCoordinate([2, 2])
    world_coordinate = sensor_model.image_to_world(image_coordinate)
    assert np.allclose(world_coordinate.coordinate, np.array([14.0, 14.0, 0.0]))
    new_image_coordinate = sensor_model.world_to_image(world_coordinate)
    assert np.allclose(image_coordinate.coordinate, new_image_coordinate.coordinate)
