from typing import List

import numpy as np
from skimage.transform import ProjectiveTransform

from .coordinates import GeodeticWorldCoordinate, ImageCoordinate
from .sensor_model import SensorModel


class ChippedImageSensorModel(SensorModel):
    def __init__(
        self,
        original_image_coordinates: List[ImageCoordinate],
        chipped_image_coordinates: List[ImageCoordinate],
        full_image_sensor_model: SensorModel,
    ):
        self.full_image_sensor_model = full_image_sensor_model
        self.full_to_chip_transform = ProjectiveTransform()
        src_coordinates = [
            image_coordinate.coordinate for image_coordinate in original_image_coordinates
        ]
        dst_coordinates = [
            image_coordinate.coordinate for image_coordinate in chipped_image_coordinates
        ]
        self.full_to_chip_transform.estimate(np.vstack(src_coordinates), np.vstack(dst_coordinates))

    def image_to_world(self, image_coordinate: ImageCoordinate) -> GeodeticWorldCoordinate:
        full_coords = self.full_to_chip_transform.inverse(image_coordinate.coordinate)
        full_image_coordinate = ImageCoordinate(full_coords[0])
        return self.full_image_sensor_model.image_to_world(full_image_coordinate)

    def world_to_image(self, world_coordinate: GeodeticWorldCoordinate) -> ImageCoordinate:
        full_image_coordinate = self.full_image_sensor_model.world_to_image(world_coordinate)
        chip_coords = self.full_to_chip_transform(full_image_coordinate.coordinate)
        chipped_image_coordinate = ImageCoordinate(chip_coords[0])
        return chipped_image_coordinate
