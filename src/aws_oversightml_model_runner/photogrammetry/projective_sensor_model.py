from typing import Any, Dict, List, Optional

import numpy as np
from skimage.transform import ProjectiveTransform

from .coordinates import GeodeticWorldCoordinate, ImageCoordinate
from .elevation_model import ElevationModel
from .sensor_model import SensorModel


class ProjectiveSensorModel(SensorModel):
    """
    This sensor model is used when we have a set of 2D tie point correspondences (longitude, latitude) -> (x, y) for
    an image.
    """

    def __init__(
        self,
        world_coordinates: List[GeodeticWorldCoordinate],
        image_coordinates: List[ImageCoordinate],
    ):
        """
        This constructor estimates a projective transform given the image correspondences.

        :param world_coordinates: a list of world points
        :param image_coordinates: the corresponding list of locations in the image
        """
        super().__init__()
        self.lonlat_to_xy_transform = ProjectiveTransform()
        src_coordinates = [
            np.array([world_coordinate.longitude, world_coordinate.latitude])
            for world_coordinate in world_coordinates
        ]
        dst_coordinates = [image_coordinate.coordinate for image_coordinate in image_coordinates]
        self.lonlat_to_xy_transform.estimate(np.vstack(src_coordinates), np.vstack(dst_coordinates))

    def image_to_world(
        self,
        image_coordinate: ImageCoordinate,
        elevation_model: Optional[ElevationModel] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> GeodeticWorldCoordinate:
        """
        This function returns the longitude, latitude, elevation world coordinate associated with the x, y coordinate
        of any pixel in the image.

        :param image_coordinate: the x, y image coordinate
        :param elevation_model: an optional elevation model used to fix the elevation of the image coordinate
        :param options: a optional dictionary of hints, this camera does not support any specific hints
        :return: the longitude, latitude, elevation world coordinate
        """
        world_coords = self.lonlat_to_xy_transform.inverse(image_coordinate.coordinate)
        world_coordinate = GeodeticWorldCoordinate(np.append(world_coords[0], [0]))
        if elevation_model:
            elevation_model.set_elevation(world_coordinate)
        return world_coordinate

    def world_to_image(self, world_coordinate: GeodeticWorldCoordinate) -> ImageCoordinate:
        """
        This function returns the x, y image coordinate associated with a given longitude, latitude, elevation world
        coordinate.

        :param world_coordinate: the longitude, latitude, elevation world coordinate
        :return: the x, y image coordinate
        """
        image_coords = self.lonlat_to_xy_transform(world_coordinate.coordinate[0:2])
        image_coordinate = ImageCoordinate(image_coords[0])
        return image_coordinate
