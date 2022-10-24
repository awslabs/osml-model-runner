import functools
import logging
import math
from abc import ABC, abstractmethod
from typing import List, Tuple

import geojson

from .coordinates import GeodeticWorldCoordinate, ImageCoordinate

logger = logging.getLogger(__name__)


class SensorModel(ABC):
    """
    A sensor model is an abstraction that maps the information in a georeferenced image to the real world. The
    concrete implementations of this abstraction will either capture the physical service model characteristics or
    more frequently an approximation of that physical model that allow users to transform world coordinates to
    image coordinates.
    """

    def __init__(self) -> None:
        pass

    @abstractmethod
    def image_to_world(self, image_coordinate: ImageCoordinate) -> GeodeticWorldCoordinate:
        """
        This function returns the longitude, latitude, elevation world coordinate associated with the x, y coordinate
        of any pixel in the image.

        :param image_coordinate: the x, y image coordinate
        :return: the longitude, latitude, elevation world coordinate
        """
        pass

    @abstractmethod
    def world_to_image(self, world_coordinate: GeodeticWorldCoordinate) -> ImageCoordinate:
        """
        This function returns the x, y image coordinate associated with a given longitude, latitude, elevation world
        coordinate.

        :param world_coordinate: the longitude, latitude, elevation world coordinate
        :return: the x, y image coordinate
        """
        pass

    def geolocate_detections(self, features: List[geojson.Feature]) -> None:
        """
        This function takes a collection of GeoJSON features that have a "bounds_imcoords" property that contains
        x, y image coordinates and then generates a corresponding world geometry for that feature.

        # TODO: Move this into ModelRunner; it is closely tied to the format of features in the app not the SensorModel

        :param features: a List of GeoJSON features to update
        :return: None, the features of the input parameter has a new "geometry" and other geospatial properties are set.
        """
        for feature in features:
            bbox = feature["properties"]["bounds_imcoords"]
            center_xy = [
                bbox[0] + (bbox[2] - bbox[0]) / 2,
                bbox[1] + (bbox[3] - bbox[1]) / 2,
            ]
            center_location = self.image_to_world(ImageCoordinate(center_xy))

            # This converts the coordinates of the bounding box in the image into a polygon in
            # lat/lon/elevation space. We may want to make this calculation a little smarter and detect
            # cases where the camera model returns several near identical points for this bounding
            # box. In that case it may be better to create a single simplified point geometry e.g.
            # feature['geometry'] = geojson.Point(center_location.coordinate)
            polygon_coords = (
                SensorModel.radians_coordinate_to_degrees(
                    self.image_to_world(ImageCoordinate([bbox[0], bbox[1]]))
                ),
                SensorModel.radians_coordinate_to_degrees(
                    self.image_to_world(ImageCoordinate([bbox[0], bbox[3]]))
                ),
                SensorModel.radians_coordinate_to_degrees(
                    self.image_to_world(ImageCoordinate([bbox[2], bbox[3]]))
                ),
                SensorModel.radians_coordinate_to_degrees(
                    self.image_to_world(ImageCoordinate([bbox[2], bbox[1]]))
                ),
                SensorModel.radians_coordinate_to_degrees(
                    self.image_to_world(ImageCoordinate([bbox[0], bbox[1]]))
                ),
            )
            # Note that for geojson polygons the "coordinates" member must be an array of
            # LinearRing coordinate arrays. For Polygons with multiple rings, the first must be
            # the exterior ring and any others must be interior rings or holes. We only have an
            # exterior ring hence creating an array of the coordinates is appropriate here.
            feature["geometry"] = geojson.Polygon([polygon_coords])

            # Geojson features can optionally have a bounding box that contains [min lon, min lat,
            # max lon, max lat]. This code computes that bounding box from the polygon boundary
            # coordinates
            feature["bbox"] = functools.reduce(
                lambda prev, coord: [
                    min(coord[0], prev[0]),
                    min(coord[1], prev[1]),
                    max(coord[0], prev[2]),
                    max(coord[1], prev[3]),
                ],
                polygon_coords,
                [math.inf, math.inf, -math.inf, -math.inf],
            )

            # Adding these because some visualization tools (e.g. kepler.gl) can perform more
            # advanced rendering (e.g. cluster layers) if the data points have single coordinates.
            feature["properties"]["center_longitude"] = math.degrees(center_location.longitude)
            feature["properties"]["center_latitude"] = math.degrees(center_location.latitude)

    @staticmethod
    def radians_coordinate_to_degrees(
        coordinate: GeodeticWorldCoordinate,
    ) -> Tuple[float, float, float]:
        """
        GeoJSON coordinate order is a decimal longitude, latitude with an optional height as a 3rd value
        (i.e. [lon, lat, ht]). The WorldCoordinate uses the same ordering but the longitude and latitude are expressed
        in radians rather than degrees.

        :param coordinate: the geodetic world coordinate (longitude, latitude, elevation)
        :return: degrees(longitude), degrees(latitude), elevation
        """
        return (
            math.degrees(coordinate.longitude),
            math.degrees(coordinate.latitude),
            coordinate.elevation,
        )
