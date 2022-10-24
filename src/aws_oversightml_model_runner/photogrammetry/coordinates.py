# TODO: Add typing for ArrayLike inputs once Numpy upgraded to 1.20+
# from numpy.typing import ArrayLike
from typing import Any

import numpy as np
import pyproj


class WorldCoordinate:
    """
    A world coordinate is a vector representing a position in 3D space. The ground coordinate system specified is
    either Geodetic (latitude, longitude, and height above the WGS 84 reference ellipsoid), or Rectangular (cartesian
    coordinates in reference to a local tangent plane). Regardless whether the coordinate system is specified as
    Geodetic or Rectangular, associated ground point locations are represented as a triple – x, y, and z.

    A Rectangular system should be specified when the image footprint is near the earth’s North or South pole. Either
    a Rectangular or Geodetic system can be specified when the footprint is near 180 degrees East longitude. However,
    if Geodetic, the range for longitude is then specified as (0,2pi) radians instead of the usual (-pi, +pi) radians.
    """

    def __init__(self, coordinate: Any = None):
        """
        Constructs a world coordinate from an x, y, z triple. The triple can be expressed as a List or any other
        structure that can be used to construct a Numpy array.

        TODO: Update type hint for coordinate to be ArrayLike once Numpy > 1.20
        :param coordinate: the x,y,z components
        """
        if coordinate is None:
            coordinate = [0.0, 0.0, 0.0]

        if len(coordinate) != 3:
            raise ValueError("WorldCoordinates must have 3 components (x,y,z)")

        self.coordinate = np.array(coordinate, dtype=np.float64)

    @property
    def x(self) -> float:
        return self.coordinate[0]

    @x.setter
    def x(self, value: float) -> None:
        self.coordinate[0] = value

    @property
    def y(self) -> float:
        return self.coordinate[1]

    @y.setter
    def y(self, value: float) -> None:
        self.coordinate[1] = value

    @property
    def z(self) -> float:
        return self.coordinate[2]

    @z.setter
    def z(self, value: float) -> None:
        self.coordinate[2] = value


class GeodeticWorldCoordinate(WorldCoordinate):
    """
    A GeodeticWorldCoordinate is an WorldCoordinate where the x,y,z components can be interpreted as longitude,
    latitude, and elevation.
    """

    def __init__(self, coordinate: Any = None):
        """
        Constructs a geodetic world coordinate from an longitude, latitude, elevation triple. The longitude and
        latitude components are in radians. The triple can be expressed as a List or any other structure that can
        be used to construct a Numpy array.

        TODO: Update type hint for coordinate to be ArrayLike once Numpy > 1.20
        :param coordinate: the longitude, latitude, elevation components
        """
        super().__init__(coordinate)

    @property
    def longitude(self) -> float:
        return self.x

    @longitude.setter
    def longitude(self, value: float) -> None:
        self.x = value

    @property
    def latitude(self) -> float:
        return self.y

    @latitude.setter
    def latitude(self, value: float) -> None:
        self.y = value

    @property
    def elevation(self) -> float:
        return self.z

    @elevation.setter
    def elevation(self, value: float) -> None:
        self.z = value


# These are common definitions of projections used by Pyproj. They are used when converting between an Earth Centered
# Earth Fixed (ECEF or geocentric) coordinate system that uses cartesian coordinates and a longitude, latitude based
# geographic coordinate system. Both of these systems use the WGS84 datum which is a widely used standard among our
# customers
ECEF_PROJ = pyproj.Proj(proj="geocent", ellps="WGS84", datum="WGS84")
LLA_PROJ = pyproj.Proj(proj="latlong", ellps="WGS84", datum="WGS84")


def geocentric_to_geodetic(ecef_world_coordinate: WorldCoordinate) -> GeodeticWorldCoordinate:
    """
    Converts a ECEF world coordinate (x, y, z) in meters into a (longititude, latitude, elevation) geodetic coordinate
    with units of radians, radians, meters.

    :param ecef_world_coordinate: the geocentric coordinate
    :return: the geodetic coordinate
    """
    return GeodeticWorldCoordinate(
        pyproj.transform(
            ECEF_PROJ,
            LLA_PROJ,
            ecef_world_coordinate.x,
            ecef_world_coordinate.y,
            ecef_world_coordinate.z,
            radians=True,
        )
    )


def geodetic_to_geocentric(geodetic_coordinate: GeodeticWorldCoordinate) -> WorldCoordinate:
    """
    Converts a geodetic world coordinate (longitude, latitude, elevation) with units of radians, radians, meters into
    a ECEF / geocentric world coordinate (x, y, z) in meters.

    :param geodetic_coordinate: the geodetic coordinate
    :return: the geocentric coordinate
    """
    return WorldCoordinate(
        pyproj.transform(
            LLA_PROJ,
            ECEF_PROJ,
            geodetic_coordinate.longitude,
            geodetic_coordinate.latitude,
            geodetic_coordinate.elevation,
            radians=True,
        )
    )


class ImageCoordinate:
    """
    This image coordinate system convention is defined as follows. The upper left corner of the upper left pixel
    of the original full image has continuous image coordinates (pixel position) (r, c) = (0.0,0.0) , and the center
    of the upper left pixel has continuous image coordinates (r, c) = (0.5,0.5) . The first row of the original full
    image has discrete image row coordinate R = 0 , and corresponds to a range of continuous image row coordinates of
    r = [0,1) . The first column of the original full image has discrete image column coordinate C = 0 , and
    corresponds to a range of continuous image column coordinates of c = [0,1) . Thus, for example, continuous image
    coordinates (r, c) = (5.6,8.3) correspond to the sixth row and ninth column of the original full image, and
    discrete image coordinates (R,C) = (5,8).
    """

    def __init__(self, coordinate: Any = None):
        """
        Constructs an image coordinate from an x, y tuple. The tuple can be expressed as a List or any other
        structure that can be used to construct a Numpy array.

        TODO: Update type hint for coordinate to be ArrayLike once Numpy > 1.20
        :param coordinate: the x, y components
        """
        if coordinate is None:
            coordinate = [0.0, 0.0]

        if len(coordinate) != 2:
            raise ValueError("ImageCoordinates must have 2 components (x,y)")

        self.coordinate = np.array(coordinate, dtype=np.float64)

    @property
    def c(self) -> float:
        return self.coordinate[0]

    @c.setter
    def c(self, value: float) -> None:
        self.coordinate[0] = value

    @property
    def r(self) -> float:
        return self.coordinate[1]

    @r.setter
    def r(self, value: float) -> None:
        self.coordinate[1] = value

    @property
    def x(self) -> float:
        return self.c

    @x.setter
    def x(self, value: float) -> None:
        self.c = value

    @property
    def y(self) -> float:
        return self.r

    @y.setter
    def y(self, value: float) -> None:
        self.r = value
