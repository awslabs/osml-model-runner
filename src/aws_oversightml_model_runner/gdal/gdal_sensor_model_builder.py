from math import radians
from typing import List, Optional

from osgeo import gdal

from aws_oversightml_model_runner.photogrammetry import (
    GDALAffineSensorModel,
    GeodeticWorldCoordinate,
    ImageCoordinate,
    ProjectiveSensorModel,
    SensorModel,
)

from .sensor_model_builder import SensorModelBuilder


class GDALAffineSensorModelBuilder(SensorModelBuilder):
    """
    This builder is used to create sensor models for images that have GDAL geo transforms.
    """

    def __init__(self, geo_transform: List[float]):
        """
        Constructor for the builder accepting the required GDAL geotransform.
        :param geo_transform: the geotransform for this image
        """
        super().__init__()
        self.geo_transform = geo_transform

    def build(self) -> Optional[SensorModel]:
        """
        Use the GDAL GeoTransform to construct a sensor model.

        :return: an affine transform based SensorModel that uses the GDAL GeoTransform information provided
        """
        return GDALAffineSensorModel(self.geo_transform)


class GDALGCPSensorModelBuilder(SensorModelBuilder):
    """
    This builder is used to create sensor models for images that have GDAL ground control points (GCPs).
    """

    def __init__(self, ground_control_points: List[gdal.GCP]):
        """
        Constructor for the builder accepting the required GDAL GCPs.
        :param ground_control_points: the ground control points for this image
        """
        super().__init__()
        self.ground_control_points = ground_control_points

    def build(self) -> Optional[SensorModel]:
        """
        Use the GCPs to construct a projective sensor model.

        :return: a projective transform based SensorModel that uses the GDAL GCPs provided
        """
        if not self.ground_control_points or len(self.ground_control_points) < 4:
            return None

        world_coordinates = [
            GeodeticWorldCoordinate([radians(gcp.GCPX), radians(gcp.GCPY), gcp.GCPZ])
            for gcp in self.ground_control_points
        ]
        image_coordinates = [
            ImageCoordinate([gcp.GCPPixel, gcp.GCPLine]) for gcp in self.ground_control_points
        ]
        return ProjectiveSensorModel(world_coordinates, image_coordinates)
