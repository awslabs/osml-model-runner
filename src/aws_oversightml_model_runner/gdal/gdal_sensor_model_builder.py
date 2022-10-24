from typing import List, Optional

from aws_oversightml_model_runner.photogrammetry import GDALAffineSensorModel, SensorModel

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
