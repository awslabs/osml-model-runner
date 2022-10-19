import logging
from typing import List, Optional
from xml.etree import ElementTree

from aws_oversightml_model_runner.photogrammetry import (
    ChippedImageSensorModel,
    GDALAffineSensorModel,
    ImageCoordinate,
    SensorModel,
)

from .gdal_sensor_model_builder import GDALAffineSensorModelBuilder
from .rpc_sensor_model_builder import RPCSensorModelBuilder
from .rsm_sensor_model_builder import RSMSensorModelBuilder
from .sensor_model_builder import SensorModelBuilder
from .xmltre_utils import get_tre_field_value


class SensorModelFactory:
    """
    This class encapsulates the logic necessary to construct SensorModels from imagery metadata parsed using GDAL.
    Users initialize the builder by providing whatever metadata is available and this class will decide how to create
    the most accurate SensorModel from the available information.
    """

    def __init__(
        self,
        xml_tres: Optional[ElementTree.Element] = None,
        geo_transform: Optional[List[float]] = None,
    ):
        """
        Construct a builder providing whatever metadata is available from the image. All of the parameters are named and
        optional allowing users to provide whatever they can and trusting that this builder will make use of as much of
        the information as possible.

        # TODO: Add an ElevationModel parameter to allow clients to specify a custom DEM to use with sensor models
        :param xml_tres: the parsed XML representing metadata organized in the tagged record extensions (TRE) format
        :param geo_transform: a GDAL affine transform
        """
        super().__init__()
        self.xml_tres = xml_tres
        self.geo_transform = geo_transform
        self.builders: List[SensorModelBuilder] = []
        if xml_tres is not None:
            self.builders.append(RSMSensorModelBuilder(xml_tres))
            self.builders.append(RPCSensorModelBuilder(xml_tres))
            # TODO: Add 4-Corner Support: self.builders.append(CornerInterpolationSensorModelBuilder(xml_tres)
        if geo_transform is not None:
            self.builders.append(GDALAffineSensorModelBuilder(geo_transform))

    def build(self) -> Optional[SensorModel]:
        """
        Constructs the sensor model from the available information. Note that in cases where not enough information is
        available to provide any solution this method will return None.

        :return: the highest quality sensor model available given the information provided
        """
        for sensor_model_builder in self.builders:
            sensor_model = sensor_model_builder.build()
            if sensor_model is not None:
                return self.wrap_sensor_model_if_necessary(sensor_model)
        return None

    def wrap_sensor_model_if_necessary(self, sensor_model: SensorModel) -> SensorModel:
        """
        This method creates any additional wrappers around the original sensor model necessary to cover special cases
        (e.g. chipped imagery).

        :param sensor_model: the original sensor model
        :return: a composite sensor model
        """

        if self.xml_tres is None or isinstance(sensor_model, GDALAffineSensorModel):
            return sensor_model

        result = sensor_model

        # Check to see if this image is a chip from a larger image. Chipped images will have an ICHIPB TRE in
        # the metadata.
        ichipb_tre = self.xml_tres.find("./tre[@name='ICHIPB']")
        if ichipb_tre is not None:

            try:
                # Loop through the Output Product (OP) and Full Image (FI) fields in the ICHIPB TRE and construct
                # the corresponding image coordinates needed to create a chipped sensor model.
                full_image_coordinates = []
                chipped_image_coordinates = []
                for grid_point in ["11", "12", "21", "22"]:
                    op_col = get_tre_field_value(ichipb_tre, f"OP_COL_{grid_point}", float)
                    op_row = get_tre_field_value(ichipb_tre, f"OP_ROW_{grid_point}", float)
                    fi_col = get_tre_field_value(ichipb_tre, f"FI_COL_{grid_point}", float)
                    fi_row = get_tre_field_value(ichipb_tre, f"FI_ROW_{grid_point}", float)
                    full_image_coordinates.append(ImageCoordinate([fi_col, fi_row]))
                    chipped_image_coordinates.append(ImageCoordinate([op_col, op_row]))

                # Construct a chipped sensor model that will wrap the original sensor model. This wrapper will
                # convert the chipped image coordinates to full image coordinates before they are passed to the
                # original sensor model which assumes it is operating over the full image.
                result = ChippedImageSensorModel(
                    full_image_coordinates, chipped_image_coordinates, sensor_model
                )
            except ValueError as ve:
                logging.warning(
                    "Unable to parse ICHIPB TRE found in XML metadata. SensorModel is unchanged."
                )
                logging.warning(str(ve))

        return result
