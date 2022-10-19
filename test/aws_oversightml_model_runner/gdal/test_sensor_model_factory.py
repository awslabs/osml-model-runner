import xml.etree.ElementTree as ElementTree
from math import radians

import numpy as np
import pytest

from aws_oversightml_model_runner.gdal.sensor_model_factory import SensorModelFactory
from aws_oversightml_model_runner.photogrammetry import (
    GeodeticWorldCoordinate,
    ImageCoordinate,
    RPCSensorModel,
    RSMPolynomialSensorModel,
)

# Strictly speaking the tests in this file are not pure unit tests of the SensorModelFactory. Here we deliberately
# did not mock out the underlying sensor models and are instead testing that a fully functional model can be
# constructed from the example metadata provided. Small integration tests like these, run as part of the automated
# unit testing, will ensure that the complex sensor models perform correctly when exposed to real metadata. These
# tests do not have any dependencies on external infrastructure and should run fast enough that we shouldn't need
# to break them out from the other automated tests.


def test_sensor_model_builder_ms_rpc00b():

    with open("test/data/sample-metadata-ms-rpc00b.xml", "rb") as xml_file:
        xml_tres = ElementTree.parse(xml_file)
        sensor_model_builder = SensorModelFactory(xml_tres=xml_tres)
        sensor_model = sensor_model_builder.build()
        assert sensor_model is not None
        assert isinstance(sensor_model, RPCSensorModel)

        # These are the corner coordinates taken from the CSCRNA TRE. The represent the corners of the "intelligent
        # pixels" which are the pixels that actually contain visual information (i.e. not padding pixels). A more
        # complete definition is in STDI-0002 Volume 3 Appendix B. Note that the image coordinates are not the full
        # image size but the location of the corners without the padding.
        ulcorner_world_coordinate = GeodeticWorldCoordinate(
            [radians(121.48749), radians(25.02860), 27.1]
        )
        ulcorner_image_coordinate = sensor_model.world_to_image(ulcorner_world_coordinate)
        assert np.allclose(ulcorner_image_coordinate.coordinate, np.array([0.0, 0.0]), atol=1.0)

        urcorner_world_coordinate = GeodeticWorldCoordinate(
            [radians(121.68566), radians(25.01000), 234.7]
        )
        urcorner_image_coordinate = sensor_model.world_to_image(urcorner_world_coordinate)
        assert np.allclose(urcorner_image_coordinate.coordinate, np.array([8819.0, 0.0]), atol=1.0)

        lrcorner_world_coordinate = GeodeticWorldCoordinate(
            [radians(121.68595), radians(24.91148), 403.1]
        )
        lrcorner_image_coordinate = sensor_model.world_to_image(lrcorner_world_coordinate)
        assert np.allclose(
            lrcorner_image_coordinate.coordinate, np.array([8819.0, 5211.0]), atol=1.0
        )

        llcorner_world_coordinate = GeodeticWorldCoordinate(
            [radians(121.48975), radians(24.92772), 431.4]
        )
        llcorner_image_coordinate = sensor_model.world_to_image(llcorner_world_coordinate)
        assert np.allclose(llcorner_image_coordinate.coordinate, np.array([0.0, 5211.0]), atol=1.0)


def test_sensor_model_builder_rsmpca():
    with open("test/data/i_6130a_truncated_tres.xml") as xml_file:
        xml_tres = ElementTree.parse(xml_file)
        sensor_model_builder = SensorModelFactory(xml_tres=xml_tres)
        sensor_model = sensor_model_builder.build()
        assert sensor_model is not None
        assert isinstance(sensor_model, RSMPolynomialSensorModel)

        geodetic_ground_domain_origin = GeodeticWorldCoordinate(
            [radians(-117.03881), radians(33.16173), -6.7]
        )
        image_ground_domain_origin = sensor_model.world_to_image(geodetic_ground_domain_origin)
        assert image_ground_domain_origin.x == pytest.approx(0.5, abs=1.0)
        assert image_ground_domain_origin.y == pytest.approx(0.5, abs=1.0)

        new_geodetic_ground_domain_origin = sensor_model.image_to_world(ImageCoordinate([0.5, 0.5]))
        assert new_geodetic_ground_domain_origin.longitude == pytest.approx(
            geodetic_ground_domain_origin.longitude, abs=0.000001
        )
        assert new_geodetic_ground_domain_origin.latitude == pytest.approx(
            geodetic_ground_domain_origin.latitude, abs=0.000001
        )
