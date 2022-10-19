from math import radians

import numpy as np
import pytest

from aws_oversightml_model_runner.photogrammetry import (
    GeodeticWorldCoordinate,
    RSMContext,
    RSMGroundDomain,
    RSMGroundDomainForm,
    RSMImageDomain,
    RSMLowOrderPolynomial,
    RSMPolynomial,
    RSMPolynomialSensorModel,
    RSMSectionedPolynomialSensorModel,
    WorldCoordinate,
    geodetic_to_geocentric,
)


@pytest.fixture
def sample_image_domain():
    return RSMImageDomain(0, 2048, 10, 2038)


@pytest.fixture
def sample_geodetic_ground_domain():
    ground_domain_vertices = [
        GeodeticWorldCoordinate([radians(0.0), radians(10.0), -100.0]),
        GeodeticWorldCoordinate([radians(0.0), radians(0.0), -100.0]),
        GeodeticWorldCoordinate([radians(10.0), radians(10.0), -100.0]),
        GeodeticWorldCoordinate([radians(10.0), radians(0.0), -100.0]),
        GeodeticWorldCoordinate([radians(0.0), radians(10.0), 100.0]),
        GeodeticWorldCoordinate([radians(0.0), radians(0.0), 100.0]),
        GeodeticWorldCoordinate([radians(10.0), radians(10.0), 100.0]),
        GeodeticWorldCoordinate([radians(10.0), radians(0.0), 100.0]),
    ]
    return RSMGroundDomain(RSMGroundDomainForm.GEODETIC, ground_domain_vertices)


@pytest.fixture
def sample_rectangular_ground_domain():

    geodetic_coordinate_origin = GeodeticWorldCoordinate([radians(5.0), radians(10.0), 0.0])
    rectangular_coordinate_origin = geodetic_to_geocentric(geodetic_coordinate_origin)

    ground_domain_vertices = [
        WorldCoordinate(np.add(rectangular_coordinate_origin.coordinate, [0.0, 10.0, -100.0])),
        WorldCoordinate(np.add(rectangular_coordinate_origin.coordinate, [0.0, 0.0, -100.0])),
        WorldCoordinate(np.add(rectangular_coordinate_origin.coordinate, [10.0, 10.0, -100.0])),
        WorldCoordinate(np.add(rectangular_coordinate_origin.coordinate, [10.0, 0.0, -100.0])),
        WorldCoordinate(np.add(rectangular_coordinate_origin.coordinate, [0.0, 10.0, 100.0])),
        WorldCoordinate(np.add(rectangular_coordinate_origin.coordinate, [0.0, 0.0, 100.0])),
        WorldCoordinate(np.add(rectangular_coordinate_origin.coordinate, [10.0, 10.0, 100.0])),
        WorldCoordinate(np.add(rectangular_coordinate_origin.coordinate, [10.0, 0.0, 100.0])),
    ]

    rectangular_coordinate_unit_vectors = [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]]
    return RSMGroundDomain(
        RSMGroundDomainForm.RECTANGULAR,
        ground_domain_vertices,
        rectangular_coordinate_origin,
        rectangular_coordinate_unit_vectors,
    )


@pytest.fixture
def sample_polynomial_sensor_model(sample_geodetic_ground_domain, sample_image_domain):
    context = RSMContext(sample_geodetic_ground_domain, sample_image_domain)
    coln = RSMPolynomial(1, 1, 1, [0.0, 100.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
    cold = RSMPolynomial(0, 0, 0, [1.0])
    rown = RSMPolynomial(1, 1, 1, [0.0, 0.0, 100.0, 0.0, 0.0, 0.0, 0.0, 0.0])
    rowd = RSMPolynomial(0, 0, 0, [1.0])

    return RSMPolynomialSensorModel(
        context, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, rown, rowd, coln, cold
    )


@pytest.fixture
def sample_sectioned_polynomial_sensor_model(sample_geodetic_ground_domain, sample_image_domain):
    context = RSMContext(sample_geodetic_ground_domain, sample_image_domain)

    col_poly = RSMLowOrderPolynomial([0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
    row_poly = RSMLowOrderPolynomial([0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])

    coln = RSMPolynomial(1, 1, 1, [0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
    cold = RSMPolynomial(0, 0, 0, [1.0])
    rown = RSMPolynomial(1, 1, 1, [0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0])
    rowd = RSMPolynomial(0, 0, 0, [1.0])
    identity_polynomial_sensor_model = RSMPolynomialSensorModel(
        context, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, rown, rowd, coln, cold
    )

    return RSMSectionedPolynomialSensorModel(
        context,
        2,
        1,
        1024,
        1024,
        row_poly,
        col_poly,
        [[identity_polynomial_sensor_model], [identity_polynomial_sensor_model]],
    )


def test_image_domain(sample_image_domain):
    assert sample_image_domain.min_row == 0
    assert sample_image_domain.max_row == 2048
    assert sample_image_domain.min_column == 10
    assert sample_image_domain.max_column == 2038


def test_geodetic_ground_domain(sample_geodetic_ground_domain):
    assert len(sample_geodetic_ground_domain.ground_domain_vertices) == 8
    assert np.array_equal(
        sample_geodetic_ground_domain.ground_domain_vertices[0].coordinate,
        np.array([radians(0.0), radians(10.0), -100.0]),
    )
    assert np.array_equal(
        sample_geodetic_ground_domain.ground_domain_vertices[7].coordinate,
        np.array([radians(10.0), radians(0.0), 100.0]),
    )

    world_coordinate = GeodeticWorldCoordinate([radians(5.0), radians(5.0), 50.0])
    domain_coordinate = sample_geodetic_ground_domain.geodetic_to_ground_domain_coordinate(
        world_coordinate
    )
    assert np.array_equal(
        domain_coordinate.coordinate, np.array([radians(5.0), radians(5.0), 50.0])
    )


def test_rectangular_ground_domain(sample_rectangular_ground_domain):
    world_coordinate = GeodeticWorldCoordinate([radians(5.0), radians(10.0), 0.0])
    domain_coordinate = sample_rectangular_ground_domain.geodetic_to_ground_domain_coordinate(
        world_coordinate
    )
    assert np.array_equal(domain_coordinate.coordinate, np.array([0.0, 0.0, 0.0]))

    new_world_coordinate = sample_rectangular_ground_domain.ground_domain_coordinate_to_geodetic(
        domain_coordinate
    )
    assert world_coordinate.longitude == pytest.approx(new_world_coordinate.longitude, abs=0.000001)
    assert world_coordinate.latitude == pytest.approx(new_world_coordinate.latitude, abs=0.000001)
    assert world_coordinate.elevation == pytest.approx(new_world_coordinate.elevation, abs=0.1)

    # TODO: More Testing!!!


def test_rsmpolynomial_eval():
    polynomial = RSMPolynomial(
        2,
        1,
        1,
        [
            1.0,  # constant
            1.0,  # X
            0.0,  # XX
            2.0,  # Y
            0.0,  # XY
            0.0,  # XXY
            3.0,  # Z
            0.0,  # XZ
            0.0,  # XXZ
            0.0,  # YZ
            0.0,  # XYZ
            1.0,  # XXYZ
        ],
    )

    world_coordinate = WorldCoordinate([10, 20, 30])
    assert polynomial(world_coordinate) == 1.0 + 10.0 + 40.0 + 90.0 + (100.0 * 20.0 * 30.0)


def test_rsmloworderpolynomial_eval():
    low_order_polynomial = RSMLowOrderPolynomial(
        [
            42.0,  # constant
            1.0,  # X
            1.0,  # Y
            1.0,  # Z
            0.0,  # XX
            0.0,  # XY
            2.0,  # XZ
            0.0,  # YY
            0.0,  # YZ
            3.0,  # ZZ
        ]
    )

    world_coordinate = WorldCoordinate([1.0, 2.0, 3.0])
    assert low_order_polynomial(world_coordinate) == 42.0 + 1.0 + 2.0 + 3.0 + 6.0 + 27.0


def test_polynomial_sensor_model(sample_polynomial_sensor_model):
    world_coordinate = GeodeticWorldCoordinate([radians(5.0), radians(5.0), 0.0])
    image_coordinate = sample_polynomial_sensor_model.world_to_image(world_coordinate)

    assert np.array_equal(
        image_coordinate.coordinate, np.array([100.0 * radians(5.0), 100.0 * radians(5.0)])
    )
    new_world_coordinate = sample_polynomial_sensor_model.image_to_world(image_coordinate)

    assert np.array_equal(world_coordinate.coordinate, new_world_coordinate.coordinate)


def test_segmented_polynomial_sensor_model(sample_sectioned_polynomial_sensor_model):

    world_coordinate = GeodeticWorldCoordinate([radians(5.0), radians(5.0), 0.0])
    image_coordinate = sample_sectioned_polynomial_sensor_model.world_to_image(world_coordinate)
    assert np.allclose(image_coordinate.coordinate, np.array([radians(5.0), radians(5.0)]))
    new_world_coordinate = sample_sectioned_polynomial_sensor_model.image_to_world(image_coordinate)
    assert np.array_equal(world_coordinate.coordinate, new_world_coordinate.coordinate)
