import pytest
from data.sample_request_data import SAMPLE_REGION_REQUEST_DATA

from aws_oversightml_model_runner.sqs.request_utils import shared_properties_are_valid


@pytest.fixture
def test_request_data():
    from aws_oversightml_model_runner.sqs.region_request import RegionRequest

    return RegionRequest(SAMPLE_REGION_REQUEST_DATA)


def test_invalid_request_image_id(test_request_data):
    test_request_data.image_id = ""
    assert not shared_properties_are_valid(test_request_data)


def test_invalid_request_image_url(test_request_data):
    test_request_data.image_url = ""
    assert not shared_properties_are_valid(test_request_data)


def test_invalid_request_model_name(test_request_data):
    test_request_data.model_name = ""
    assert not shared_properties_are_valid(test_request_data)


def test_invalid_request_model_hosting_type(test_request_data):
    test_request_data.model_hosting_type = None
    assert not shared_properties_are_valid(test_request_data)


def test_invalid_request_tile_size(test_request_data):
    test_request_data.tile_size = None
    assert not shared_properties_are_valid(test_request_data)

    test_request_data.tile_size = 0
    assert not shared_properties_are_valid(test_request_data)

    test_request_data.tile_size = (-1, 0)
    assert not shared_properties_are_valid(test_request_data)

    test_request_data.tile_size = (-1, -1)
    assert not shared_properties_are_valid(test_request_data)


def test_invalid_tile_overlap(test_request_data):
    test_request_data.tile_overlap = None
    assert not shared_properties_are_valid(test_request_data)

    test_request_data.tile_overlap = 0
    assert not shared_properties_are_valid(test_request_data)

    test_request_data.tile_overlap = (-1, 0)
    assert not shared_properties_are_valid(test_request_data)

    test_request_data.tile_overlap = (0, -1)
    assert not shared_properties_are_valid(test_request_data)

    test_request_data.tile_overlap = (-1, -1)
    assert not shared_properties_are_valid(test_request_data)

    test_request_data.tile_overlap = (10, 10)
    test_request_data.tile_size = (5, 12)
    assert not shared_properties_are_valid(test_request_data)

    test_request_data.tile_overlap = (10, 10)
    test_request_data.tile_size = (12, 5)
    assert not shared_properties_are_valid(test_request_data)


def test_invalid_request_tile_format(test_request_data):
    test_request_data.tile_format = None
    assert not shared_properties_are_valid(test_request_data)


def test_invalid_request_image_read_role(test_request_data):
    test_request_data.image_read_role = "010321660603:role/OversightMLS3ReadOnly"
    assert not shared_properties_are_valid(test_request_data)


def test_invalid_request_model_invocation_role(test_request_data):
    test_request_data.model_invocation_role = "010321660603:role/OversightMLModelInvoker"
    assert not shared_properties_are_valid(test_request_data)
