#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

import pytest

from aws.osml.model_runner.api.region_request import ModelInvokeMode, RegionRequest
from aws.osml.model_runner.common import ImageFormats


@pytest.fixture
def sample_request_data():
    """
    Set up sample data for each test.
    """
    return {
        "tile_size": (10, 10),
        "tile_overlap": (1, 1),
        "tile_format": "NITF",
        "image_id": "test-image-id",
        "image_url": "test-image-url",
        "region_bounds": ((0, 0), (50, 50)),
        "model_name": "test-model-name",
        "model_invoke_mode": ModelInvokeMode.SM_ENDPOINT,
        "execution_role": "arn:aws:iam::012345678910:role/OversightMLBetaInvokeRole",
    }


def test_invalid_data(sample_request_data):
    """
    Test RegionRequest with missing or invalid image_id.
    """
    rr = RegionRequest(sample_request_data, image_id="")
    assert not rr.is_valid()


def test_invalid_region_bounds(sample_request_data):
    """
    Test RegionRequest with missing or invalid region_bounds.
    """
    rr = RegionRequest(sample_request_data, region_bounds=None)
    assert not rr.is_valid()


def test_valid_data(sample_request_data):
    """
    Test RegionRequest with valid data to ensure it passes validation.
    """
    rr = RegionRequest(sample_request_data)
    assert rr.is_valid()


def test_default_initialization():
    """
    Test RegionRequest default initialization to ensure default values are set correctly.
    """
    rr = RegionRequest()
    assert rr.tile_size == (1024, 1024)
    assert rr.tile_overlap == (50, 50)
    assert rr.tile_format == ImageFormats.NITF
    assert rr.model_invoke_mode == ModelInvokeMode.NONE


def test_custom_tile_size(sample_request_data):
    """
    Test RegionRequest with a custom tile size to verify correct initialization.
    """
    custom_tile_size = (256, 256)
    rr = RegionRequest(sample_request_data, tile_size=custom_tile_size)
    assert rr.tile_size == custom_tile_size


def test_region_request_initialization_from_data():
    """
    Test initializing a RegionRequest with a variety of attributes to ensure they are set correctly.
    """
    rr = RegionRequest(
        region_id="test-region-id",
        image_id="test-image-id",
        image_url="test-image-url",
        tile_format=ImageFormats.NITF,
        region_bounds=((10, 10), (100, 100)),
    )
    assert rr.region_id == "test-region-id"
    assert rr.image_id == "test-image-id"
    assert rr.image_url == "test-image-url"
    assert rr.tile_format == ImageFormats.NITF
    assert rr.region_bounds == ((10, 10), (100, 100))
