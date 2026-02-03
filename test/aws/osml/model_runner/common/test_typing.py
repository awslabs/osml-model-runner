#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

from aws.osml.model_runner.common.typing import (
    VALID_IMAGE_COMPRESSION,
    VALID_IMAGE_FORMATS,
    GeojsonDetectionField,
    ImageCompression,
    ImageFormats,
    RequestStatus,
    TileState,
)


def test_request_status_values():
    """
    Test RequestStatus values are auto-generated strings.
    """
    assert RequestStatus.STARTED.value == "STARTED"
    assert RequestStatus.IN_PROGRESS.value == "IN_PROGRESS"
    assert RequestStatus.PARTIAL.value == "PARTIAL"
    assert RequestStatus.SUCCESS.value == "SUCCESS"
    assert RequestStatus.FAILED.value == "FAILED"


def test_image_compression_values_and_valid_list():
    """
    Test ImageCompression values and valid list consistency.
    """
    values = [item.value for item in ImageCompression]
    assert values == VALID_IMAGE_COMPRESSION
    assert ImageCompression.NONE.value == "NONE"
    assert ImageCompression.JPEG.value == "JPEG"
    assert ImageCompression.J2K.value == "J2K"
    assert ImageCompression.LZW.value == "LZW"


def test_image_formats_values_and_valid_list():
    """
    Test ImageFormats values and valid list consistency.
    """
    values = [item.value for item in ImageFormats]
    assert values == VALID_IMAGE_FORMATS
    assert ImageFormats.NITF.value == "NITF"
    assert ImageFormats.JPEG.value == "JPEG"
    assert ImageFormats.PNG.value == "PNG"
    assert ImageFormats.GTIFF.value == "GTIFF"


def test_geojson_detection_field_values():
    """
    Test GeojsonDetectionField string values.
    """
    assert GeojsonDetectionField.BOUNDS.value == "bounds_imcoords"
    assert GeojsonDetectionField.GEOM.value == "geom_imcoords"


def test_tile_state_values():
    """
    Test TileState string values.
    """
    assert TileState.SUCCEEDED.value == "succeeded"
    assert TileState.FAILED.value == "failed"
