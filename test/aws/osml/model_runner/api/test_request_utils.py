#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

import pytest

SAMPLE_REGION_REQUEST_DATA = {
    "tile_size": (10, 10),
    "tile_overlap": (1, 1),
    "tile_format": "NITF",
    "image_id": "test-image-id",
    "image_url": "test-image-url",
    "region_bounds": ((0, 0), (50, 50)),
    "model_name": "test-model-name",
    "model_invoke_mode": "SM_ENDPOINT",
    "output_bucket": "unit-test",
    "output_prefix": "region-request",
    "execution_role": "arn:aws:iam::012345678910:role/OversightMLBetaInvokeRole",
}


@pytest.fixture
def sample_request_data():
    from aws.osml.model_runner.api.region_request import RegionRequest

    return RegionRequest(SAMPLE_REGION_REQUEST_DATA)


def test_invalid_request_image_id(sample_request_data):
    from aws.osml.model_runner.api.request_utils import shared_properties_are_valid

    sample_request_data.image_id = ""
    assert not shared_properties_are_valid(sample_request_data)


def test_invalid_request_image_url(sample_request_data):
    from aws.osml.model_runner.api.request_utils import shared_properties_are_valid

    sample_request_data.image_url = ""
    assert not shared_properties_are_valid(sample_request_data)


def test_invalid_request_model_name(sample_request_data):
    from aws.osml.model_runner.api.request_utils import shared_properties_are_valid

    sample_request_data.model_name = ""
    assert not shared_properties_are_valid(sample_request_data)


def test_invalid_request_model_invoke_mode(sample_request_data):
    from aws.osml.model_runner.api.request_utils import shared_properties_are_valid

    sample_request_data.model_invoke_mode = None
    assert not shared_properties_are_valid(sample_request_data)


def test_invalid_request_tile_size(sample_request_data):
    from aws.osml.model_runner.api.request_utils import shared_properties_are_valid

    sample_request_data.tile_size = None
    assert not shared_properties_are_valid(sample_request_data)

    sample_request_data.tile_size = 0
    assert not shared_properties_are_valid(sample_request_data)

    sample_request_data.tile_size = (-1, 0)
    assert not shared_properties_are_valid(sample_request_data)

    sample_request_data.tile_size = (-1, -1)
    assert not shared_properties_are_valid(sample_request_data)


def test_invalid_tile_overlap(sample_request_data):
    from aws.osml.model_runner.api.request_utils import shared_properties_are_valid

    sample_request_data.tile_overlap = None
    assert not shared_properties_are_valid(sample_request_data)

    sample_request_data.tile_overlap = 0
    assert not shared_properties_are_valid(sample_request_data)

    sample_request_data.tile_overlap = (-1, 0)
    assert not shared_properties_are_valid(sample_request_data)

    sample_request_data.tile_overlap = (0, -1)
    assert not shared_properties_are_valid(sample_request_data)

    sample_request_data.tile_overlap = (-1, -1)
    assert not shared_properties_are_valid(sample_request_data)

    sample_request_data.tile_overlap = (10, 10)
    sample_request_data.tile_size = (5, 12)
    assert not shared_properties_are_valid(sample_request_data)

    sample_request_data.tile_overlap = (10, 10)
    sample_request_data.tile_size = (12, 5)
    assert not shared_properties_are_valid(sample_request_data)


def test_invalid_request_tile_format(sample_request_data):
    from aws.osml.model_runner.api.request_utils import shared_properties_are_valid

    sample_request_data.tile_format = None
    assert not shared_properties_are_valid(sample_request_data)


def test_invalid_request_image_read_role(sample_request_data):
    from aws.osml.model_runner.api.request_utils import shared_properties_are_valid

    sample_request_data.image_read_role = "012345678910:role/OversightMLS3ReadOnly"
    assert not shared_properties_are_valid(sample_request_data)


def test_invalid_request_model_invocation_role(sample_request_data):
    from aws.osml.model_runner.api.request_utils import shared_properties_are_valid

    sample_request_data.model_invocation_role = "012345678910:role/OversightMLModelInvoker"
    assert not shared_properties_are_valid(sample_request_data)


def test_shared_properties_invalid_tile_compression_returns_false(sample_request_data):
    """Test shared_properties_are_valid returns False for invalid tile_compression"""
    from aws.osml.model_runner.api.request_utils import shared_properties_are_valid

    # Arrange - set invalid tile_compression
    sample_request_data.tile_compression = "INVALID_COMPRESSION"

    # Act
    result = shared_properties_are_valid(sample_request_data)

    # Assert
    assert not result


def test_get_image_path_non_s3_returns_unchanged():
    """Test get_image_path returns local/network path unchanged (no vsis3 prefix)"""
    from aws.osml.model_runner.api.request_utils import get_image_path

    # Arrange - local file path
    local_path = "/local/data/image.tif"

    # Act
    result = get_image_path(local_path, assumed_role=None)

    # Assert - path unchanged (no s3 in path)
    assert result == local_path


def test_validate_image_path_with_assumed_role_uses_credentials(mocker):
    """Test validate_image_path uses assumed role credentials when provided"""
    from aws.osml.model_runner.api.request_utils import validate_image_path

    # Arrange
    image_url = "s3://test-bucket/test-image.tif"
    assumed_role = "arn:aws:iam::123456789012:role/TestRole"
    mock_credentials = {
        "AccessKeyId": "test-access-key",
        "SecretAccessKey": "test-secret-key",
        "SessionToken": "test-session-token",
    }

    mock_get_creds = mocker.patch("aws.osml.model_runner.api.request_utils.get_credentials_for_assumed_role")
    mock_boto_client = mocker.patch("aws.osml.model_runner.api.request_utils.boto3.client")
    mock_get_creds.return_value = mock_credentials
    mock_s3 = mocker.Mock()
    mock_s3.head_object.return_value = {}
    mock_boto_client.return_value = mock_s3

    # Act
    result = validate_image_path(image_url, assumed_role)

    # Assert - credentials used
    assert result
    mock_get_creds.assert_called_once_with(assumed_role)
    mock_boto_client.assert_called_once()
    call_kwargs = mock_boto_client.call_args[1]
    assert call_kwargs["aws_access_key_id"] == mock_credentials["AccessKeyId"]
    assert call_kwargs["aws_secret_access_key"] == mock_credentials["SecretAccessKey"]
    assert call_kwargs["aws_session_token"] == mock_credentials["SessionToken"]


def test_validate_image_path_raises_exception_for_missing_object(mocker):
    """Test validate_image_path raises InvalidS3ObjectException for missing object"""
    from botocore.exceptions import ClientError

    from aws.osml.model_runner.api.exceptions import InvalidS3ObjectException
    from aws.osml.model_runner.api.request_utils import validate_image_path

    # Arrange - mock S3 client to raise ClientError (404)
    image_url = "s3://test-bucket/missing-image.tif"

    mock_boto_client = mocker.patch("aws.osml.model_runner.api.request_utils.boto3.client")
    mock_s3 = mocker.Mock()
    mock_s3.head_object.side_effect = ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject")
    mock_boto_client.return_value = mock_s3

    # Act / Assert
    with pytest.raises(InvalidS3ObjectException) as context:
        validate_image_path(image_url, assumed_role=None)

    # Verify error message
    assert "does not exist" in str(context.value)
