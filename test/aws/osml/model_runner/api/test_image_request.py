#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

import boto3
import pytest
from botocore.stub import Stubber
from dacite import exceptions

from aws.osml.model_runner.api import InvalidS3ObjectException
from aws.osml.model_runner.api.image_request import ImageRequest, ModelInvokeMode
from aws.osml.model_runner.api.request_utils import validate_image_path
from aws.osml.model_runner.app_config import BotoConfig
from aws.osml.model_runner.common import (
    FeatureDistillationNMS,
    FeatureDistillationSoftNMS,
    MRPostProcessing,
    MRPostprocessingStep,
)
from aws.osml.model_runner.sink import Sink, SinkFactory


@pytest.fixture
def image_request():
    """
    Helper fixture to build sample request data for tests.
    """
    return ImageRequest(
        job_id="test-job-id",
        image_id="test-image-id",
        image_url="test-image-url",
        image_read_role="arn:aws:iam::012345678910:role/TestRole",
        outputs=[
            {"type": "S3", "bucket": "test-bucket", "prefix": "test-bucket-prefix"},
            {"type": "Kinesis", "stream": "test-stream", "batchSize": 1000},
        ],
        tile_size=(1024, 1024),
        tile_overlap=(50, 50),
        tile_format="NITF",
        model_name="test-model-name",
        model_invoke_mode=ModelInvokeMode.SM_ENDPOINT,
        model_invocation_role="arn:aws:iam::012345678910:role/TestRole",
    )


def test_invalid_data(image_request):
    """
    Test ImageRequest with missing or invalid image_id.
    """
    image_request.image_id = None
    assert not image_request.is_valid()


def test_invalid_job_id(image_request):
    """
    Test ImageRequest with missing job_id.
    """
    image_request.job_id = None
    assert not image_request.is_valid()


def test_valid_data(image_request):
    """
    Test ImageRequest with valid data to ensure it passes validation.
    """
    assert image_request.is_valid()


def test_valid_data_without_outputs(image_request):
    """
    Test ImageRequest is valid when outputs are empty.
    """
    image_request.outputs = []
    assert image_request.is_valid()


def test_is_valid_with_empty_outputs_branch(mocker):
    """
    Test is_valid returns True when outputs are empty and shared properties are valid.
    """
    mocker.patch("aws.osml.model_runner.api.image_request.shared_properties_are_valid", return_value=True)
    ir = ImageRequest(
        job_id="job",
        image_id="image-id",
        image_url="image-url",
        model_name="model",
        model_invoke_mode=ModelInvokeMode.SM_ENDPOINT,
        tile_size=(10, 10),
        tile_overlap=(0, 0),
        tile_format="NITF",
    )
    ir.outputs = []
    assert ir.is_valid()


def test_invalid_tile_size(image_request):
    """
    Test ImageRequest with invalid tile size to check error handling.
    """
    image_request.tile_size = None
    assert not image_request.is_valid()


def test_from_external_message_zero_tile_dimensions_int():
    """Test zero tile dimensions as integers matching SQS format"""
    message = {
        "jobId": "test-job-id",
        "imageUrls": ["test-image-url"],
        "imageProcessor": {"name": "test-model", "type": "SM_ENDPOINT"},
        "imageProcessorTileSize": 512,
        "imageProcessorTileOverlap": 0,
        "outputs": [{"type": "S3", "bucket": "test-bucket", "prefix": "test-prefix"}],
    }
    ir = ImageRequest.from_external_message(message)
    assert ir.tile_size == (512, 512)
    assert ir.tile_overlap == (0, 0)
    assert ir.is_valid()


def test_from_external_message_zero_tile_dimensions_string():
    """Test zero tile dimensions as strings"""
    message = {
        "jobId": "test-job-id",
        "imageUrls": ["test-image-url"],
        "imageProcessor": {"name": "test-model", "type": "SM_ENDPOINT"},
        "imageProcessorTileSize": "512",
        "imageProcessorTileOverlap": "0",
        "outputs": [{"type": "S3", "bucket": "test-bucket", "prefix": "test-prefix"}],
    }
    ir = ImageRequest.from_external_message(message)
    assert ir.tile_size == (512, 512)
    assert ir.tile_overlap == (0, 0)


def test_from_external_message_invalid_tile_dimensions():
    """Test invalid tile dimensions"""
    message = {
        "jobId": "test-job-id",
        "imageUrls": ["test-image-url"],
        "imageProcessor": {"name": "test-model", "type": "SM_ENDPOINT"},
        "imageProcessorTileSize": "invalid",
        "imageProcessorTileOverlap": None,
        "outputs": [{"type": "S3", "bucket": "test-bucket", "prefix": "test-prefix"}],
    }
    # Should evalute to None
    with pytest.raises(exceptions.WrongTypeError, match="wrong value type"):
        ir = ImageRequest.from_external_message(message)
        assert ir.tile_size is None
        assert ir.tile_overlap is None


def test_from_external_message():
    """
    Test ImageRequest created from external message deserialization.
    """
    ir = ImageRequest.from_external_message(
        {
            "jobName": "test-job-name",
            "jobId": "test-job-id",
            "imageUrls": ["test-image-url"],
            "outputs": [
                {"type": "S3", "bucket": "test-bucket", "prefix": "test-bucket-prefix"},
                {"type": "Kinesis", "stream": "test-stream", "batchSize": 1000},
            ],
            "imageProcessor": {"name": "test-model", "type": "SM_ENDPOINT"},
            "imageProcessorTileSize": 1024,
            "imageProcessorTileOverlap": 50,
        }
    )
    assert ir.is_valid()
    assert ir.job_id == "test-job-id"
    assert ir.model_name == "test-model"
    assert ir.tile_size == (1024, 1024)
    assert ir.tile_overlap == (50, 50)


def test_default_initialization():
    """
    Test ImageRequest default initialization to ensure default values are set correctly.
    """
    ir = ImageRequest()
    assert ir.tile_size == (1024, 1024)
    assert ir.tile_overlap == (50, 50)
    assert ir.tile_format == "NITF"
    assert ir.model_invoke_mode == ModelInvokeMode.NONE
    assert ir.get_shared_values()["model_endpoint_parameters"] is None


def test_feature_distillation_parsing(image_request):
    """
    Test that ImageRequest can correctly parse and handle feature distillation options.
    """
    distillation_option = image_request.get_feature_distillation_option()
    assert isinstance(distillation_option, list)
    assert len(distillation_option) == 1


def test_feature_distillation_multiple_options_invalid(image_request):
    """
    Test that multiple feature distillation options are rejected.
    """
    image_request.post_processing = [
        MRPostProcessing(step=MRPostprocessingStep.FEATURE_DISTILLATION, algorithm=FeatureDistillationNMS()),
        MRPostProcessing(step=MRPostprocessingStep.FEATURE_DISTILLATION, algorithm=FeatureDistillationSoftNMS()),
    ]
    assert not image_request.is_valid()


def test_image_request_from_minimal_message_legacy_output():
    """
    Test ImageRequest creation from a minimal message using legacy output fields.
    """
    ir = ImageRequest.from_external_message(
        {
            "jobName": "test-job-name",
            "jobId": "test-job-id",
            "imageUrls": ["test-image-url"],
            "imageProcessor": {"name": "test-model", "type": "SM_ENDPOINT"},
            "imageProcessorTileSize": 1024,
            "imageProcessorTileOverlap": 50,
            "outputBucket": "test-bucket",
            "outputPrefix": "images/outputs",
        }
    )

    assert ir.is_valid()
    assert len(ir.outputs) == 1

    # Check S3 Sink creation from outputs
    sinks = SinkFactory.outputs_to_sinks(ir.outputs)
    s3_sink: Sink = sinks[0]
    assert s3_sink.name() == "S3"
    assert getattr(s3_sink, "bucket") == "test-bucket"
    assert getattr(s3_sink, "prefix") == "images/outputs"


def test_image_request_invalid_sink():
    """
    Test ImageRequest creation with an invalid sink type.
    """
    request = ImageRequest.from_external_message(
        {
            "jobName": "test-job-name",
            "jobId": "test-job-id",
            "imageUrls": ["test-image-url"],
            "outputs": [{"type": "SQS", "queue": "FakeQueue"}],
            "imageProcessor": {"name": "test-model", "type": "SM_ENDPOINT"},
            "imageProcessorTileSize": 1024,
            "imageProcessorTileOverlap": 50,
        }
    )

    # Should fail with an invalid sync type provided
    assert not request.is_valid()


def test_image_request_invalid_roles(image_request):
    """
    Test invalid role formats are rejected.
    """
    image_request.image_read_role = "not-an-arn"
    assert not image_request.is_valid()

    # Create a fresh fixture instance for the second test
    ir2 = ImageRequest(
        job_id="test-job-id",
        image_id="test-image-id",
        image_url="test-image-url",
        outputs=[{"type": "S3", "bucket": "test-bucket", "prefix": "test-bucket-prefix"}],
        tile_size=(1024, 1024),
        tile_overlap=(50, 50),
        tile_format="NITF",
        model_name="test-model-name",
        model_invoke_mode=ModelInvokeMode.SM_ENDPOINT,
    )
    ir2.model_invocation_role = "not-an-arn"
    assert not ir2.is_valid()


def test_image_request_invalid_image_path():
    """
    Test validation of an invalid S3 image path.
    """
    s3_client = boto3.client("s3", config=BotoConfig.default)
    s3_client_stub = Stubber(s3_client)
    s3_client_stub.activate()

    image_path = "s3://test-results-bucket/test/data/small.ntf"

    s3_client_stub.add_client_error(
        "head_object",
        service_error_code="404",
        service_message="Not Found",
        expected_params={"Bucket": image_path},
    )

    with pytest.raises(InvalidS3ObjectException):
        validate_image_path(image_path, None)

    s3_client_stub.deactivate()


def test_parse_model_endpoint_parameters_valid():
    """
    Test parsing of valid model endpoint parameters.
    """
    model_params = {"TargetVariant": "version-1", "CustomAttributes": "custom-attributes"}
    request = ImageRequest.from_external_message(
        {
            "jobName": "test-job-name",
            "jobId": "test-job-id",
            "imageUrls": ["test-image-url"],
            "outputs": [{"type": "SQS", "queue": "FakeQueue"}],
            "imageProcessor": {"name": "test-model", "type": "SM_ENDPOINT"},
            "imageProcessorTileSize": 1024,
            "imageProcessorTileOverlap": 50,
            "imageProcessorParameters": model_params,
        }
    )
    assert request.model_endpoint_parameters == model_params


def test_parse_model_endpoint_parameters_none():
    """
    Test parsing when no model endpoint parameters are provided.
    """
    request = ImageRequest.from_external_message(
        {
            "jobName": "test-job-name",
            "jobId": "test-job-id",
            "imageUrls": ["test-image-url"],
            "outputs": [{"type": "SQS", "queue": "FakeQueue"}],
            "imageProcessor": {"name": "test-model", "type": "SM_ENDPOINT"},
            "imageProcessorTileSize": 1024,
            "imageProcessorTileOverlap": 50,
        }
    )
    assert request.model_endpoint_parameters is None


def test_parse_model_endpoint_parameters_invalid(caplog):
    """
    Test parsing of invalid model endpoint parameters.
    """
    invalid_params = "not a dictionary"

    with caplog.at_level("INFO"):
        request = ImageRequest.from_external_message(
            {
                "jobName": "test-job-name",
                "jobId": "test-job-id",
                "imageUrls": ["test-image-url"],
                "outputs": [{"type": "SQS", "queue": "FakeQueue"}],
                "imageProcessor": {"name": "test-model", "type": "SM_ENDPOINT"},
                "imageProcessorTileSize": 1024,
                "imageProcessorTileOverlap": 50,
                "imageProcessorParameters": invalid_params,
            }
        )

        assert request.model_endpoint_parameters is None
        assert "Invalid model endpoint parameters dictionary" in caplog.text


def test_parse_model_endpoint_parameters_empty_dict():
    """
    Test parsing of empty dictionary for model endpoint parameters.
    """
    empty_params = {}

    request = ImageRequest.from_external_message(
        {
            "jobName": "test-job-name",
            "jobId": "test-job-id",
            "imageUrls": ["test-image-url"],
            "outputs": [{"type": "SQS", "queue": "FakeQueue"}],
            "imageProcessor": {"name": "test-model", "type": "SM_ENDPOINT"},
            "imageProcessorTileSize": 1024,
            "imageProcessorTileOverlap": 50,
            "imageProcessorParameters": empty_params,
        }
    )

    assert request.model_endpoint_parameters == empty_params


def test_parse_tile_dimension():
    """
    Test parsing tile dimension helper.
    """
    assert ImageRequest._parse_tile_dimension("64") == (64, 64)
    assert ImageRequest._parse_tile_dimension(32) == (32, 32)
    assert ImageRequest._parse_tile_dimension("invalid") is None


def test_parse_roi():
    """
    Test parsing ROI WKT.
    """
    roi = ImageRequest._parse_roi("POINT (1 2)")
    assert roi is not None
    assert roi.geom_type == "Point"
    assert ImageRequest._parse_roi(None) is None


def test_parse_tile_format_and_compression_defaults():
    """
    Test parsing tile format and compression defaults.
    """
    assert ImageRequest._parse_tile_format(None) == "NITF"
    assert ImageRequest._parse_tile_compression(None) == "NONE"


def test_parse_tile_format_invalid():
    """
    Test invalid tile format raises KeyError.
    """
    with pytest.raises(KeyError):
        ImageRequest._parse_tile_format("NOT_A_FORMAT")


def test_parse_tile_compression_invalid():
    """
    Test invalid tile compression raises KeyError.
    """
    with pytest.raises(KeyError):
        ImageRequest._parse_tile_compression("NOT_A_COMPRESSION")


def test_parse_model_invoke_mode_default():
    """
    Test model invoke mode defaults to SM_ENDPOINT.
    """
    assert ImageRequest._parse_model_invoke_mode(None) == ModelInvokeMode.SM_ENDPOINT


def test_parse_outputs_no_outputs(caplog):
    """
    Test outputs parsing when no outputs provided.
    """
    with caplog.at_level("WARNING"):
        outputs = ImageRequest._parse_outputs({"jobId": "test-job"})
    assert outputs == []
    assert "No output syncs were present in this request." in caplog.text


def test_parse_post_processing_defaults_and_conversion():
    """
    Test post-processing parsing defaults and camelCase conversion.
    """
    default_ops = ImageRequest._parse_post_processing(None)
    assert len(default_ops) == 1

    post_processing = [
        {
            "step": "FEATURE_DISTILLATION",
            "algorithm": {
                "algorithmType": "SOFT_NMS",
                "iouThreshold": 0.5,
                "skipBoxThreshold": 0.1,
                "sigma": 0.2,
            },
        }
    ]
    ops = ImageRequest._parse_post_processing(post_processing)
    assert len(ops) == 1
    assert ops[0].algorithm.iou_threshold == 0.5
    assert ops[0].algorithm.skip_box_threshold == 0.1
    assert ops[0].algorithm.sigma == 0.2


def test_get_shared_values(image_request):
    """
    Test shared values include key fields.
    """
    image_request.model_endpoint_parameters = {"CustomAttributes": "x=y"}
    shared = image_request.get_shared_values()
    assert shared["image_id"] == "test-image-id"
    assert shared["model_endpoint_parameters"] == {"CustomAttributes": "x=y"}
