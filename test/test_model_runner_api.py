from typing import Any, Dict

import mock
import pytest
import shapely.geometry

from aws_oversightml_model_runner.sqs.request_utils import ModelHostingOptions
from aws_oversightml_model_runner.worker.image_utils import ImageCompression, ImageFormats
from configuration import TEST_ENV_CONFIG

base_request = {
    "jobArn": "arn:aws:oversightml:us-east-1:674401241798:ipj/test-job",
    "jobName": "test-job",
    "jobId": "5f4e8a55-95cf-4d96-95cd-9b037f767eff",
    "imageUrls": ["s3://fake-bucket/images/test-image-id"],
    "imageProcessor": {"name": "test-model-name", "type": "SM_ENDPOINT"},
}


@mock.patch.dict("os.environ", TEST_ENV_CONFIG, clear=True)
def test_region_request_constructor():
    from aws_oversightml_model_runner.sqs.region_request import RegionRequest

    region_request_template = {
        "model_name": "test-model-name",
        "model_hosting_type": "SM_ENDPOINT",
        "model_invocation_role": "arn:aws:iam::010321660603:role/OversightMLBetaModelInvokerRole",
    }

    rr = RegionRequest(
        region_request_template,
        image_id="test-image-id",
        image_url="s3://fake-bucket/images/test-image-id",
        image_read_role="arn:aws:iam::010321660603:role/OversightMLBetaS3ReadOnly",
        region_bounds=[0, 1, 2, 3],
    )

    # Check to ensure we've created a valid request
    assert rr.is_valid()

    # Checks to ensure the dictionary provided values are set
    assert rr.model_name == "test-model-name"
    assert rr.model_hosting_type == ModelHostingOptions.SM_ENDPOINT
    assert (
        rr.model_invocation_role == "arn:aws:iam::010321660603:role/OversightMLBetaModelInvokerRole"
    )

    # Checks to ensure the keyword arguments are set
    assert rr.image_id == "test-image-id"
    assert rr.image_url == "s3://fake-bucket/images/test-image-id"
    assert rr.image_read_role == "arn:aws:iam::010321660603:role/OversightMLBetaS3ReadOnly"
    assert rr.region_bounds == [0, 1, 2, 3]

    # Checks to ensure the defaults are set
    assert rr.tile_size == (1024, 1024)
    assert rr.tile_overlap == (50, 50)
    assert rr.tile_format == ImageFormats.NITF
    assert rr.tile_compression == ImageCompression.NONE


@mock.patch.dict("os.environ", TEST_ENV_CONFIG, clear=True)
def test_image_request_constructor():
    from aws_oversightml_model_runner.sinks.s3_sink import S3Sink
    from aws_oversightml_model_runner.sqs.image_request import ImageRequest

    image_request_template = {
        "model_name": "test-model-name",
        "model_hosting_type": "SM_ENDPOINT",
        "image_read_role": "arn:aws:iam::010321660603:role/OversightMLBetaS3ReadOnly",
    }
    fake_s3_sink = {
        "type": "S3",
        "bucket": "fake-bucket",
        "prefix": "images/outputs",
        "mode": "Aggregate",
    }
    ir = ImageRequest(
        image_request_template,
        job_arn="arn:aws:oversightml:us-east-1:674401241798:ipj/test-job",
        job_name="test-job",
        job_id="5f4e8a55-95cf-4d96-95cd-9b037f767eff",
        image_id="5f4e8a55-95cf-4d96-95cd-9b037f767eff:s3://fake-bucket/images/test-image-id",
        image_url="s3://fake-bucket/images/test-image-id",
        outputs=[fake_s3_sink],
    )

    assert ir.is_valid()
    assert ir.image_url == "s3://fake-bucket/images/test-image-id"
    assert (
        ir.image_id == "5f4e8a55-95cf-4d96-95cd-9b037f767eff:s3://fake-bucket/images/test-image-id"
    )
    assert ir.image_read_role == "arn:aws:iam::010321660603:role/OversightMLBetaS3ReadOnly"
    assert ir.tile_size == (1024, 1024)
    assert ir.tile_overlap == (50, 50)
    assert ir.model_name == "test-model-name"
    assert ir.model_hosting_type == "SM_ENDPOINT"
    assert ir.model_invocation_role == ""
    assert ir.tile_format == "NITF"
    assert ir.tile_compression == ImageCompression.NONE
    assert ir.job_id == "5f4e8a55-95cf-4d96-95cd-9b037f767eff"
    assert ir.job_arn == "arn:aws:oversightml:us-east-1:674401241798:ipj/test-job"
    assert len(ir.outputs) == 1
    target: S3Sink = ir.outputs[0]
    assert target.bucket == "fake-bucket"
    assert target.prefix == "images/outputs"
    assert ir.roi is None


@mock.patch.dict("os.environ", TEST_ENV_CONFIG, clear=True)
@mock.patch("aws_oversightml_model_runner.worker.credentials_utils.sts_client")
def test_image_request_from_message(mock_sts):
    from aws_oversightml_model_runner.sinks.s3_sink import S3Sink
    from aws_oversightml_model_runner.sqs.image_request import ImageRequest

    test_access_key_id = "123456789"
    test_secret_access_key = "987654321"
    test_secret_token = "SecretToken123"
    mock_sts.assume_role.return_value = {
        "Credentials": {
            "AccessKeyId": test_access_key_id,
            "SecretAccessKey": test_secret_access_key,
            "SessionToken": test_secret_token,
        }
    }
    updates: Dict[str, Any] = {
        "jobStatus": "SUBMITTED",
        "processingSubmitted": "2021-09-14T00:18:32.130000+00:00",
        "imageReadRole": "arn:aws:iam::010321660603:role/OversightMLS3ReadOnly",
        "outputs": [
            {
                "type": "S3",
                "bucket": "fake-bucket",
                "prefix": "images/outputs",
                "assumedRole": "arn:aws:iam::010321660603:role/OversightMLBetaS3ReadOnlyRole",
            }
        ],
        "imageProcessorTileSize": 2048,
        "imageProcessorTileOverlap": 100,
        "imageProcessorTileFormat": "PNG",
        "imageProcessorTileCompression": ImageCompression.NONE,
        "regionOfInterest": "POLYGON((0.5 0.5,5 0,5 5,0 5,0.5 0.5), (1.5 1,4 3,4 1,1.5 1))",
    }
    message_body = base_request.copy()
    message_body.update(updates)

    ir = ImageRequest.from_external_message(message_body)

    assert ir.is_valid()
    assert ir.image_url == "s3://fake-bucket/images/test-image-id"
    assert (
        ir.image_id == "5f4e8a55-95cf-4d96-95cd-9b037f767eff:s3://fake-bucket/images/test-image-id"
    )
    assert ir.image_read_role == "arn:aws:iam::010321660603:role/OversightMLS3ReadOnly"
    assert ir.tile_size == (2048, 2048)
    assert ir.tile_overlap == (100, 100)
    assert ir.model_name == "test-model-name"
    assert ir.model_hosting_type == "SM_ENDPOINT"
    assert ir.tile_format == "PNG"
    assert ir.tile_compression == ImageCompression.NONE
    assert ir.job_id == "5f4e8a55-95cf-4d96-95cd-9b037f767eff"
    assert ir.job_arn == "arn:aws:oversightml:us-east-1:674401241798:ipj/test-job"
    assert len(ir.outputs) == 1
    target: S3Sink = ir.outputs[0]
    assert target.bucket == "fake-bucket"
    assert target.prefix == "images/outputs"
    assert isinstance(ir.roi, shapely.geometry.Polygon)


@mock.patch.dict("os.environ", TEST_ENV_CONFIG, clear=True)
def test_image_request_from_minimal_message_legacy_output():
    from aws_oversightml_model_runner.sinks.s3_sink import S3Sink
    from aws_oversightml_model_runner.sqs.image_request import ImageRequest

    updates: Dict[str, Any] = {"outputBucket": "fake-bucket", "outputPrefix": "images/outputs"}
    message_body = base_request.copy()
    message_body.update(updates)

    ir = ImageRequest.from_external_message(message_body)

    assert ir.is_valid()
    assert ir.image_url == "s3://fake-bucket/images/test-image-id"
    assert (
        ir.image_id == "5f4e8a55-95cf-4d96-95cd-9b037f767eff:s3://fake-bucket/images/test-image-id"
    )
    assert ir.image_read_role == ""
    assert ir.tile_size == (1024, 1024)
    assert ir.tile_overlap == (50, 50)
    assert ir.model_name == "test-model-name"
    assert ir.model_hosting_type == "SM_ENDPOINT"
    assert ir.model_invocation_role == ""
    assert ir.tile_format == "NITF"
    assert ir.tile_compression == ImageCompression.NONE
    assert ir.job_id == "5f4e8a55-95cf-4d96-95cd-9b037f767eff"
    assert ir.job_arn == "arn:aws:oversightml:us-east-1:674401241798:ipj/test-job"
    assert len(ir.outputs) == 1
    target: S3Sink = ir.outputs[0]
    assert target.bucket == "fake-bucket"
    assert target.prefix == "images/outputs"
    assert ir.roi is None


@mock.patch.dict("os.environ", TEST_ENV_CONFIG, clear=True)
def test_image_request_multiple_sinks():
    from aws_oversightml_model_runner.sinks.kinesis_sink import KinesisSink
    from aws_oversightml_model_runner.sinks.s3_sink import S3Sink
    from aws_oversightml_model_runner.sqs.image_request import ImageRequest

    updates: Dict[str, Any] = {
        "outputs": [
            {
                "type": "S3",
                "bucket": "fake-bucket",
                "prefix": "images/outputs",
                "mode": "Aggregate",
            },
            {"type": "Kinesis", "stream": "FakeStream", "batchSize": 500},
        ]
    }
    message_body = base_request.copy()
    message_body.update(updates)

    ir = ImageRequest.from_external_message(message_body)

    assert ir.is_valid()
    assert ir.image_url == "s3://fake-bucket/images/test-image-id"
    assert (
        ir.image_id == "5f4e8a55-95cf-4d96-95cd-9b037f767eff:s3://fake-bucket/images/test-image-id"
    )
    assert ir.image_read_role == ""
    assert ir.tile_size == (1024, 1024)
    assert ir.tile_overlap == (50, 50)
    assert ir.model_name == "test-model-name"
    assert ir.model_hosting_type == "SM_ENDPOINT"
    assert ir.model_invocation_role == ""
    assert ir.tile_format == "NITF"
    assert ir.tile_compression == ImageCompression.NONE
    assert ir.job_id == "5f4e8a55-95cf-4d96-95cd-9b037f767eff"
    assert ir.job_arn == "arn:aws:oversightml:us-east-1:674401241798:ipj/test-job"
    assert len(ir.outputs) == 2
    first_target: S3Sink = ir.outputs[0]
    assert first_target.bucket == "fake-bucket"
    assert first_target.prefix == "images/outputs"
    second_target: KinesisSink = ir.outputs[1]
    assert second_target.stream == "FakeStream"
    assert second_target.batch_size == 500
    assert ir.roi is None


@mock.patch.dict("os.environ", TEST_ENV_CONFIG, clear=True)
def test_image_request_invalid_sink():
    from aws_oversightml_model_runner.sqs.image_request import ImageRequest

    updates: Dict[str, Any] = {"outputs": [{"type": "SQS", "queue": "FakeQueue"}]}
    message_body = base_request.copy()
    message_body.update(updates)

    with pytest.raises(ValueError) as e_info:
        ImageRequest.from_external_message(message_body)
    assert str(e_info.value) == "Invalid Image Request"
