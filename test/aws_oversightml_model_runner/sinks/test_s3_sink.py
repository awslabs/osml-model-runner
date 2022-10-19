from typing import List
from unittest import mock

import boto3
import geojson
import pytest
from botocore.stub import ANY, Stubber

from aws_oversightml_model_runner.sinks import SinkMode
from configuration import TEST_ENV_CONFIG

test_image_id = "fake_image_123"
test_bucket = "test_bucket"
test_prefix = "folder"
mock_response = {
    "ResponseMetadata": {
        "RequestId": "5994D680BF127CE3",
        "HTTPStatusCode": 200,
        "RetryAttempts": 1,
    },
    "ETag": '"6299528715bad0e3510d1e4c4952ee7e"',
}


@pytest.fixture
def test_feature_list() -> List[geojson.Feature]:
    with open("./test/data/detections.geojson", "r") as geojson_file:
        sample_features = geojson.load(geojson_file)["features"]
    return sample_features


@mock.patch.dict("os.environ", TEST_ENV_CONFIG, clear=True)
def test_write_features_default_credentials(test_feature_list):
    from aws_oversightml_model_runner.sinks.s3_sink import S3Sink

    s3_sink = S3Sink(test_bucket, test_prefix)
    s3_client_stub = Stubber(s3_sink.s3Client)
    s3_client_stub.activate()
    s3_client_stub.add_response(
        "put_object",
        mock_response,
        {
            "ACL": "bucket-owner-full-control",
            "Bucket": test_bucket,
            "Key": "{}/{}.geojson".format(test_prefix, test_image_id),
            "Body": ANY,
        },
    )
    s3_sink.write(test_image_id, test_feature_list)
    s3_client_stub.assert_no_pending_responses()


@mock.patch.dict("os.environ", TEST_ENV_CONFIG, clear=True)
def test_write_features_default_credentials_image_id_with_slash(test_feature_list):
    from aws_oversightml_model_runner.sinks.s3_sink import S3Sink

    image_id_with_slashes = "fake/image/123"

    s3_sink = S3Sink(test_bucket, test_prefix)
    s3_client_stub = Stubber(s3_sink.s3Client)
    s3_client_stub.activate()
    s3_client_stub.add_response(
        "put_object",
        mock_response,
        {
            "ACL": "bucket-owner-full-control",
            "Bucket": test_bucket,
            "Key": "{}/123.geojson".format(test_prefix),
            "Body": ANY,
        },
    )
    s3_sink.write(image_id_with_slashes, test_feature_list)
    s3_client_stub.assert_no_pending_responses()


@mock.patch.dict("os.environ", TEST_ENV_CONFIG, clear=True)
@mock.patch("aws_oversightml_model_runner.worker.credentials_utils.sts_client")
def test_assumed_credentials(mock_sts):
    from aws_oversightml_model_runner.sinks.s3_sink import S3Sink

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

    session_patch = mock.patch("boto3.Session", autospec=True)
    test_session = session_patch.start()
    boto3.DEFAULT_SESSION = test_session

    S3Sink(test_bucket, test_prefix, assumed_role="OSMLS3Writer")

    boto3.DEFAULT_SESSION.client.assert_called_with(
        "s3",
        aws_access_key_id=test_access_key_id,
        aws_secret_access_key=test_secret_access_key,
        aws_session_token=test_secret_token,
        config=ANY,
    )
    boto3.DEFAULT_SESSION = None
    session_patch.stop()


def test_return_name():
    from aws_oversightml_model_runner.sinks.s3_sink import S3Sink

    s3_sink = S3Sink(test_bucket, test_prefix)
    assert "S3" == s3_sink.name()


def test_return_mode():
    from aws_oversightml_model_runner.sinks.s3_sink import S3Sink

    s3_sink = S3Sink(test_bucket, test_prefix)
    assert SinkMode.AGGREGATE == s3_sink.mode
