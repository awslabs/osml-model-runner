#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

import boto3
import geojson
import pytest
from botocore.stub import ANY, Stubber
from geojson import Feature
from moto import mock_aws

TEST_PREFIX = "folder"
TEST_RESULTS_BUCKET = "test-results-bucket"
TEST_IMAGE_ID = "test-image-id"

MOCK_S3_PUT_OBJECT_RESPONSE = {
    "ResponseMetadata": {
        "RequestId": "5994D680BF127CE3",
        "HTTPStatusCode": 200,
        "RetryAttempts": 1,
    },
    "ETag": '"6299528715bad0e3510d1e4c4952ee7e"',
}

MOCK_S3_BUCKETS_RESPONSE = {
    "ResponseMetadata": {
        "RequestId": "5994D680BF127CE3",
        "HTTPStatusCode": 200,
        "RetryAttempts": 1,
    },
}


@pytest.fixture
def sample_feature_list():
    """
    Builds a known list of testing features from a data file.

    :return: A list of 6 different GeoJSON Features.
    """
    with open("./test/data/detections.geojson", "r") as geojson_file:
        sample_features = geojson.load(geojson_file)["features"]
    return sample_features


def test_write_features_default_credentials(sample_feature_list):
    """
    Write features to S3 using default credentials.
    Ensures that the `write` method can send a GeoJSON file correctly when default credentials are used.
    """
    from aws.osml.model_runner.sink.s3_sink import S3Sink

    s3_sink = S3Sink(TEST_RESULTS_BUCKET, TEST_PREFIX)
    s3_client_stub = Stubber(s3_sink.s3_client)
    s3_client_stub.activate()
    s3_client_stub.add_response(
        "head_bucket",
        MOCK_S3_BUCKETS_RESPONSE,
        {"Bucket": TEST_RESULTS_BUCKET},
    )
    s3_client_stub.add_response(
        "put_object",
        MOCK_S3_PUT_OBJECT_RESPONSE,
        {
            "ACL": "bucket-owner-full-control",
            "Bucket": TEST_RESULTS_BUCKET,
            "Key": f"{TEST_PREFIX}/{TEST_IMAGE_ID}.geojson",
            "Body": ANY,
        },
    )
    s3_sink.write(TEST_IMAGE_ID, sample_feature_list)
    s3_client_stub.assert_no_pending_responses()


def test_write_features_default_credentials_image_id_with_slash(sample_feature_list):
    """
    Write features to S3 when image ID contains slashes.
    Ensures that slashes in the image ID are properly handled, and only the file name is used for the S3 key.
    """
    from aws.osml.model_runner.sink.s3_sink import S3Sink

    image_id_with_slashes = "fake/image/123"

    s3_sink = S3Sink(TEST_RESULTS_BUCKET, TEST_PREFIX)
    s3_client_stub = Stubber(s3_sink.s3_client)
    s3_client_stub.activate()
    s3_client_stub.add_response(
        "head_bucket",
        MOCK_S3_BUCKETS_RESPONSE,
        {"Bucket": TEST_RESULTS_BUCKET},
    )
    s3_client_stub.add_response(
        "put_object",
        MOCK_S3_PUT_OBJECT_RESPONSE,
        {
            "ACL": "bucket-owner-full-control",
            "Bucket": TEST_RESULTS_BUCKET,
            "Key": f"{TEST_PREFIX}/123.geojson",
            "Body": ANY,
        },
    )
    s3_sink.write(image_id_with_slashes, sample_feature_list)
    s3_client_stub.assert_no_pending_responses()


def test_s3_bucket_404_failure(sample_feature_list):
    """
    Attempt to write to a non-existent S3 bucket (HTTP 404).
    Validates that `write` does not proceed if the bucket cannot be found.
    """
    from aws.osml.model_runner.sink.s3_sink import S3Sink

    s3_sink = S3Sink(TEST_RESULTS_BUCKET, TEST_PREFIX)
    s3_client_stub = Stubber(s3_sink.s3_client)
    s3_client_stub.activate()
    s3_client_stub.add_client_error(
        "head_bucket",
        service_error_code="404",
        service_message="Not Found",
        expected_params={"Bucket": "test-results-bucket"},
    )
    assert not s3_sink.write(TEST_IMAGE_ID, sample_feature_list)
    s3_client_stub.assert_no_pending_responses()


def test_s3_bucket_403_failure(sample_feature_list):
    """
    Attempt to write to an S3 bucket where access is forbidden (HTTP 403).
    Validates that `write` does not proceed if there is no permission to access the bucket.
    """
    from aws.osml.model_runner.sink.s3_sink import S3Sink

    s3_sink = S3Sink(TEST_RESULTS_BUCKET, TEST_PREFIX)
    s3_client_stub = Stubber(s3_sink.s3_client)
    s3_client_stub.activate()
    s3_client_stub.add_client_error(
        "head_bucket",
        service_error_code="403",
        service_message="Forbidden",
        expected_params={"Bucket": "test-results-bucket"},
    )
    assert not s3_sink.write(TEST_IMAGE_ID, sample_feature_list)
    s3_client_stub.assert_no_pending_responses()


def test_assumed_credentials(mocker):
    """
    Initialize S3Sink with assumed role credentials.
    Ensures that the S3 client is correctly configured with the assumed role's credentials.
    """
    from aws.osml.model_runner.sink.s3_sink import S3Sink

    test_access_key_id = "123456789"
    test_secret_access_key = "987654321"
    test_secret_token = "SecretToken123"

    mock_sts = mocker.patch("aws.osml.model_runner.common.credentials_utils.sts_client")
    mock_sts.assume_role.return_value = {
        "Credentials": {
            "AccessKeyId": test_access_key_id,
            "SecretAccessKey": test_secret_access_key,
            "SessionToken": test_secret_token,
        }
    }

    test_session = mocker.patch("boto3.Session", autospec=True)
    boto3.DEFAULT_SESSION = test_session

    S3Sink(TEST_RESULTS_BUCKET, TEST_PREFIX, assumed_role="OSMLS3Writer")

    boto3.DEFAULT_SESSION.client.assert_called_with(
        "s3",
        aws_access_key_id=test_access_key_id,
        aws_secret_access_key=test_secret_access_key,
        aws_session_token=test_secret_token,
        config=ANY,
    )
    boto3.DEFAULT_SESSION = None


def test_return_name():
    """
    Verify the `name` method returns the correct SinkType.
    """
    from aws.osml.model_runner.sink.s3_sink import S3Sink

    s3_sink = S3Sink(TEST_RESULTS_BUCKET, TEST_PREFIX)
    assert "S3" == s3_sink.name()


def test_return_mode():
    """
    Verify the `mode` method returns the correct SinkMode.
    """
    from aws.osml.model_runner.api.sink import SinkMode
    from aws.osml.model_runner.sink.s3_sink import S3Sink

    s3_sink = S3Sink(TEST_RESULTS_BUCKET, TEST_PREFIX)
    assert SinkMode.AGGREGATE == s3_sink.mode


def test_write_triggers_multipart_upload(mocker):
    """
    Test multipart upload conditions when writing large GeoJSON data.
    Ensures that multipart upload configurations are triggered when the size exceeds thresholds.
    """
    from aws.osml.model_runner.app_config import BotoConfig
    from aws.osml.model_runner.sink.s3_sink import S3Sink

    # Mock geojson size to be 6 GB
    mocker.patch("sys.getsizeof", return_value=6 * 1024**3)
    mock_upload_file = mocker.patch("boto3.s3.transfer.S3Transfer.upload_file", autospec=True)

    with mock_aws():
        s3_client = boto3.client("s3", config=BotoConfig.default)
        s3_client.create_bucket(
            Bucket=TEST_RESULTS_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
        )
        sink = S3Sink(bucket=TEST_RESULTS_BUCKET, prefix=TEST_PREFIX)

        features = Feature(geometry={"type": "Point", "coordinates": [0.0, 0.0]})
        result = sink.write(image_id=TEST_IMAGE_ID, features=features)
        assert result

        mock_upload_file.assert_called_once()
