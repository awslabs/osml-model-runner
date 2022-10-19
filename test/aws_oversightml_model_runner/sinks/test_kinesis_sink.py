from typing import List
from unittest import mock

import boto3
import geojson
import pytest
from botocore.stub import ANY, Stubber

from aws_oversightml_model_runner.sinks import SinkMode
from configuration import TEST_ENV_CONFIG

test_image_id = "fake_image_123"
test_stream = "test_stream"
mock_response = {
    "ShardId": "shardId-000000000000",
    "SequenceNumber": "49632155903354096944077309979289188168053675801607929858",
}


@pytest.fixture
def test_feature_list() -> List[geojson.Feature]:
    with open("./test/data/detections.geojson", "r") as geojson_file:
        sample_features = geojson.load(geojson_file)["features"]
    return sample_features


@mock.patch.dict("os.environ", TEST_ENV_CONFIG, clear=True)
def test_write_features_default_credentials(test_feature_list):
    from aws_oversightml_model_runner.sinks.kinesis_sink import KinesisSink

    kinesis_sink = KinesisSink(test_stream)
    kinesis_client_stub = Stubber(kinesis_sink.kinesisClient)
    kinesis_client_stub.activate()
    kinesis_client_stub.add_response(
        "put_record",
        mock_response,
        {
            "StreamName": test_stream,
            "PartitionKey": test_image_id,
            "Data": geojson.dumps(geojson.FeatureCollection(test_feature_list)),
        },
    )
    kinesis_sink.write(test_image_id, test_feature_list)
    kinesis_client_stub.assert_no_pending_responses()


@mock.patch.dict("os.environ", TEST_ENV_CONFIG, clear=True)
def test_write_features_default_credentials_image_id_with_slash(test_feature_list):
    from aws_oversightml_model_runner.sinks.kinesis_sink import KinesisSink

    image_id_with_slashes = "fake/image/123"

    kinesis_sink = KinesisSink(test_stream)
    kinesis_client_stub = Stubber(kinesis_sink.kinesisClient)
    kinesis_client_stub.activate()
    kinesis_client_stub.add_response(
        "put_record",
        mock_response,
        {
            "StreamName": test_stream,
            "PartitionKey": "123",
            "Data": geojson.dumps(geojson.FeatureCollection(test_feature_list)),
        },
    )
    kinesis_sink.write(image_id_with_slashes, test_feature_list)
    kinesis_client_stub.assert_no_pending_responses()


@mock.patch.dict("os.environ", TEST_ENV_CONFIG, clear=True)
def test_write_features_batch_size_one(test_feature_list):
    from aws_oversightml_model_runner.sinks.kinesis_sink import KinesisSink

    kinesis_sink = KinesisSink(stream=test_stream, batch_size=1)
    kinesis_client_stub = Stubber(kinesis_sink.kinesisClient)
    kinesis_client_stub.activate()
    for index, feature in enumerate(test_feature_list):
        kinesis_client_stub.add_response(
            "put_record",
            {"ShardId": "shardId-000000000000", "SequenceNumber": str(index)},
            {
                "StreamName": test_stream,
                "PartitionKey": test_image_id,
                "Data": geojson.dumps(geojson.FeatureCollection([feature])),
            },
        )
    kinesis_sink.write(test_image_id, test_feature_list)
    kinesis_client_stub.assert_no_pending_responses()


@mock.patch.dict("os.environ", TEST_ENV_CONFIG, clear=True)
def test_write_batch_size_three(test_feature_list):
    from aws_oversightml_model_runner.sinks.kinesis_sink import KinesisSink

    kinesis_sink = KinesisSink(stream=test_stream, batch_size=3)
    kinesis_client_stub = Stubber(kinesis_sink.kinesisClient)
    kinesis_client_stub.activate()
    # We expect the test list to have 4 features because we're specifically
    # testing the draining of the list here
    assert len(test_feature_list) == 4

    kinesis_client_stub.add_response(
        "put_record",
        mock_response,
        {
            "StreamName": test_stream,
            "PartitionKey": test_image_id,
            "Data": geojson.dumps(geojson.FeatureCollection(test_feature_list[:3])),
        },
    )
    kinesis_client_stub.add_response(
        "put_record",
        mock_response,
        {
            "StreamName": test_stream,
            "PartitionKey": test_image_id,
            "Data": geojson.dumps(geojson.FeatureCollection(test_feature_list[3:])),
        },
    )
    kinesis_sink.write(test_image_id, test_feature_list)
    kinesis_client_stub.assert_no_pending_responses()


@mock.patch.dict("os.environ", TEST_ENV_CONFIG, clear=True)
def test_write_oversized_record(test_feature_list):
    from aws_oversightml_model_runner.sinks.kinesis_sink import KinesisSink

    kinesis_sink = KinesisSink(test_stream)
    kinesis_client_stub = Stubber(kinesis_sink.kinesisClient)
    kinesis_client_stub.activate()

    kinesis_client_stub.add_client_error(
        "put_record",
        service_error_code="ValidationException",
        service_message="""An error occurred (ValidationException) when calling the PutRecord
        operation: 1 validation error detected: Value at 'data' failed to satisfy constraint:
        Member must have length less than or equal to 1048576.""",
        expected_params={
            "StreamName": test_stream,
            "PartitionKey": test_image_id,
            "Data": geojson.dumps(geojson.FeatureCollection(test_feature_list)),
        },
    )
    with pytest.raises(Exception) as e_info:
        kinesis_sink.write(test_image_id, test_feature_list)
    assert str(e_info.value).startswith(
        "An error occurred (ValidationException) when calling the PutRecord operation"
    )
    kinesis_client_stub.assert_no_pending_responses()


@mock.patch.dict("os.environ", TEST_ENV_CONFIG, clear=True)
@mock.patch("aws_oversightml_model_runner.worker.credentials_utils.sts_client")
def test_assumed_credentials(mock_sts):
    from aws_oversightml_model_runner.sinks.kinesis_sink import KinesisSink

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

    KinesisSink(stream=test_stream, assumed_role="OSMLKinesisWriter")

    boto3.DEFAULT_SESSION.client.assert_called_with(
        "kinesis",
        aws_access_key_id=test_access_key_id,
        aws_secret_access_key=test_secret_access_key,
        aws_session_token=test_secret_token,
        config=ANY,
    )
    boto3.DEFAULT_SESSION = None
    session_patch.stop()


def test_return_name():
    from aws_oversightml_model_runner.sinks.kinesis_sink import KinesisSink

    kinesis_sink = KinesisSink(test_stream)
    assert "Kinesis" == kinesis_sink.name()


def test_return_mode():
    from aws_oversightml_model_runner.sinks.kinesis_sink import KinesisSink

    kinesis_sink = KinesisSink(test_stream)
    assert SinkMode.AGGREGATE == kinesis_sink.mode
