import unittest
from typing import List
from unittest import mock

import boto3
import geojson
import pytest
from botocore.stub import ANY, Stubber

from configuration import TEST_ENV_CONFIG, TEST_IMAGE_ID, TEST_RESULTS_STREAM

MOCK_KINESIS_RESPONSE = {
    "ShardId": "shardId-000000000000",
    "SequenceNumber": "49632155903354096944077309979289188168053675801607929858",
}


@mock.patch.dict("os.environ", TEST_ENV_CONFIG, clear=True)
class TestKinesisSink(unittest.TestCase):
    def setUp(self):
        self.test_feature_list = self.build_feature_list()

    def tearDown(self):
        self.test_feature_list = None

    def test_write_features_default_credentials(self):
        from aws_oversightml_model_runner.sink import KinesisSink

        kinesis_sink = KinesisSink(TEST_RESULTS_STREAM)
        kinesis_client_stub = Stubber(kinesis_sink.kinesisClient)
        kinesis_client_stub.activate()
        kinesis_client_stub.add_response(
            "put_record",
            MOCK_KINESIS_RESPONSE,
            {
                "StreamName": TEST_RESULTS_STREAM,
                "PartitionKey": TEST_IMAGE_ID,
                "Data": geojson.dumps(geojson.FeatureCollection(self.test_feature_list)),
            },
        )
        kinesis_sink.write(TEST_IMAGE_ID, self.test_feature_list)
        kinesis_client_stub.assert_no_pending_responses()

    def test_write_features_default_credentials_image_id_with_slash(self):
        from aws_oversightml_model_runner.sink import KinesisSink

        image_id_with_slashes = "fake/image/123"

        kinesis_sink = KinesisSink(TEST_RESULTS_STREAM)
        kinesis_client_stub = Stubber(kinesis_sink.kinesisClient)
        kinesis_client_stub.activate()
        kinesis_client_stub.add_response(
            "put_record",
            MOCK_KINESIS_RESPONSE,
            {
                "StreamName": TEST_RESULTS_STREAM,
                "PartitionKey": image_id_with_slashes,
                "Data": geojson.dumps(geojson.FeatureCollection(self.test_feature_list)),
            },
        )
        kinesis_sink.write(image_id_with_slashes, self.test_feature_list)
        kinesis_client_stub.assert_no_pending_responses()

    def test_write_features_batch_size_one(self):
        from aws_oversightml_model_runner.sink import KinesisSink

        kinesis_sink = KinesisSink(stream=TEST_RESULTS_STREAM, batch_size=1)
        kinesis_client_stub = Stubber(kinesis_sink.kinesisClient)
        kinesis_client_stub.activate()
        for index, feature in enumerate(self.test_feature_list):
            kinesis_client_stub.add_response(
                "put_record",
                {"ShardId": "shardId-000000000000", "SequenceNumber": str(index)},
                {
                    "StreamName": TEST_RESULTS_STREAM,
                    "PartitionKey": TEST_IMAGE_ID,
                    "Data": geojson.dumps(geojson.FeatureCollection([feature])),
                },
            )
        kinesis_sink.write(TEST_IMAGE_ID, self.test_feature_list)
        kinesis_client_stub.assert_no_pending_responses()

    def test_write_batch_size_three(self):
        from aws_oversightml_model_runner.sink import KinesisSink

        kinesis_sink = KinesisSink(stream=TEST_RESULTS_STREAM, batch_size=3)
        kinesis_client_stub = Stubber(kinesis_sink.kinesisClient)
        kinesis_client_stub.activate()
        # We expect the test list to have 4 features because we're specifically
        # testing the draining of the list here
        assert len(self.test_feature_list) == 4

        kinesis_client_stub.add_response(
            "put_record",
            MOCK_KINESIS_RESPONSE,
            {
                "StreamName": TEST_RESULTS_STREAM,
                "PartitionKey": TEST_IMAGE_ID,
                "Data": geojson.dumps(geojson.FeatureCollection(self.test_feature_list[:3])),
            },
        )
        kinesis_client_stub.add_response(
            "put_record",
            MOCK_KINESIS_RESPONSE,
            {
                "StreamName": TEST_RESULTS_STREAM,
                "PartitionKey": TEST_IMAGE_ID,
                "Data": geojson.dumps(geojson.FeatureCollection(self.test_feature_list[3:])),
            },
        )
        kinesis_sink.write(TEST_IMAGE_ID, self.test_feature_list)
        kinesis_client_stub.assert_no_pending_responses()

    def test_write_oversized_record(self):
        from aws_oversightml_model_runner.sink import KinesisSink

        kinesis_sink = KinesisSink(TEST_RESULTS_STREAM)
        kinesis_client_stub = Stubber(kinesis_sink.kinesisClient)
        kinesis_client_stub.activate()

        kinesis_client_stub.add_client_error(
            "put_record",
            service_error_code="ValidationException",
            service_message="""An error occurred (ValidationException) when calling the PutRecord
            operation: 1 validation error detected: Value at 'data' failed to satisfy constraint:
            Member must have length less than or equal to 1048576.""",
            expected_params={
                "StreamName": TEST_RESULTS_STREAM,
                "PartitionKey": TEST_IMAGE_ID,
                "Data": geojson.dumps(geojson.FeatureCollection(self.test_feature_list)),
            },
        )
        with pytest.raises(Exception) as e_info:
            kinesis_sink.write(TEST_IMAGE_ID, self.test_feature_list)
        assert str(e_info.value).startswith(
            "An error occurred (ValidationException) when calling the PutRecord operation"
        )
        kinesis_client_stub.assert_no_pending_responses()

    @mock.patch("aws_oversightml_model_runner.common.credentials_utils.sts_client")
    def test_assumed_credentials(self, mock_sts):
        from aws_oversightml_model_runner.sink.kinesis_sink import KinesisSink

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

        KinesisSink(stream=TEST_RESULTS_STREAM, assumed_role="OSMLKinesisWriter")

        boto3.DEFAULT_SESSION.client.assert_called_with(
            "kinesis",
            aws_access_key_id=test_access_key_id,
            aws_secret_access_key=test_secret_access_key,
            aws_session_token=test_secret_token,
            config=ANY,
        )
        boto3.DEFAULT_SESSION = None
        session_patch.stop()

    def test_return_name(self):
        from aws_oversightml_model_runner.sink import KinesisSink

        kinesis_sink = KinesisSink(TEST_RESULTS_STREAM)
        assert "Kinesis" == kinesis_sink.name()

    def test_return_mode(self):
        from aws_oversightml_model_runner.sink import KinesisSink, SinkMode

        kinesis_sink = KinesisSink(TEST_RESULTS_STREAM)
        assert SinkMode.AGGREGATE == kinesis_sink.mode

    @staticmethod
    def build_feature_list() -> List[geojson.Feature]:
        with open("./test/data/detections.geojson", "r") as geojson_file:
            sample_features = geojson.load(geojson_file)["features"]
        return sample_features


if __name__ == "__main__":
    unittest.main()
