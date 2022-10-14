import datetime
import io
import json
from json import JSONDecodeError

import boto3
import botocore
import mock
from botocore.stub import ANY, Stubber

from configuration import TEST_ENV_CONFIG


@mock.patch.dict("os.environ", TEST_ENV_CONFIG, clear=True)
def test_construct_with_execution_role():
    from aws_oversightml_model_runner.worker.feature_detector import FeatureDetector

    sm_client = boto3.client("sagemaker-runtime")
    sm_client_stub = Stubber(sm_client)
    sm_client_stub.activate()
    aws_credentials = {
        "AccessKeyId": "FAKE-ACCESS-KEY-ID",
        "SecretAccessKey": "FAKE-ACCESS-KEY",
        "SessionToken": "FAKE-SESSION-TOKEN",
        "Expiration": datetime.datetime.now(),
    }
    with mock.patch("aws_oversightml_model_runner.worker.feature_detector.boto3") as mock_boto3:
        mock_boto3.client.return_value = sm_client
        FeatureDetector("test-endpoint", aws_credentials)
        mock_boto3.client.assert_called_once_with(
            "sagemaker-runtime",
            aws_access_key_id="FAKE-ACCESS-KEY-ID",
            aws_secret_access_key="FAKE-ACCESS-KEY",
            aws_session_token="FAKE-SESSION-TOKEN",
            config=ANY,
        )


@mock.patch.dict("os.environ", TEST_ENV_CONFIG, clear=True)
def test_find_features():
    from aws_oversightml_model_runner.worker.feature_detector import FeatureDetector

    feature_detector = FeatureDetector("test-endpoint")
    sm_runtime_stub = Stubber(feature_detector.sm_client)
    sm_runtime_stub.add_response(
        "invoke_endpoint",
        expected_params={"EndpointName": "test-endpoint", "Body": ANY},
        service_response=mock_response,
    )
    sm_runtime_stub.activate()

    with open("./test/data/GeogToWGS84GeoKey5.tif", "rb") as image_file:
        encoded_image = image_file.read()
        feature_collection = feature_detector.find_features(encoded_image)
        sm_runtime_stub.assert_no_pending_responses()
        assert feature_collection["type"] == "FeatureCollection"
        assert len(feature_collection["features"]) == 1


@mock.patch.dict("os.environ", TEST_ENV_CONFIG, clear=True)
def test_find_features_throw_json_exception():
    from aws_oversightml_model_runner.worker.feature_detector import FeatureDetector

    feature_detector = FeatureDetector("test-endpoint")
    sm_runtime_stub = Stubber(feature_detector.sm_client)
    sm_runtime_stub.add_response(
        "invoke_endpoint",
        expected_params={"EndpointName": "test-endpoint", "Body": ANY},
        service_response=mock_response,
    )
    sm_runtime_stub.add_client_error(JSONDecodeError)
    sm_runtime_stub.activate()

    with open("./test/data/GeogToWGS84GeoKey5.tif", "rb") as image_file:
        encoded_image = image_file.read()
        feature_collection = feature_detector.find_features(encoded_image)
        assert feature_collection["type"] == "FeatureCollection"
        assert len(feature_collection) == 2
        assert len(feature_collection["features"]) == 0


@mock.patch.dict("os.environ", TEST_ENV_CONFIG, clear=True)
def test_find_features_throw_client_exception():
    from aws_oversightml_model_runner.worker.feature_detector import FeatureDetector

    feature_detector = FeatureDetector("test-endpoint")
    sm_runtime_stub = Stubber(feature_detector.sm_client)
    sm_runtime_stub.add_response(
        "invoke_endpoint",
        expected_params={"EndpointName": "test-endpoint", "Body": ANY},
        service_response=mock_response,
    )
    sm_runtime_stub.add_client_error(
        botocore.exceptions.ClientError(
            {"Error": {"Code": 500, "Message": "ClientError"}}, "update_item"
        )
    )
    sm_runtime_stub.activate()

    with open("./test/data/GeogToWGS84GeoKey5.tif", "rb") as image_file:
        encoded_image = image_file.read()
        feature_collection = feature_detector.find_features(encoded_image)
        assert feature_collection["type"] == "FeatureCollection"
        assert len(feature_collection) == 2
        assert len(feature_collection["features"]) == 0


mock_response = {
    "Body": io.StringIO(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "id": "1cc5e6d6-e12f-430d-adf0-8d2276ce8c5a",
                        "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
                        "properties": {
                            "bounds_imcoords": [429, 553, 440, 561],
                            "feature_types": {"ground_motor_passenger_vehicle": 0.2961518168449402},
                            "detection_score": 0.2961518168449402,
                            "image_id": "test-image-id",
                        },
                    }
                ],
            }
        )
    )
}
