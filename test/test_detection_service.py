
import mock
import json
import boto3
import io
import datetime

from botocore.stub import Stubber, ANY


from aws_oversightml_model_runner.detection_service import FeatureDetector
from aws_oversightml_model_runner.metrics import configure_metrics

configure_metrics("test", "stdout")

@mock.patch.dict("os.environ", {"AWS_DEFAULT_REGION": "us-east-1"})
def test_construct_with_execution_role():
    sts_client = boto3.client('sts')
    sts_client_stub = Stubber(sts_client)
    sts_client_stub.add_response('assume_role',
                                 expected_params={
                                     "RoleArn": "arn:aws:iam::010321660603:role/OversightMLBetaInvokeRole",
                                     "RoleSessionName": "AWSOversightMLModelRunner"
                                 },
                                 service_response={
                                     "Credentials": {
                                         "AccessKeyId": "FAKE-ACCESS-KEY-ID",
                                         "SecretAccessKey": "FAKE-ACCESS-KEY",
                                         "SessionToken": "FAKE-SESSION-TOKEN",
                                         'Expiration': datetime.datetime.now()
                                     }
                                 })
    sts_client_stub.activate()

    with mock.patch('aws_oversightml_model_runner.detection_service.boto3') as mock_boto3:
        mock_boto3.client.return_value = sts_client
        feature_detector = FeatureDetector("test-endpoint", "arn:aws:iam::010321660603:role/OversightMLBetaInvokeRole")
        sts_client_stub.assert_no_pending_responses()


@mock.patch.dict("os.environ", {"AWS_DEFAULT_REGION": "us-east-1"})
def test_find_features():

    feature_detector = FeatureDetector("test-endpoint")
    sm_runtime_stub = Stubber(feature_detector.sm_client)
    sm_runtime_stub.add_response('invoke_endpoint',
                                 expected_params={
                                     "EndpointName": "test-endpoint",
                                     "Body": ANY
                                 },
                                 service_response=mock_response
                                 )
    sm_runtime_stub.activate()

    with open('./test/data/GeogToWGS84GeoKey5.tif','rb') as image_file:
        encoded_image = image_file.read()
        feature_collection = feature_detector.find_features(encoded_image)
        sm_runtime_stub.assert_no_pending_responses()
        assert feature_collection["type"] == "FeatureCollection"
        assert len(feature_collection['features']) == 1


mock_response = {
    'Body': io.StringIO(json.dumps({
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "id": "1cc5e6d6-e12f-430d-adf0-8d2276ce8c5a",
                "geometry": {
                    "type": "Point",
                    "coordinates": [
                        0.0,
                        0.0
                    ]
                },
                "properties": {
                    "bounds_imcoords": [
                        429,
                        553,
                        440,
                        561
                    ],
                    "feature_types": {
                        "ground_motor_passenger_vehicle": 0.2961518168449402
                    },
                    "detection_score": 0.2961518168449402,
                    "image_id": "test-image-id"
                }
            }
        ]
    }))
}