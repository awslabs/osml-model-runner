#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

import datetime
import io
import json
import unittest
from json import JSONDecodeError
from unittest import TestCase
from unittest.mock import call, patch

import boto3
import pytest
from botocore.exceptions import ClientError
from botocore.stub import ANY, Stubber

# Mock response simulating a SageMaker model's output for feature detection
MOCK_MODEL_RESPONSE = {
    "Body": io.StringIO(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "id": "1cc5e6d6-e12f-430d-adf0-8d2276ce8c5a",
                        "geometry": {"type": "Point", "coordinates": [-43.679691, -22.941953]},
                        "properties": {
                            "bounds_imcoords": [429, 553, 440, 561],
                            "geom_imcoords": [[429, 553], [429, 561], [440, 561], [440, 553], [429, 553]],
                            "featureClasses": [{"iri": "ground_motor_passenger_vehicle", "score": 0.2961518168449402}],
                            "detection_score": 0.2961518168449402,
                            "image_id": "2pp5e6d6-e12f-430d-adf0-8d2276ceadf0",
                        },
                    }
                ],
            }
        )
    )
}


@patch("aws.osml.model_runner.inference.sm_detector.boto3")
class TestSMDetector(TestCase):
    def setUp(self):
        """
        Create the client that will be used across tests
        """
        self.sm_runtime_client = boto3.client("sagemaker-runtime")
        self.sm_runtime_stub = Stubber(self.sm_runtime_client)

    def tearDown(self):
        """
        Deactivate stub after each test
        """
        self.sm_runtime_stub.deactivate()

    def test_construct_with_execution_role(self, mock_boto3):
        """
        Test the construction of SMDetector with AWS credentials passed,
        ensuring that the client is correctly instantiated with the provided access keys.
        """
        from aws.osml.model_runner.inference import SMDetector

        mock_boto3.client.return_value = self.sm_runtime_client
        aws_credentials = {
            "AccessKeyId": "FAKE-ACCESS-KEY-ID",
            "SecretAccessKey": "FAKE-ACCESS-KEY",
            "SessionToken": "FAKE-SESSION-TOKEN",
            "Expiration": datetime.datetime.now(),
        }

        SMDetector("test-endpoint", assumed_credentials=aws_credentials)

        # Verify that the boto3 client was called with the correct parameters
        mock_boto3.client.assert_has_calls(
            [
                call(
                    "sagemaker-runtime",
                    aws_access_key_id="FAKE-ACCESS-KEY-ID",
                    aws_secret_access_key="FAKE-ACCESS-KEY",
                    aws_session_token="FAKE-SESSION-TOKEN",
                    config=ANY,
                )
            ]
        )

    def test_find_features(self, mock_boto3):
        """
        Test the find_features method of SMDetector, verifying that it correctly processes
        an image and returns a valid feature collection.
        """
        from aws.osml.model_runner.inference import SMDetector

        mock_boto3.client.return_value = self.sm_runtime_client

        self.sm_runtime_stub.add_response(
            "invoke_endpoint",
            expected_params={"EndpointName": "test-endpoint", "Body": ANY, "TargetVariant": "variant1"},
            service_response=MOCK_MODEL_RESPONSE,
        )

        self.sm_runtime_stub.activate()

        sm_detector = SMDetector("test-endpoint", endpoint_parameters={"TargetVariant": "variant1"})

        with open("./test/data/GeogToWGS84GeoKey5.tif", "rb") as image_file:
            encoded_image = image_file.read()

            feature_collection = sm_detector.find_features(encoded_image)
            self.sm_runtime_stub.assert_no_pending_responses()
            assert feature_collection["type"] == "FeatureCollection"
            assert len(feature_collection["features"]) == 1

    def test_find_features_throw_json_exception(self, mock_boto3):
        """
        Test that find_features raises a JSONDecodeError when the SageMaker response
        contains invalid JSON data.
        """
        from aws.osml.model_runner.inference import SMDetector

        mock_boto3.client.return_value = self.sm_runtime_client

        self.sm_runtime_stub.add_response(
            "invoke_endpoint",
            expected_params={"EndpointName": "test-endpoint", "Body": ANY, "TargetVariant": "variant1"},
            service_response=MOCK_MODEL_RESPONSE,
        )
        self.sm_runtime_stub.add_client_error(str(JSONDecodeError))

        self.sm_runtime_stub.activate()

        sm_detector = SMDetector("test-endpoint", endpoint_parameters={"TargetVariant": "variant1"})

        with open("./test/data/GeogToWGS84GeoKey5.tif", "rb") as image_file:
            with pytest.raises(JSONDecodeError):
                sm_detector.find_features(image_file)

    def test_find_features_throw_client_exception(self, mock_boto3):
        """
        Test that find_features raises a ClientError when SageMaker invocation fails
        due to a client-side error (e.g., network issues or endpoint misconfiguration).
        """
        from aws.osml.model_runner.inference import SMDetector

        mock_boto3.client.return_value = self.sm_runtime_client

        self.sm_runtime_stub.add_client_error("invoke_endpoint", service_error_code="500", service_message="ClientError")

        self.sm_runtime_stub.activate()

        sm_detector = SMDetector("test-endpoint", {"TargetVariant": "variant1"})

        with open("./test/data/GeogToWGS84GeoKey5.tif", "rb") as image_file:
            with pytest.raises(ClientError):
                sm_detector.find_features(image_file)

    def test_sm_name_generation(self, mock_boto3):
        """
        Test that SMDetector and HTTPDetector correctly set their invocation modes and endpoints.
        """
        from aws.osml.model_runner.api.inference import ModelInvokeMode
        from aws.osml.model_runner.inference import HTTPDetector, SMDetector

        mock_boto3.client.return_value = self.sm_runtime_client

        sm_name = "sm-test"
        sm_detector = SMDetector(endpoint=sm_name)

        # Verify SageMaker detector mode and endpoint
        assert sm_detector.mode == ModelInvokeMode.SM_ENDPOINT
        assert sm_detector.endpoint == sm_name

        http_name = "http-test"
        http_detector = HTTPDetector(endpoint=http_name)

        # Verify HTTP detector mode and endpoint
        assert http_detector.mode == ModelInvokeMode.HTTP_ENDPOINT
        assert http_detector.endpoint == http_name

    def test_set_endpoint_parameters_valid_parameters(self, mock_boto3):
        """
        Test setting only valid SageMaker endpoint parameters
        """
        from aws.osml.model_runner.inference import SMDetector

        mock_boto3.client.return_value = self.sm_runtime_client

        valid_params = {
            "ContentType": "application/json",
            "Accept": "application/json",
            "CustomAttributes": "custom",
            "TargetModel": "model1",
            "TargetVariant": "variant1",
        }
        sm_detector = SMDetector(endpoint="sm-test", endpoint_parameters=valid_params)
        assert sm_detector.endpoint_parameters == valid_params

    def test_set_endpoint_parameters_update_existing_parameters(self, mock_boto3):
        """
        Test that new parameters take precedent over existing SageMaker endpoint parameters
        """
        from aws.osml.model_runner.inference import SMDetector

        mock_boto3.client.return_value = self.sm_runtime_client

        initial_params = {
            "ContentType": "application/json",
            "Accept": "application/json",
            "CustomAttributes": "custom",
            "TargetModel": "model1",
            "TargetVariant": "variant1",
        }
        sm_detector = SMDetector(endpoint="sm-test", endpoint_parameters=initial_params)

        updated_params = {
            "ContentType": "application/json",
            "Accept": "application/json",
            "CustomAttributes": "2",
            "TargetModel": "model2",
            "TargetVariant": "variant2",
        }
        sm_detector.set_endpoint_parameters(updated_params)
        assert sm_detector.endpoint_parameters == updated_params

    def test_set_endpoint_parameters_invalid_parameters(self, mock_boto3):
        """
        Test filtering out invalid SageMaker endpoint parameters
        """
        from aws.osml.model_runner.inference import SMDetector

        mock_boto3.client.return_value = self.sm_runtime_client

        invalid_params = {
            "ContentType": "application/json",
            "InvalidParam1": "value1",
            "Accept": "application/json",
            "InvalidParam2": "value2",
            "EndpointName": "test",
            "Body": "data for model",
        }

        expected_params = {"ContentType": "application/json", "Accept": "application/json"}

        with self.assertLogs() as captured:
            sm_detector = SMDetector(endpoint="sm-test", endpoint_parameters=invalid_params)

        assert sm_detector.endpoint_parameters["ContentType"] == expected_params["ContentType"]
        assert sm_detector.endpoint_parameters["Accept"] == expected_params["Accept"]
        assert any("Ignoring invalid sagemaker endpoint parameters" in msg for msg in captured.output)

    def test_set_endpoint_parameters_empty_parameters(self, mock_boto3):
        """
        Test setting empty parameters dictionary
        """
        from aws.osml.model_runner.inference import SMDetector

        mock_boto3.client.return_value = self.sm_runtime_client

        sm_detector = SMDetector(endpoint="sm-test", endpoint_parameters={})
        assert not bool(sm_detector.endpoint_parameters)


if __name__ == "__main__":
    unittest.main()
