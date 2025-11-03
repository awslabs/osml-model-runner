#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

import json
import os
import unittest

from moto import mock_aws


@mock_aws
class CenterpointModelTest(unittest.TestCase):
    """
    Unit test case for testing Flask endpoints in the centerpoint detection app.

    This test suite uses the unittest framework and mocks AWS services using `moto`.
    Environment variables are set for the segmentation feature. Each test case
    simulates HTTP requests and verifies responses from the app.
    """

    os.environ["ENABLE_SEGMENTATION"] = "True"

    def setUp(self):
        """
        Set up the test environment by creating a Flask app and initializing the test client.
        """
        # Initialize Flask application context and test client
        from aws.osml.test_models.centerpoint.app import app

        self.ctx = app.app_context()
        self.ctx.push()
        self.client = app.test_client()

    def tearDown(self):
        """
        Clean up the test environment by popping the Flask app context.
        """
        self.ctx.pop()

    def test_ping(self):
        """
        Test the `/ping` endpoint to check if the application is running.

        Sends a GET request to `/ping` and verifies that the response status code is 200.
        """
        response = self.client.get("/ping")
        self.assertEqual(response.status_code, 200)

    @staticmethod
    def compare_two_geojson_results(actual_geojson_result, expected_json_result):
        """
        Helper method to compare two GeoJSON results.

        :param actual_geojson_result: GeoJSON result returned from the prediction model.
        :type actual_geojson_result: dict
        :param expected_json_result: Expected GeoJSON result for comparison.
        :type expected_json_result: dict

        The method checks the `type` and `features` fields and compares the geometries
        of the features. It also handles differences in image_id fields.
        """
        assert actual_geojson_result.get("type") == expected_json_result.get("type")
        assert len(actual_geojson_result.get("features")) == len(expected_json_result.get("features"))

        for actual_result, expected_result in zip(
            actual_geojson_result.get("features"), expected_json_result.get("features")
        ):
            # Both actual and expected geometry should be None in the new structure
            assert actual_result.get("geometry") is None
            assert expected_result.get("geometry") is None

            # Handle unique image_id differences
            actual_image_id = actual_result["properties"]["image_id"]
            expected_result["properties"]["image_id"] = actual_image_id

            assert actual_result.get("properties") == expected_result.get("properties")

    def test_predict_center_point_model(self):
        """
        Test the centerpoint detection model's prediction using a sample image.

        This test sends a sample image in a POST request to the `/invocations` endpoint
        and verifies that the GeoJSON result matches the expected model output.

        The method uses `compare_two_geojson_results` to compare the predicted result
        with the expected GeoJSON result.

        :raises AssertionError: If the GeoJSON results do not match.
        """
        with open("test/data/test-model.tif", "rb") as data_binary:
            response = self.client.post("/invocations", data=data_binary)

        self.assertEqual(response.status_code, 200)

        sample_output = "test/data/centerpoint_tests.geojson"
        with open(sample_output, "r") as model_output_geojson:
            expected_json_result = json.loads(model_output_geojson.read())

        actual_geojson_result = json.loads(response.data)
        self.compare_two_geojson_results(actual_geojson_result, expected_json_result)

    def test_predict_bad_data_file(self):
        """
        Test the model's response to invalid data input.

        Sends an empty byte string in the POST request to the `/invocations` endpoint
        and verifies that the response status code is 400 (Bad Request).

        :raises AssertionError: If the response status is not 400.
        """
        response = self.client.post("/invocations", data=b"")

        self.assertEqual(response.status_code, 400)
