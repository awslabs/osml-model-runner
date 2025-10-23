#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

import json
import os
import unittest

from moto import mock_aws


@mock_aws
class FloodModelTest(unittest.TestCase):
    """
    Unit test case for testing Flask endpoints in the flood detection model application.

    This test suite utilizes the unittest framework and mocks AWS services using `moto`.
    Environment variables are set for the flood volume. Each test case simulates HTTP
    requests and verifies responses from the flood model app.
    """

    # Set flood volume for testing
    os.environ["FLOOD_VOLUME"] = "500"

    def setUp(self):
        """
        Set up the test environment before each test case.

        This method patches the Docker container ID used in logging, initializes the
        Flask application context, and creates a test client to simulate requests.
        """
        # Initialize Flask application context and test client
        from aws.osml.test_models.flood.app import app

        self.ctx = app.app_context()
        self.ctx.push()
        self.client = app.test_client()

    def tearDown(self):
        """
        Clean up the test environment after each test case.

        This method pops the Flask application context to ensure proper cleanup after
        tests.
        """
        self.ctx.pop()

    def test_ping(self):
        """
        Test the `/ping` endpoint to check if the application is running.

        Sends a GET request to the `/ping` endpoint and verifies that the response
        status code is 200, indicating that the app is alive and healthy.
        """
        response = self.client.get("/ping")
        assert response.status_code == 200

    @staticmethod
    def compare_two_geojson_results(actual_geojson_result, expected_json_result):
        """
        Helper method to compare two GeoJSON results.

        :param actual_geojson_result: The actual GeoJSON result returned by the
                                      prediction model.
        :param expected_json_result: The expected GeoJSON result for comparison.

        The method checks the `type` and `features` fields and compares the geometries
        and properties of the features. Due to the unique `image_id` and randomized
        `bounds_imcoords`, these properties are overwritten for accurate comparison.
        """
        assert actual_geojson_result.get("type") == expected_json_result.get("type")
        assert len(actual_geojson_result.get("features")) == len(expected_json_result.get("features"))

        for actual_result, expected_result in zip(
            actual_geojson_result.get("features"), expected_json_result.get("features")
        ):
            assert actual_result.get("geometry") == expected_result.get("geometry")

            # Handle image_id and bounds_imcoords differences
            actual_image_id = actual_result["properties"]["image_id"]
            expected_result["properties"]["image_id"] = actual_image_id

            actual_bounds_imcoords = actual_result["properties"]["bounds_imcoords"]
            expected_result["properties"]["bounds_imcoords"] = actual_bounds_imcoords

    def test_predict_flood_model(self):
        """
        Test the flood model prediction using a sample image.

        This test sends a sample image in a POST request to the `/invocations` endpoint
        and verifies that the GeoJSON result matches the expected model output.

        The `compare_two_geojson_results` method is used to assert that the predicted
        result is correct after accounting for differences in `image_id` and
        `bounds_imcoords`.
        """
        data_binary = open("test/data/test-model.tif", "rb")
        response = self.client.post("/invocations", data=data_binary)

        assert response.status_code == 200

        sample_output = "test/data/sample_flood_model_output.geojson"
        with open(sample_output, "r") as model_output_geojson:
            expected_json_result = json.loads(model_output_geojson.read())

        actual_geojson_result = json.loads(response.data)
        self.compare_two_geojson_results(actual_geojson_result, expected_json_result)

    def test_predict_bad_data_file(self):
        """
        Test the flood model's response to invalid data input.

        Sends a `None` object in the POST request to the `/invocations` endpoint and
        verifies that the response status code is 400, indicating that the request
        is invalid.
        """
        data_binary = None
        response = self.client.post("/invocations", data=data_binary)

        assert response.status_code == 400
