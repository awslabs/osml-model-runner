#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

import json
import os
import time
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
    os.environ["FLOOD_VOLUME"] = "10"

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

    def test_predict_flood_model(self):
        """
        Test the flood model prediction using a sample image.

        This test sends a sample image in a POST request to the `/invocations` endpoint
        and verifies that the GeoJSON result contains the expected number of features
        (based on FLOOD_VOLUME=10) and has the correct structure.
        """
        with open("test/data/test-model.tif", "rb") as data_binary:
            response = self.client.post("/invocations", data=data_binary)

        assert response.status_code == 200

        actual_geojson_result = json.loads(response.data)

        # Verify the basic structure
        assert actual_geojson_result["type"] == "FeatureCollection"
        assert "features" in actual_geojson_result

        # Verify we get exactly 10 features (FLOOD_VOLUME=10)
        assert len(actual_geojson_result["features"]) == 10

        # Verify each feature has the expected structure
        for feature in actual_geojson_result["features"]:
            assert feature["type"] == "Feature"
            assert feature["geometry"] is None  # New structure has geometry as None
            assert "id" in feature
            assert "properties" in feature

            properties = feature["properties"]
            assert "imageGeometry" in properties
            assert "imageBBox" in properties
            assert "featureClasses" in properties
            assert "modelMetadata" in properties
            assert "image_id" in properties

            # Verify featureClasses structure
            assert len(properties["featureClasses"]) == 1
            assert properties["featureClasses"][0]["iri"] == "sample_object"
            assert 0 <= properties["featureClasses"][0]["score"] <= 1  # Random score between 0 and 1

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

    def test_predict_with_mock_latency_mean_and_std(self):
        """
        Test the flood model with mock latency using both mean and std.

        Verifies that the model adds latency when custom attributes are provided
        and still returns the correct GeoJSON result with expected number of features.
        """
        with open("test/data/test-model.tif", "rb") as data_binary:
            start_time = time.time()
            response = self.client.post(
                "/invocations",
                data=data_binary,
                headers={"X-Amzn-SageMaker-Custom-Attributes": "mock_latency_mean=100,mock_latency_std=10"},
            )
            elapsed_time = time.time() - start_time

        # Verify the response is successful
        assert response.status_code == 200

        # Verify the latency was added (should be at least ~90ms, accounting for some variance)
        assert elapsed_time > 0.05

        # Verify the GeoJSON result is still correct
        actual_geojson_result = json.loads(response.data)
        assert actual_geojson_result["type"] == "FeatureCollection"
        assert len(actual_geojson_result["features"]) == 10

    def test_predict_with_mock_latency_mean_only(self):
        """
        Test the flood model with mock latency using only mean (std defaults to 10%).

        Verifies that when only mean is provided, std defaults to 10% of mean.
        """
        with open("test/data/test-model.tif", "rb") as data_binary:
            start_time = time.time()
            response = self.client.post(
                "/invocations",
                data=data_binary,
                headers={"X-Amzn-SageMaker-Custom-Attributes": "mock_latency_mean=150"},
            )
            elapsed_time = time.time() - start_time

        # Verify the response is successful
        assert response.status_code == 200

        # Verify the latency was added (should be at least ~120ms, accounting for variance)
        assert elapsed_time > 0.08

        # Verify the GeoJSON result is still correct
        actual_geojson_result = json.loads(response.data)
        assert actual_geojson_result["type"] == "FeatureCollection"
        assert len(actual_geojson_result["features"]) == 10

    def test_predict_without_mock_latency(self):
        """
        Test the flood model without mock latency custom attributes.

        Verifies that when no custom attributes are provided, no additional
        latency is added and processing is fast.
        """
        with open("test/data/test-model.tif", "rb") as data_binary:
            start_time = time.time()
            response = self.client.post("/invocations", data=data_binary)
            elapsed_time = time.time() - start_time

        # Verify the response is successful
        assert response.status_code == 200

        # Verify processing is fast (should be well under 1s without added latency)
        assert elapsed_time < 1.0

        # Verify the GeoJSON result is correct
        actual_geojson_result = json.loads(response.data)
        assert actual_geojson_result["type"] == "FeatureCollection"
        assert len(actual_geojson_result["features"]) == 10
