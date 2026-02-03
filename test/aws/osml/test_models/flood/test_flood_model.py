#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

import json
import os
import time

import pytest
from moto import mock_aws

# Set flood volume for testing
os.environ["FLOOD_VOLUME"] = "10"


@pytest.fixture
def flood_model_setup():
    """
    Set up the test environment before each test case.

    This method initializes the Flask application context and creates
    a test client to simulate requests.
    """
    with mock_aws():
        # Initialize Flask application context and test client
        from aws.osml.test_models.flood.app import app

        ctx = app.app_context()
        ctx.push()
        client = app.test_client()

        yield client

        # Cleanup
        ctx.pop()


def test_ping(flood_model_setup):
    """
    Test the `/ping` endpoint to check if the application is running.

    Sends a GET request to the `/ping` endpoint and verifies that the response
    status code is 200, indicating that the app is alive and healthy.
    """
    client = flood_model_setup
    response = client.get("/ping")
    assert response.status_code == 200


def test_predict_flood_model(flood_model_setup):
    """
    Test the flood model prediction using a sample image.

    This test sends a sample image in a POST request to the `/invocations` endpoint
    and verifies that the GeoJSON result contains the expected number of features
    (based on FLOOD_VOLUME=10) and has the correct structure.
    """
    client = flood_model_setup
    with open("test/data/test-model.tif", "rb") as data_binary:
        response = client.post("/invocations", data=data_binary)

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


def test_predict_bad_data_file(flood_model_setup):
    """
    Test the flood model's response to invalid data input.

    Sends a `None` object in the POST request to the `/invocations` endpoint and
    verifies that the response status code is 400, indicating that the request
    is invalid.
    """
    client = flood_model_setup
    data_binary = None
    response = client.post("/invocations", data=data_binary)

    assert response.status_code == 400


def test_predict_with_mock_latency_mean_and_std(flood_model_setup):
    """
    Test the flood model with mock latency using both mean and std.

    Verifies that the model adds latency when custom attributes are provided
    and still returns the correct GeoJSON result with expected number of features.
    """
    client = flood_model_setup
    with open("test/data/test-model.tif", "rb") as data_binary:
        start_time = time.time()
        response = client.post(
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


def test_predict_with_mock_latency_mean_only(flood_model_setup):
    """
    Test the flood model with mock latency using only mean (std defaults to 10%).

    Verifies that when only mean is provided, std defaults to 10% of mean.
    """
    client = flood_model_setup
    with open("test/data/test-model.tif", "rb") as data_binary:
        start_time = time.time()
        response = client.post(
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


def test_predict_without_mock_latency(flood_model_setup):
    """
    Test the flood model without mock latency custom attributes.

    Verifies that when no custom attributes are provided, no additional
    latency is added and processing is fast.
    """
    client = flood_model_setup
    with open("test/data/test-model.tif", "rb") as data_binary:
        start_time = time.time()
        response = client.post("/invocations", data=data_binary)
        elapsed_time = time.time() - start_time

    # Verify the response is successful
    assert response.status_code == 200

    # Verify processing is fast (should be well under 1s without added latency)
    assert elapsed_time < 1.0

    # Verify the GeoJSON result is correct
    actual_geojson_result = json.loads(response.data)
    assert actual_geojson_result["type"] == "FeatureCollection"
    assert len(actual_geojson_result["features"]) == 10
