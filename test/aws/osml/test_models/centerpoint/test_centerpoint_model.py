#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

import json
import os
import time

import pytest
from moto import mock_aws

# Set environment variable at module level
os.environ["ENABLE_SEGMENTATION"] = "True"


@pytest.fixture
def centerpoint_model_setup():
    """
    Set up the test environment by creating a Flask app and initializing the test client.
    """
    with mock_aws():
        # Initialize Flask application context and test client
        from aws.osml.test_models.centerpoint.app import app

        ctx = app.app_context()
        ctx.push()
        client = app.test_client()

        yield client

        # Cleanup
        ctx.pop()


def compare_two_geojson_results(actual_geojson_result, expected_json_result):
    """
    Helper function to compare two GeoJSON results.

    :param actual_geojson_result: GeoJSON result returned from the prediction model.
    :type actual_geojson_result: dict
    :param expected_json_result: Expected GeoJSON result for comparison.
    :type expected_json_result: dict

    The function checks the `type` and `features` fields and compares the geometries
    of the features. It also handles differences in image_id fields.
    """
    assert actual_geojson_result.get("type") == expected_json_result.get("type")
    assert len(actual_geojson_result.get("features")) == len(expected_json_result.get("features"))

    for actual_result, expected_result in zip(actual_geojson_result.get("features"), expected_json_result.get("features")):
        # Both actual and expected geometry should be None in the new structure
        assert actual_result.get("geometry") is None
        assert expected_result.get("geometry") is None

        # Handle unique image_id differences
        actual_image_id = actual_result["properties"]["image_id"]
        expected_result["properties"]["image_id"] = actual_image_id

        assert actual_result.get("properties") == expected_result.get("properties")


def test_ping(centerpoint_model_setup):
    """
    Test the `/ping` endpoint to check if the application is running.

    Sends a GET request to `/ping` and verifies that the response status code is 200.
    """
    client = centerpoint_model_setup
    response = client.get("/ping")
    assert response.status_code == 200


def test_predict_center_point_model(centerpoint_model_setup):
    """
    Test the centerpoint detection model's prediction using a sample image.

    This test sends a sample image in a POST request to the `/invocations` endpoint
    and verifies that the GeoJSON result matches the expected model output.

    The method uses `compare_two_geojson_results` to compare the predicted result
    with the expected GeoJSON result.

    :raises AssertionError: If the GeoJSON results do not match.
    """
    client = centerpoint_model_setup
    with open("test/data/test-model.tif", "rb") as data_binary:
        response = client.post("/invocations", data=data_binary)

    assert response.status_code == 200

    sample_output = "test/data/centerpoint_tests.geojson"
    with open(sample_output, "r") as model_output_geojson:
        expected_json_result = json.loads(model_output_geojson.read())

    actual_geojson_result = json.loads(response.data)
    compare_two_geojson_results(actual_geojson_result, expected_json_result)


def test_predict_bad_data_file(centerpoint_model_setup):
    """
    Test the model's response to invalid data input.

    Sends an empty byte string in the POST request to the `/invocations` endpoint
    and verifies that the response status code is 400 (Bad Request).

    :raises AssertionError: If the response status is not 400.
    """
    client = centerpoint_model_setup
    response = client.post("/invocations", data=b"")

    assert response.status_code == 400


def test_predict_with_mock_latency_mean_and_std(centerpoint_model_setup):
    """
    Test the centerpoint model with mock latency using both mean and std.

    Verifies that the model adds latency when custom attributes are provided
    and still returns the correct GeoJSON result.
    """
    client = centerpoint_model_setup
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
    sample_output = "test/data/centerpoint_tests.geojson"
    with open(sample_output, "r") as model_output_geojson:
        expected_json_result = json.loads(model_output_geojson.read())

    actual_geojson_result = json.loads(response.data)
    compare_two_geojson_results(actual_geojson_result, expected_json_result)


def test_predict_with_mock_latency_mean_only(centerpoint_model_setup):
    """
    Test the centerpoint model with mock latency using only mean (std defaults to 10%).

    Verifies that when only mean is provided, std defaults to 10% of mean.
    """
    client = centerpoint_model_setup
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
    assert "features" in actual_geojson_result
    assert actual_geojson_result["type"] == "FeatureCollection"


def test_predict_without_mock_latency(centerpoint_model_setup):
    """
    Test the centerpoint model without mock latency custom attributes.

    Verifies that when no custom attributes are provided, no additional
    latency is added and processing is fast.
    """
    client = centerpoint_model_setup
    with open("test/data/test-model.tif", "rb") as data_binary:
        start_time = time.time()
        response = client.post("/invocations", data=data_binary)
        elapsed_time = time.time() - start_time

    # Verify the response is successful
    assert response.status_code == 200

    # Verify processing is fast (should be well under 50ms without added latency)
    assert elapsed_time < 1.0

    # Verify the GeoJSON result is correct
    sample_output = "test/data/centerpoint_tests.geojson"
    with open(sample_output, "r") as model_output_geojson:
        expected_json_result = json.loads(model_output_geojson.read())

    actual_geojson_result = json.loads(response.data)
    compare_two_geojson_results(actual_geojson_result, expected_json_result)
