#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

import json
import os
import shutil
import tempfile

import numpy as np
import pytest
from moto import mock_aws
from osgeo import gdal

# Set environment variable at module level
os.environ["ENABLE_SEGMENTATION"] = "True"


def create_image(tmp_dir, color, name):
    """Create a test image with the given color and name."""
    filepath = os.path.join(tmp_dir, f"{name}_image.tiff")
    driver = gdal.GetDriverByName("GTiff")
    dataset = driver.Create(filepath, 200, 100, 3, gdal.GDT_Byte)

    r, g, b = color
    dataset.GetRasterBand(1).WriteArray(np.full((100, 200), r, dtype=np.uint8))
    dataset.GetRasterBand(2).WriteArray(np.full((100, 200), g, dtype=np.uint8))
    dataset.GetRasterBand(3).WriteArray(np.full((100, 200), b, dtype=np.uint8))

    dataset = None
    return filepath


@pytest.fixture
def failure_model_setup():
    """
    Set up the test environment by creating a Flask app and initializing the test client.
    """
    with mock_aws():
        # Initialize Flask application context and test client
        from aws.osml.test_models.failure.app import app

        ctx = app.app_context()
        ctx.push()
        client = app.test_client()

        # Create temporary directory for test images
        tmp_dir = tempfile.mkdtemp()

        # Create all test images
        test_images = {
            "red": create_image(tmp_dir, (255, 0, 0), "red"),
            "green": create_image(tmp_dir, (0, 255, 0), "green"),
            "purple": create_image(tmp_dir, (255, 0, 255), "purple"),
            "blue": create_image(tmp_dir, (0, 0, 255), "blue"),
            "normal": create_image(tmp_dir, (128, 128, 128), "normal"),
        }

        yield client, test_images

        # Cleanup
        ctx.pop()
        shutil.rmtree(tmp_dir)


def test_ping(failure_model_setup):
    """
    Test the `/ping` endpoint to check if the application is running.

    Sends a GET request to `/ping` and verifies that the response status code is 200.
    """
    client, test_images = failure_model_setup
    response = client.get("/ping")
    assert response.status_code == 200


def test_blue_timeout(failure_model_setup):
    """
    Test sending the blue tiff we receive an HTTP Timeout Status Code
    408 from the badModel
    """
    client, test_images = failure_model_setup
    with open(test_images["blue"], "rb") as blue_pixels:
        response = client.post("/invocations", data=blue_pixels, headers={"Content-Type": "image/tiff"})
        assert response.status_code == 408


def test_green_malformed(failure_model_setup):
    """
    Test sending the green tiff and we receive generic malformed JSON that cannot be successfully decoded
    """
    client, test_images = failure_model_setup
    with open(test_images["green"], "rb") as green_pixels:
        response = client.post("/invocations", data=green_pixels, headers={"Content-Type": "image/tiff"})
        assert response.status_code == 200
        with pytest.raises(json.JSONDecodeError):
            json.loads(response.data.decode())


def test_purple_not_geojson(failure_model_setup):
    """
    Test sending the purple tiff and we recieve a JSON but does not conform to expected GeoJSON
    """
    client, test_images = failure_model_setup
    with open(test_images["purple"], "rb") as purple_pixels:
        response = client.post("/invocations", data=purple_pixels, headers={"Content-Type": "image/tiff"})
        assert response.status_code == 200

        # Load the response data as JSON
        response_data = json.loads(response.data.decode())

        # Contains invalid key
        assert "invalid_key" in response_data


def test_red_server_error(failure_model_setup):
    """
    Test sending the red tiff we receive an HTTP Server Error Status Code
    500 from the badModel
    """
    client, test_images = failure_model_setup
    with open(test_images["red"], "rb") as red_pixels:
        response = client.post("/invocations", data=red_pixels, headers={"Content-Type": "image/tiff"})
        assert response.status_code == 500


def test_normal_image(failure_model_setup):
    """
    Test sending the normal tiff we receive an HTTP Success Status Code
    200 from the badModel
    """
    client, test_images = failure_model_setup
    with open(test_images["normal"], "rb") as normal_pixels:
        response = client.post("/invocations", data=normal_pixels, headers={"Content-Type": "image/tiff"})
        assert response.status_code == 200
