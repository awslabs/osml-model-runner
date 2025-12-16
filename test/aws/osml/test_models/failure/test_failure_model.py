#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

import json
import os
import shutil
import tempfile
import unittest

import numpy as np
from moto import mock_aws
from osgeo import gdal


@mock_aws
class FailureModelTest(unittest.TestCase):
    """
    Unit test case for testing Flask endpoints in the bad model app.

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
        from aws.osml.test_models.failure.app import app

        self.ctx = app.app_context()
        self.ctx.push()
        self.client = app.test_client()

        # Create temporary directory for test images
        self.tmp_dir = tempfile.mkdtemp()
        self.test_images = self._create_test_images()
        # Create a normal test image (gray/mixed colors that won't trigger error conditions)
        self.test_images["normal"] = self._create_image((128, 128, 128), "normal")

    def tearDown(self):
        """
        Clean up the test environment by popping the Flask app context.
        """
        self.ctx.pop()
        shutil.rmtree(self.tmp_dir)

    def _create_test_images(self):
        """Create all test images for badModel testing"""
        return {
            "red": self._create_image((255, 0, 0), "red"),
            "green": self._create_image((0, 255, 0), "green"),
            "purple": self._create_image((255, 0, 255), "purple"),
            "blue": self._create_image((0, 0, 255), "blue"),
        }

    def _create_image(self, color, name):
        """Create a test image with the given color and name."""
        filepath = os.path.join(self.tmp_dir, f"{name}_image.tiff")
        driver = gdal.GetDriverByName("GTiff")
        dataset = driver.Create(filepath, 200, 100, 3, gdal.GDT_Byte)

        r, g, b = color
        dataset.GetRasterBand(1).WriteArray(np.full((100, 200), r, dtype=np.uint8))
        dataset.GetRasterBand(2).WriteArray(np.full((100, 200), g, dtype=np.uint8))
        dataset.GetRasterBand(3).WriteArray(np.full((100, 200), b, dtype=np.uint8))

        dataset = None
        return filepath

    def test_ping(self):
        """
        Test the `/ping` endpoint to check if the application is running.

        Sends a GET request to `/ping` and verifies that the response status code is 200.
        """
        response = self.client.get("/ping")
        self.assertEqual(response.status_code, 200)

    # Test Specific Color --> Error block
    def test_blue_timeout(self):
        """
        Test sending the blue tiff we receive an HTTP Timeout Status Code
        408 from the badModel
        """
        with open(self.test_images["blue"], "rb") as blue_pixels:
            response = self.client.post("/invocations", data=blue_pixels, headers={"Content-Type": "image/tiff"})
            self.assertEqual(response.status_code, 408)

    def test_green_malformed(self):
        """
        Test sending the green tiff and we receive generic malformed JSON that cannot be successfully decoded
        """
        with open(self.test_images["green"], "rb") as green_pixels:
            response = self.client.post("/invocations", data=green_pixels, headers={"Content-Type": "image/tiff"})
            self.assertEqual(response.status_code, 200)
            with self.assertRaises(json.JSONDecodeError):
                json.loads(response.data.decode())

    def test_purple_not_geojson(self):
        """
        Test sending the purple tiff and we recieve a JSON but does not conform to expected GeoJSON
        """
        with open(self.test_images["purple"], "rb") as purple_pixels:
            response = self.client.post("/invocations", data=purple_pixels, headers={"Content-Type": "image/tiff"})
            self.assertEqual(response.status_code, 200)

            # Load the response data as JSON
            response_data = json.loads(response.data.decode())

            # Contains invalid key
            self.assertIn("invalid_key", response_data)

    def test_red_server_error(self):
        """
        Test sending the red tiff we receive an HTTP Server Error Status Code
        500 from the badModel
        """
        with open(self.test_images["red"], "rb") as red_pixels:
            response = self.client.post("/invocations", data=red_pixels, headers={"Content-Type": "image/tiff"})
            self.assertEqual(response.status_code, 500)

    def test_normal_image(self):
        """
        Test sending the normal tiff we receive an HTTP Success Status Code
        200 from the badModel
        """
        with open(self.test_images["normal"], "rb") as normal_pixels:
            response = self.client.post("/invocations", data=normal_pixels, headers={"Content-Type": "image/tiff"})
            self.assertEqual(response.status_code, 200)
