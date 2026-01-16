#!/usr/bin/env python3
#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

import json
import os
import unittest


class TestModelAppTest(unittest.TestCase):
    """
    Unit test case for the test model app.
    """

    def setUp(self):
        from aws.osml.test_models.app import app

        self.app = app
        self.ctx = app.app_context()
        self.ctx.push()
        self.client = app.test_client()

        # Preserve environment to avoid cross-test contamination
        self._saved_default_selection = os.environ.get("DEFAULT_MODEL_SELECTION")
        self._saved_model_selection = os.environ.get("MODEL_SELECTION")
        os.environ.pop("DEFAULT_MODEL_SELECTION", None)
        os.environ.pop("MODEL_SELECTION", None)

    def tearDown(self):
        self.ctx.pop()
        if self._saved_default_selection is not None:
            os.environ["DEFAULT_MODEL_SELECTION"] = self._saved_default_selection
        else:
            os.environ.pop("DEFAULT_MODEL_SELECTION", None)
        if self._saved_model_selection is not None:
            os.environ["MODEL_SELECTION"] = self._saved_model_selection
        else:
            os.environ.pop("MODEL_SELECTION", None)

    def test_ping(self):
        response = self.client.get("/ping")
        self.assertEqual(response.status_code, 200)

    def test_missing_model_selection(self):
        response = self.client.post("/invocations", data=b"")
        self.assertEqual(response.status_code, 400)

    def test_centerpoint_routing(self):
        with open("test/data/test-model.tif", "rb") as data_binary:
            response = self.client.post(
                "/invocations",
                data=data_binary,
                headers={"X-Amzn-SageMaker-Custom-Attributes": "model_selection=centerpoint"},
            )
        self.assertEqual(response.status_code, 200)
        actual_geojson_result = json.loads(response.data)
        self.assertEqual(actual_geojson_result["type"], "FeatureCollection")
        self.assertIn("features", actual_geojson_result)

    def test_flood_routing_with_volume(self):
        with open("test/data/test-model.tif", "rb") as data_binary:
            response = self.client.post(
                "/invocations",
                data=data_binary,
                headers={"X-Amzn-SageMaker-Custom-Attributes": "model_selection=flood,flood_volume=5"},
            )
        self.assertEqual(response.status_code, 200)
        actual_geojson_result = json.loads(response.data)
        self.assertEqual(actual_geojson_result["type"], "FeatureCollection")
        self.assertEqual(len(actual_geojson_result["features"]), 5)

    def test_failure_routing(self):
        with open("test/data/test-model.tif", "rb") as data_binary:
            response = self.client.post(
                "/invocations",
                data=data_binary,
                headers={"X-Amzn-SageMaker-Custom-Attributes": "model_selection=failure"},
            )
        self.assertEqual(response.status_code, 200)
        actual_geojson_result = json.loads(response.data)
        self.assertEqual(actual_geojson_result["type"], "FeatureCollection")
        self.assertIn("features", actual_geojson_result)


if __name__ == "__main__":
    unittest.main()
