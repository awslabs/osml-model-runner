#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

import logging
import unittest
from unittest.mock import MagicMock, patch

from flask import Flask

from aws.osml.test_models.server_utils import build_flask_app, build_logger, detect_to_feature, setup_server


class TestServerUtils(unittest.TestCase):
    @patch("sys.stdout")  # Patch stdout to prevent actual writing to console
    def test_build_logger(self, mock_stdout):
        # Test default logger creation
        logger = build_logger()
        self.assertIsInstance(logger, logging.Logger)
        self.assertEqual(logger.level, logging.INFO)
        self.assertTrue(logger.hasHandlers())

        # Test logger with custom log level
        logger = build_logger(logging.DEBUG)
        self.assertEqual(logger.level, logging.DEBUG)

    @patch("waitress.serve")
    def test_setup_server(self, mock_serve):
        # Test that setup_server correctly configures and starts the Waitress server
        app = Flask(__name__)
        app.logger.debug = MagicMock()

        setup_server(app)

        app.logger.debug.assert_called_once_with("Initializing OSML Model Flask server on port 8080!")
        mock_serve.assert_called_once_with(app, host="0.0.0.0", port=8080, clear_untrusted_proxy_headers=True)

    def test_build_flask_app(self):
        # Mock the logger
        logger = build_logger()

        # Build the Flask app with the mock logger
        app = build_flask_app(logger)

        self.assertIsInstance(app, Flask)
        self.assertEqual(app.logger.level, logger.level)
        self.assertEqual(len(app.logger.handlers), len(logger.handlers))
        for handler in logger.handlers:
            self.assertIn(handler, app.logger.handlers)

    def test_detect_to_feature(self):
        bbox = [10.0, 20.0, 30.0, 40.0]
        mask = [[10.0, 20.0], [30.0, 40.0], [50.0, 60.0]]
        score = 0.95
        detection_type = "aircraft"

        # Test with mask provided
        feature = detect_to_feature(bbox, mask, score, detection_type)
        self.assertEqual(feature["type"], "Feature")
        self.assertEqual(feature["geometry"]["type"], "Point")
        self.assertEqual(feature["properties"]["bounds_imcoords"], bbox)
        self.assertEqual(feature["properties"]["detection_score"], score)
        self.assertEqual(feature["properties"]["feature_types"], {detection_type: score})
        self.assertIn("geom_imcoords", feature["properties"])
        self.assertEqual(feature["properties"]["geom_imcoords"], mask)

        # Test without mask
        feature_no_mask = detect_to_feature(bbox, None, score, detection_type)
        self.assertNotIn("geom_imcoords", feature_no_mask["properties"])

        # Test with default parameters
        feature_default = detect_to_feature(bbox)
        self.assertEqual(feature_default["properties"]["detection_score"], 1.0)
        self.assertEqual(feature_default["properties"]["feature_types"], {"sample_object": 1.0})


if __name__ == "__main__":
    unittest.main()
