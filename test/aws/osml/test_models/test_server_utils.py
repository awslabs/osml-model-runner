#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

import logging
import unittest
from unittest.mock import MagicMock, patch

from flask import Flask

from aws.osml.test_models.server_utils import (
    build_flask_app,
    build_logger,
    detect_to_feature,
    parse_custom_attributes,
    setup_server,
    simulate_model_latency,
)


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
        self.assertIsNone(feature["geometry"])  # geometry is now None
        self.assertEqual(feature["properties"]["imageBBox"], bbox)
        self.assertEqual(feature["properties"]["featureClasses"], [{"iri": detection_type, "score": score}])
        self.assertEqual(feature["properties"]["imageGeometry"], {"type": "Polygon", "coordinates": [mask]})
        self.assertIn("id", feature)
        self.assertIn("image_id", feature["properties"])

        # Test without mask
        feature_no_mask = detect_to_feature(bbox, None, score, detection_type)
        self.assertEqual(feature_no_mask["properties"]["imageGeometry"], {"type": "Point", "coordinates": [0.0, 0.0]})

        # Test with default parameters
        feature_default = detect_to_feature(bbox)
        self.assertEqual(feature_default["properties"]["featureClasses"], [{"iri": "sample_object", "score": 1.0}])
        self.assertEqual(feature_default["properties"]["imageGeometry"], {"type": "Point", "coordinates": [0.0, 0.0]})

    def test_parse_custom_attributes_with_values(self):
        """Test parsing custom attributes with valid key-value pairs"""
        app = Flask(__name__)
        with app.test_request_context(
            headers={"X-Amzn-SageMaker-Custom-Attributes": "mock_latency_mean=500,mock_latency_std=50,trace_id=abc123"}
        ):
            attributes = parse_custom_attributes()
            self.assertEqual(attributes["mock_latency_mean"], "500")
            self.assertEqual(attributes["mock_latency_std"], "50")
            self.assertEqual(attributes["trace_id"], "abc123")

    def test_parse_custom_attributes_empty_header(self):
        """Test parsing custom attributes when header is not present"""
        app = Flask(__name__)
        with app.test_request_context():
            attributes = parse_custom_attributes()
            self.assertEqual(attributes, {})

    def test_parse_custom_attributes_with_spaces(self):
        """Test parsing custom attributes with extra whitespace"""
        app = Flask(__name__)
        with app.test_request_context(headers={"X-Amzn-SageMaker-Custom-Attributes": "key1 = value1 , key2= value2"}):
            attributes = parse_custom_attributes()
            self.assertEqual(attributes["key1"], "value1")
            self.assertEqual(attributes["key2"], "value2")

    def test_parse_custom_attributes_malformed(self):
        """Test parsing custom attributes with malformed input"""
        app = Flask(__name__)
        with app.test_request_context(headers={"X-Amzn-SageMaker-Custom-Attributes": "invalid_format"}):
            attributes = parse_custom_attributes()
            # Malformed pairs without '=' should be skipped
            self.assertEqual(attributes, {})

    @patch("time.sleep")
    def test_simulate_model_latency_with_mean_and_std(self, mock_sleep):
        """Test simulating latency with both mean and std provided"""
        app = Flask(__name__)
        with app.test_request_context(
            headers={"X-Amzn-SageMaker-Custom-Attributes": "mock_latency_mean=500,mock_latency_std=50"}
        ):
            simulate_model_latency()
            # Verify sleep was called
            self.assertTrue(mock_sleep.called)
            # Get the sleep duration in milliseconds
            sleep_time_ms = mock_sleep.call_args[0][0] * 1000
            # Should be roughly around 500ms (allowing for randomness)
            self.assertGreater(sleep_time_ms, 0)

    @patch("time.sleep")
    def test_simulate_model_latency_with_mean_only(self, mock_sleep):
        """Test simulating latency with only mean provided (std should default to 10%)"""
        app = Flask(__name__)
        with app.test_request_context(headers={"X-Amzn-SageMaker-Custom-Attributes": "mock_latency_mean=1000"}):
            simulate_model_latency()
            # Verify sleep was called
            self.assertTrue(mock_sleep.called)
            # Get the sleep duration in milliseconds
            sleep_time_ms = mock_sleep.call_args[0][0] * 1000
            # Should be roughly around 1000ms (allowing for randomness with 10% std)
            self.assertGreater(sleep_time_ms, 0)

    @patch("time.sleep")
    def test_simulate_model_latency_no_custom_attributes(self, mock_sleep):
        """Test that no sleep occurs when custom attributes are missing"""
        app = Flask(__name__)
        with app.test_request_context():
            simulate_model_latency()
            # Verify sleep was NOT called
            mock_sleep.assert_not_called()

    @patch("time.sleep")
    def test_simulate_model_latency_missing_mean(self, mock_sleep):
        """Test that no sleep occurs when mock_latency_mean is missing"""
        app = Flask(__name__)
        with app.test_request_context(headers={"X-Amzn-SageMaker-Custom-Attributes": "mock_latency_std=50"}):
            simulate_model_latency()
            # Verify sleep was NOT called
            mock_sleep.assert_not_called()

    @patch("time.sleep")
    def test_simulate_model_latency_invalid_values(self, mock_sleep):
        """Test that no sleep occurs with invalid numeric values"""
        app = Flask(__name__)
        with app.test_request_context(
            headers={"X-Amzn-SageMaker-Custom-Attributes": "mock_latency_mean=invalid,mock_latency_std=bad"}
        ):
            simulate_model_latency()
            # Verify sleep was NOT called due to conversion error
            mock_sleep.assert_not_called()

    @patch("random.gauss")
    @patch("time.sleep")
    def test_simulate_model_latency_negative_random_value(self, mock_sleep, mock_gauss):
        """Test that negative random values are clamped to zero"""
        # Mock gauss to return a negative value
        mock_gauss.return_value = -100
        app = Flask(__name__)
        with app.test_request_context(headers={"X-Amzn-SageMaker-Custom-Attributes": "mock_latency_mean=500"}):
            simulate_model_latency()
            # Verify sleep was called with 0 (negative values should be clamped)
            mock_sleep.assert_called_once_with(0.0)


if __name__ == "__main__":
    unittest.main()
