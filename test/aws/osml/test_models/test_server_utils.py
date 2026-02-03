#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

import logging
from unittest.mock import MagicMock, patch

from flask import Flask

from aws.osml.test_models.server_utils import (
    build_flask_app,
    build_logger,
    detect_to_feature,
    parse_custom_attributes,
    parse_custom_attributes_header,
    setup_server,
    simulate_model_latency,
)


@patch("sys.stdout")  # Patch stdout to prevent actual writing to console
def test_build_logger(mock_stdout):
    # Test default logger creation
    logger = build_logger()
    assert isinstance(logger, logging.Logger)
    assert logger.level == logging.INFO
    assert logger.hasHandlers()

    # Test logger with custom log level
    logger = build_logger(logging.DEBUG)
    assert logger.level == logging.DEBUG


@patch("waitress.serve")
def test_setup_server(mock_serve):
    # Test that setup_server correctly configures and starts the Waitress server
    app = Flask(__name__)
    app.logger.debug = MagicMock()

    setup_server(app)

    app.logger.debug.assert_called_once_with("Initializing OSML Model Flask server on port 8080!")
    mock_serve.assert_called_once_with(app, host="0.0.0.0", port=8080, clear_untrusted_proxy_headers=True)


def test_build_flask_app():
    # Mock the logger
    logger = build_logger()

    # Build the Flask app with the mock logger
    app = build_flask_app(logger)

    assert isinstance(app, Flask)
    assert app.logger.level == logger.level
    assert len(app.logger.handlers) == len(logger.handlers)
    for handler in logger.handlers:
        assert handler in app.logger.handlers


def test_detect_to_feature():
    bbox = [10.0, 20.0, 30.0, 40.0]
    mask = [[10.0, 20.0], [30.0, 40.0], [50.0, 60.0]]
    score = 0.95
    detection_type = "aircraft"

    # Test with mask provided
    feature = detect_to_feature(bbox, mask, score, detection_type)
    assert feature["type"] == "Feature"
    assert feature["geometry"] is None  # geometry is now None
    assert feature["properties"]["imageBBox"] == bbox
    assert feature["properties"]["featureClasses"] == [{"iri": detection_type, "score": score}]
    assert feature["properties"]["imageGeometry"] == {"type": "Polygon", "coordinates": [mask]}
    assert "id" in feature
    assert "image_id" in feature["properties"]

    # Test without mask
    feature_no_mask = detect_to_feature(bbox, None, score, detection_type)
    assert feature_no_mask["properties"]["imageGeometry"] == {"type": "Point", "coordinates": [0.0, 0.0]}

    # Test with default parameters
    feature_default = detect_to_feature(bbox)
    assert feature_default["properties"]["featureClasses"] == [{"iri": "sample_object", "score": 1.0}]
    assert feature_default["properties"]["imageGeometry"] == {"type": "Point", "coordinates": [0.0, 0.0]}


def test_parse_custom_attributes_with_values():
    """Test parsing custom attributes with valid key-value pairs"""
    app = Flask(__name__)
    with app.test_request_context(
        headers={"X-Amzn-SageMaker-Custom-Attributes": "mock_latency_mean=500,mock_latency_std=50,trace_id=abc123"}
    ):
        attributes = parse_custom_attributes()
        assert attributes["mock_latency_mean"] == "500"
        assert attributes["mock_latency_std"] == "50"
        assert attributes["trace_id"] == "abc123"


def test_parse_custom_attributes_header():
    attributes = parse_custom_attributes_header("key1=value1,key2=value2")
    assert attributes["key1"] == "value1"
    assert attributes["key2"] == "value2"


def test_parse_custom_attributes_empty_header():
    """Test parsing custom attributes when header is not present"""
    app = Flask(__name__)
    with app.test_request_context():
        attributes = parse_custom_attributes()
        assert attributes == {}


def test_parse_custom_attributes_with_spaces():
    """Test parsing custom attributes with extra whitespace"""
    app = Flask(__name__)
    with app.test_request_context(headers={"X-Amzn-SageMaker-Custom-Attributes": "key1 = value1 , key2= value2"}):
        attributes = parse_custom_attributes()
        assert attributes["key1"] == "value1"
        assert attributes["key2"] == "value2"


def test_parse_custom_attributes_malformed():
    """Test parsing custom attributes with malformed input"""
    app = Flask(__name__)
    with app.test_request_context(headers={"X-Amzn-SageMaker-Custom-Attributes": "invalid_format"}):
        attributes = parse_custom_attributes()
        # Malformed pairs without '=' should be skipped
        assert attributes == {}


@patch("time.sleep")
def test_simulate_model_latency_with_mean_and_std(mock_sleep):
    """Test simulating latency with both mean and std provided"""
    app = Flask(__name__)
    with app.test_request_context(
        headers={"X-Amzn-SageMaker-Custom-Attributes": "mock_latency_mean=500,mock_latency_std=50"}
    ):
        simulate_model_latency()
        # Verify sleep was called
        assert mock_sleep.called
        # Get the sleep duration in milliseconds
        sleep_time_ms = mock_sleep.call_args[0][0] * 1000
        # Should be roughly around 500ms (allowing for randomness)
        assert sleep_time_ms > 0


@patch("time.sleep")
def test_simulate_model_latency_with_mean_only(mock_sleep):
    """Test simulating latency with only mean provided (std should default to 10%)"""
    app = Flask(__name__)
    with app.test_request_context(headers={"X-Amzn-SageMaker-Custom-Attributes": "mock_latency_mean=1000"}):
        simulate_model_latency()
        # Verify sleep was called
        assert mock_sleep.called
        # Get the sleep duration in milliseconds
        sleep_time_ms = mock_sleep.call_args[0][0] * 1000
        # Should be roughly around 1000ms (allowing for randomness with 10% std)
        assert sleep_time_ms > 0


@patch("time.sleep")
def test_simulate_model_latency_no_custom_attributes(mock_sleep):
    """Test that no sleep occurs when custom attributes are missing"""
    app = Flask(__name__)
    with app.test_request_context():
        simulate_model_latency()
        # Verify sleep was NOT called
        mock_sleep.assert_not_called()


@patch("time.sleep")
def test_simulate_model_latency_missing_mean(mock_sleep):
    """Test that no sleep occurs when mock_latency_mean is missing"""
    app = Flask(__name__)
    with app.test_request_context(headers={"X-Amzn-SageMaker-Custom-Attributes": "mock_latency_std=50"}):
        simulate_model_latency()
        # Verify sleep was NOT called
        mock_sleep.assert_not_called()


@patch("time.sleep")
def test_simulate_model_latency_invalid_values(mock_sleep):
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
def test_simulate_model_latency_negative_random_value(mock_sleep, mock_gauss):
    """Test that negative random values are clamped to zero"""
    # Mock gauss to return a negative value
    mock_gauss.return_value = -100
    app = Flask(__name__)
    with app.test_request_context(headers={"X-Amzn-SageMaker-Custom-Attributes": "mock_latency_mean=500"}):
        simulate_model_latency()
        # Verify sleep was called with 0 (negative values should be clamped)
        mock_sleep.assert_called_once_with(0.0)
