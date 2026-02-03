#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

import json
from json import JSONDecodeError

import pytest
from urllib3 import Retry
from urllib3.exceptions import MaxRetryError
from urllib3.response import HTTPResponse

# Mock response simulating a successful HTTP response with valid JSON feature collection
MOCK_RESPONSE = HTTPResponse(
    body=json.dumps(
        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "id": "1cc5e6d6-e12f-430d-adf0-8d2276ce8c5a",
                    "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
                    "properties": {
                        "bounds_imcoords": [429, 553, 440, 561],
                        "feature_types": {"ground_motor_passenger_vehicle": 0.2961518168449402},
                        "detection_score": 0.2961518168449402,
                        "image_id": "test-image-id",
                    },
                }
            ],
        }
    ).encode(),
    status=200,
)

# Mock response simulating an HTTP response with invalid JSON
MOCK_BAD_JSON_RESPONSE = HTTPResponse(body="Not a json string".encode(), status=200)


def _open_payload():
    """Helper to open test payload file"""
    return open("./test/data/small.ntf", "rb")


def _build_metrics(mocker):
    """Helper to build mock metrics logger"""
    from aws_embedded_metrics.logger.metrics_logger import MetricsLogger

    metrics = MetricsLogger(resolve_environment=mocker.Mock())
    metrics.set_dimensions = mocker.Mock()
    metrics.put_dimensions = mocker.Mock()
    metrics.put_metric = mocker.Mock()
    return metrics


def _set_mock_response(mock_pool_manager, response):
    """Helper to set mock response"""
    mock_pool_manager.return_value.request.return_value = response


def test_counting_retry_increment():
    """
    Test that CountingRetry increments retry counts.
    """
    from aws.osml.model_runner.inference.http_detector import CountingRetry

    retry = CountingRetry(total=1)
    result = retry.increment(
        method="GET",
        url="http://example.com",
        response=HTTPResponse(status=500),
        error=None,
        _pool=None,
        _stacktrace=None,
    )
    assert result.retry_counts == 1


def test_counting_retry_from_retry():
    """
    Test creating a CountingRetry from a Retry instance.
    """
    from aws.osml.model_runner.inference.http_detector import CountingRetry

    base_retry = Retry(total=2, backoff_factor=1)
    converted = CountingRetry.from_retry(base_retry)
    assert isinstance(converted, CountingRetry)
    assert converted.total == base_retry.total

    assert CountingRetry.from_retry(converted) is converted


def test_http_detector_retry_passthrough():
    """
    Test that passing a CountingRetry preserves the instance.
    """
    from aws.osml.model_runner.inference.http_detector import CountingRetry, HTTPDetector

    retry = CountingRetry(total=1)
    detector = HTTPDetector(endpoint="http://dummy/endpoint", retry=retry)
    assert detector.retry is retry


def test_http_detector_mode_and_builder():
    """
    Test detector mode and builder wiring.
    """
    from aws.osml.model_runner.api import ModelInvokeMode
    from aws.osml.model_runner.inference.http_detector import HTTPDetector, HTTPDetectorBuilder

    detector = HTTPDetector(endpoint="http://dummy/endpoint")
    assert detector.mode == ModelInvokeMode.HTTP_ENDPOINT

    builder = HTTPDetectorBuilder(endpoint="http://dummy/endpoint", endpoint_parameters={"key": "value"})
    built = builder.build()
    assert isinstance(built, HTTPDetector)
    assert built.endpoint == "http://dummy/endpoint"
    assert built.endpoint_parameters == {"key": "value"}


def test_http_detector_retry_override():
    """
    Test that providing a retry policy is wrapped in CountingRetry.
    """
    from aws.osml.model_runner.inference import HTTPDetector
    from aws.osml.model_runner.inference.http_detector import CountingRetry

    detector = HTTPDetector(endpoint="http://dummy/endpoint", retry=Retry(total=1))
    assert isinstance(detector.retry, CountingRetry)
    assert detector.retry.total == 1


def test_find_features(mocker):
    """
    Test the find_features method to verify that the HTTPDetector correctly processes
    a valid HTTP response and returns a feature collection.
    """
    from aws.osml.model_runner.inference import HTTPDetector

    mock_pool_manager = mocker.patch("aws.osml.model_runner.inference.http_detector.urllib3.PoolManager", autospec=True)

    mock_endpoint = "http://dummy/endpoint"
    mock_name = "test"
    feature_detector = HTTPDetector(endpoint=mock_endpoint, name=mock_name)

    # Verify that the detector is correctly initialized
    assert feature_detector.name == mock_name

    # Mock the HTTP response
    _set_mock_response(mock_pool_manager, MOCK_RESPONSE)

    with _open_payload() as image_file:
        # Call the method and verify the response
        feature_collection = feature_detector.find_features(image_file)
        assert feature_collection["type"] == "FeatureCollection"
        assert len(feature_collection["features"]) == 1


def test_find_features_with_headers_and_metrics(mocker):
    """
    Test custom headers and metric logging in find_features.
    """
    from aws.osml.model_runner.inference import HTTPDetector

    mock_pool_manager = mocker.patch("aws.osml.model_runner.inference.http_detector.urllib3.PoolManager", autospec=True)

    _set_mock_response(mock_pool_manager, MOCK_RESPONSE)
    feature_detector = HTTPDetector(
        endpoint="http://dummy/endpoint",
        endpoint_parameters={"CustomAttributes": "a=b", "Extra": 123},
    )

    metrics = _build_metrics(mocker)
    with _open_payload() as image_file:
        feature_collection = HTTPDetector.find_features.__wrapped__(  # type: ignore[attr-defined]
            feature_detector, image_file, metrics
        )

    assert feature_collection["type"] == "FeatureCollection"
    mock_pool_manager.return_value.request.assert_called_once()
    _, kwargs = mock_pool_manager.return_value.request.call_args
    assert kwargs["headers"]["X-Amzn-SageMaker-Custom-Attributes"] == "a=b"
    assert kwargs["headers"]["Extra"] == "123"
    metrics.set_dimensions.assert_called_once()
    metrics.put_dimensions.assert_called_once()
    metrics.put_metric.assert_called()


def test_find_features_without_metrics_logger(mocker):
    """
    Test find_features with a non-MetricsLogger metrics object.
    """
    from aws.osml.model_runner.inference import HTTPDetector

    mock_pool_manager = mocker.patch("aws.osml.model_runner.inference.http_detector.urllib3.PoolManager", autospec=True)

    _set_mock_response(mock_pool_manager, MOCK_RESPONSE)
    detector = HTTPDetector(endpoint="http://dummy/endpoint")
    metrics = mocker.Mock()

    with _open_payload() as image_file:
        feature_collection = HTTPDetector.find_features.__wrapped__(  # type: ignore[attr-defined]
            detector, image_file, metrics
        )

    assert feature_collection["type"] == "FeatureCollection"
    metrics.set_dimensions.assert_not_called()
    metrics.put_dimensions.assert_not_called()
    metrics.put_metric.assert_called()


def test_find_features_without_endpoint_parameters(mocker):
    """
    Test headers are omitted when no endpoint parameters are set.
    """
    from aws.osml.model_runner.inference import HTTPDetector

    mock_pool_manager = mocker.patch("aws.osml.model_runner.inference.http_detector.urllib3.PoolManager", autospec=True)

    _set_mock_response(mock_pool_manager, MOCK_RESPONSE)
    detector = HTTPDetector(endpoint="http://dummy/endpoint")

    with _open_payload() as image_file:
        feature_collection = detector.find_features(image_file)

    assert feature_collection["type"] == "FeatureCollection"
    _, kwargs = mock_pool_manager.return_value.request.call_args
    assert kwargs["headers"] is None


def test_find_features_custom_attributes_only(mocker):
    """
    Test CustomAttributes header when it's the only endpoint parameter.
    """
    from aws.osml.model_runner.inference import HTTPDetector

    mock_pool_manager = mocker.patch("aws.osml.model_runner.inference.http_detector.urllib3.PoolManager", autospec=True)

    _set_mock_response(mock_pool_manager, MOCK_RESPONSE)
    detector = HTTPDetector(endpoint="http://dummy/endpoint", endpoint_parameters={"CustomAttributes": "a=b"})

    with _open_payload() as image_file:
        detector.find_features(image_file)

    _, kwargs = mock_pool_manager.return_value.request.call_args
    assert kwargs["headers"] == {"X-Amzn-SageMaker-Custom-Attributes": "a=b"}


def test_find_features_RetryError(mocker):
    """
    Test that find_features raises a RetryError when a retry issue occurs during the HTTP request.
    """
    from requests.exceptions import RetryError

    from aws.osml.model_runner.inference import HTTPDetector

    mock_pool_manager = mocker.patch("aws.osml.model_runner.inference.http_detector.urllib3.PoolManager", autospec=True)

    mock_endpoint = "http://dummy/endpoint"
    feature_detector = HTTPDetector(endpoint=mock_endpoint)

    # Simulate a retry error during the HTTP request
    mock_pool_manager.return_value.request.side_effect = RetryError("test RetryError")

    with _open_payload() as image_file:
        # Expecting the function to raise a RetryError
        with pytest.raises(RetryError):
            feature_detector.find_features(image_file)


def test_find_features_MaxRetryError(mocker):
    """
    Test that find_features raises a MaxRetryError when maximum retries are exceeded during the HTTP request.
    """
    from aws.osml.model_runner.inference import HTTPDetector

    mock_pool_manager = mocker.patch("aws.osml.model_runner.inference.http_detector.urllib3.PoolManager", autospec=True)

    mock_endpoint = "http://dummy/endpoint"
    feature_detector = HTTPDetector(endpoint=mock_endpoint)

    # Simulate a maximum retry error during the HTTP request
    mock_pool_manager.return_value.request.side_effect = MaxRetryError("test MaxRetryError", url=mock_endpoint)

    with _open_payload() as image_file:
        # Expecting the function to raise a MaxRetryError
        with pytest.raises(MaxRetryError):
            feature_detector.find_features(image_file)


def test_find_features_JSONDecodeError(mocker):
    """
    Test that find_features raises a JSONDecodeError when the HTTP response contains invalid JSON data.
    """
    from aws.osml.model_runner.inference import HTTPDetector

    mock_pool_manager = mocker.patch("aws.osml.model_runner.inference.http_detector.urllib3.PoolManager", autospec=True)

    mock_endpoint = "http://dummy/endpoint"
    feature_detector = HTTPDetector(endpoint=mock_endpoint)

    # Simulate an HTTP response with invalid JSON content
    _set_mock_response(mock_pool_manager, MOCK_BAD_JSON_RESPONSE)

    with _open_payload() as image_file:
        # Expecting the function to raise a JSONDecodeError
        with pytest.raises(JSONDecodeError):
            feature_detector.find_features(image_file)


def test_find_features_retry_error_records_metrics(mocker):
    """
    Test RetryError handling with metrics logging.
    """
    from requests.exceptions import RetryError

    from aws.osml.model_runner.inference import HTTPDetector

    mock_pool_manager = mocker.patch("aws.osml.model_runner.inference.http_detector.urllib3.PoolManager", autospec=True)

    feature_detector = HTTPDetector(endpoint="http://dummy/endpoint")
    mock_pool_manager.return_value.request.side_effect = RetryError("test RetryError")
    metrics = _build_metrics(mocker)

    with _open_payload() as image_file:
        with pytest.raises(RetryError):
            HTTPDetector.find_features.__wrapped__(feature_detector, image_file, metrics)  # type: ignore[attr-defined]

    metrics.put_metric.assert_called()


def test_find_features_json_decode_records_metrics(mocker):
    """
    Test JSONDecodeError handling with metrics logging.
    """
    from aws.osml.model_runner.inference import HTTPDetector

    mock_pool_manager = mocker.patch("aws.osml.model_runner.inference.http_detector.urllib3.PoolManager", autospec=True)

    feature_detector = HTTPDetector(endpoint="http://dummy/endpoint")
    _set_mock_response(mock_pool_manager, MOCK_BAD_JSON_RESPONSE)
    metrics = _build_metrics(mocker)

    with _open_payload() as image_file:
        with pytest.raises(JSONDecodeError):
            HTTPDetector.find_features.__wrapped__(feature_detector, image_file, metrics)  # type: ignore[attr-defined]

    metrics.put_metric.assert_called()


def test_find_features_max_retry_records_metrics(mocker):
    """
    Test MaxRetryError handling with metrics logging.
    """
    from aws.osml.model_runner.inference import HTTPDetector

    mock_pool_manager = mocker.patch("aws.osml.model_runner.inference.http_detector.urllib3.PoolManager", autospec=True)

    feature_detector = HTTPDetector(endpoint="http://dummy/endpoint")
    mock_pool_manager.return_value.request.side_effect = MaxRetryError("test MaxRetryError", url=feature_detector.endpoint)
    metrics = _build_metrics(mocker)

    with _open_payload() as image_file:
        with pytest.raises(MaxRetryError):
            HTTPDetector.find_features.__wrapped__(feature_detector, image_file, metrics)  # type: ignore[attr-defined]

    metrics.put_metric.assert_called()


def test_set_endpoint_parameters():
    """
    Test setting only valid SageMaker endpoint parameters
    """
    from aws.osml.model_runner.inference import HTTPDetector

    detector = HTTPDetector(endpoint="http-test")
    valid_params = {
        "key1": "value1",
        "key2": "value2",
    }

    detector.set_endpoint_parameters(valid_params)
    assert detector.endpoint_parameters == valid_params
