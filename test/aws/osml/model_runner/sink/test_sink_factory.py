#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

import json

import pytest
from geojson import Feature

from aws.osml.model_runner.api import InvalidImageRequestException
from aws.osml.model_runner.sink.sink_factory import SinkFactory


@pytest.fixture
def sample_feature_list():
    """
    Builds a sample list of geojson features for testing.
    """
    return [Feature(geometry={"type": "Point", "coordinates": [102.0, 0.5]}, properties={"prop0": "value0"})]


@pytest.fixture
def destinations():
    """
    Setup mock destinations for testing.
    """
    return {
        "s3": json.dumps([{"type": "S3", "bucket": "test-bucket", "prefix": "test-prefix"}]),
        "kinesis": json.dumps([{"type": "Kinesis", "stream": "test-stream"}]),
        "mixed": json.dumps(
            [
                {"type": "S3", "bucket": "test-bucket", "prefix": "test-prefix"},
                {"type": "Kinesis", "stream": "test-stream"},
            ]
        ),
    }


def test_s3_sink(mocker, sample_feature_list, destinations):
    """
    Test sink features writing to S3 sink.
    Ensures that `sink_features` writes correctly to S3 using the SinkFactory.
    """
    mock_write = mocker.patch("aws.osml.model_runner.sink.s3_sink.S3Sink.write", return_value=True)
    result = SinkFactory.sink_features("test-job-id", destinations["s3"], sample_feature_list)
    assert result
    mock_write.assert_called_once()


def test_kinesis_sink(mocker, sample_feature_list, destinations):
    """
    Test sink features writing to Kinesis sink.
    Ensures that `sink_features` writes correctly to Kinesis using the SinkFactory.
    """
    mock_write = mocker.patch("aws.osml.model_runner.sink.kinesis_sink.KinesisSink.write", return_value=True)
    result = SinkFactory.sink_features("test-job-id", destinations["kinesis"], sample_feature_list)
    assert result
    mock_write.assert_called_once()


def test_mixed_sinks_success(mocker, sample_feature_list, destinations):
    """
    Test sink features with mixed S3 and Kinesis sinks.
    Ensures that `sink_features` can handle both sinks and reports success when both write correctly.
    """
    mock_s3_write = mocker.patch("aws.osml.model_runner.sink.s3_sink.S3Sink.write", return_value=True)
    mock_kinesis_write = mocker.patch("aws.osml.model_runner.sink.kinesis_sink.KinesisSink.write", return_value=True)
    result = SinkFactory.sink_features("test-job-id", destinations["mixed"], sample_feature_list)
    assert result
    mock_s3_write.assert_called_once()
    mock_kinesis_write.assert_called_once()


def test_mixed_sinks_partial_success(mocker, sample_feature_list, destinations):
    """
    Test sink features with one successful and one failed sink.
    Ensures that `sink_features` continues when one sink fails but another succeeds.
    """
    mock_s3_write = mocker.patch("aws.osml.model_runner.sink.s3_sink.S3Sink.write", return_value=False)
    mock_kinesis_write = mocker.patch("aws.osml.model_runner.sink.kinesis_sink.KinesisSink.write", return_value=True)
    result = SinkFactory.sink_features("test-job-id", destinations["mixed"], sample_feature_list)
    assert result
    mock_s3_write.assert_called_once()
    mock_kinesis_write.assert_called_once()


def test_mixed_sinks_failure(mocker, sample_feature_list, destinations):
    """
    Test sink features when both S3 and Kinesis write operations fail.
    Ensures that `sink_features` reports failure when neither sink can write.
    """
    mock_s3_write = mocker.patch("aws.osml.model_runner.sink.s3_sink.S3Sink.write", return_value=False)
    mock_kinesis_write = mocker.patch("aws.osml.model_runner.sink.kinesis_sink.KinesisSink.write", return_value=False)
    result = SinkFactory.sink_features("test-job-id", destinations["mixed"], sample_feature_list)
    assert not result
    mock_s3_write.assert_called_once()
    mock_kinesis_write.assert_called_once()


def test_invalid_sink_type():
    """
    Test outputs_to_sinks with an invalid sink type.
    Ensures that the method raises an InvalidImageRequestException for unknown sink types.
    """
    invalid_destination = json.dumps([{"type": "InvalidType", "bucket": "test-bucket", "prefix": "test-prefix"}])
    with pytest.raises(InvalidImageRequestException):
        SinkFactory.outputs_to_sinks(json.loads(invalid_destination))


def test_no_outputs_defined(sample_feature_list):
    """
    Test sink_features with no output destinations.
    Ensures that the method raises an InvalidImageRequestException when no destinations are provided.
    """
    with pytest.raises(InvalidImageRequestException):
        SinkFactory.sink_features("test-job-id", "", sample_feature_list)
