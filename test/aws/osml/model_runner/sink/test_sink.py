#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

from typing import List

import pytest
from geojson import Feature

from aws.osml.model_runner.api import SinkMode
from aws.osml.model_runner.sink.sink import Sink


class MockSink(Sink):
    """
    A mock implementation of the Sink abstract class to test the interface.
    """

    @staticmethod
    def name() -> str:
        return "MockSink"

    @property
    def mode(self) -> SinkMode:
        return SinkMode.AGGREGATE

    def write(self, image_id: str, features: List[Feature]) -> bool:
        return True


@pytest.fixture
def mock_sink():
    """
    Create an instance of the MockSink class for testing.
    """
    return MockSink()


def test_str_representation(mock_sink):
    """
    Test the `__str__` method.
    Verifies that the string representation combines name and mode.
    """
    expected_str = "MockSink AGGREGATE"
    assert str(mock_sink) == expected_str
