import abc
from enum import auto
from typing import List

from geojson import Feature

from aws_oversightml_model_runner.common import AutoStringEnum


class SinkMode(str, AutoStringEnum):
    """
    Enumeration defining different sink output modes.
    """

    AGGREGATE = auto()
    STREAMING = auto()


class Sink(abc.ABC):
    """The mechanism by which detected features are sent to their destination."""

    def __str__(self) -> str:
        return f"{self.name()} {self.mode.value}"

    @staticmethod
    @abc.abstractmethod
    def name() -> str:
        """The name of the sink."""

    @property
    @abc.abstractmethod
    def mode(self) -> SinkMode:
        """
        The write mode of the sink. Either Streaming (per tile results)
        or Aggregate (per image results).
        """

    @abc.abstractmethod
    def write(self, image_id: str, features: List[Feature]) -> None:
        """Write feature list for given image id to the sink."""
