from dataclasses import dataclass
from unittest.mock import ANY

from aws_oversightml_model_runner.photogrammetry import ConstantElevationModel, WorldCoordinate
from aws_oversightml_model_runner.photogrammetry.elevation_model import ElevationModel


def test_constant_elevation_model():
    elevation_model = ConstantElevationModel(10.0)
    world_coordinate = WorldCoordinate([1, 2, 0])
    assert world_coordinate.z == 0.0
    elevation_model.set_elevation(world_coordinate)
    assert world_coordinate.z == 10.0


def test_elevation_model_abstract_methods():
    # to fulfill abstract method unit test
    ElevationModel.__abstractmethods__ = set()

    @dataclass
    class ElevationModelDummy(ElevationModel):
        pass

    d = ElevationModelDummy()
    eval = d.set_elevation(ANY)

    assert not eval
