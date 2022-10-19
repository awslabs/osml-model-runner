from abc import ABC, abstractmethod

from .coordinates import WorldCoordinate


class ElevationModel(ABC):
    """
    An elevation model associates a height z for a given x, y of a world coordinate. It typically provides information
    about the terrain associated with longitude, latitude locations of an ellipsoid but it can also be used to model
    surfaces for other ground domains.
    """

    def __init__(self) -> None:
        pass

    @abstractmethod
    def set_elevation(self, world_coordinate: WorldCoordinate) -> None:
        """
        This method updates the z component of a world coordinate to match the surface elevation at x, y.

        :param world_coordinate: the coordinate to update
        :return: None
        """
        pass


class ConstantElevationModel(ElevationModel):
    """
    A constant elevation model with a single value for all x, y.
    """

    def __init__(self, constant_elevation: float):
        """
        Constructs the constant elevation model.

        :param constant_elevation: the z value for all x, y
        """
        super().__init__()
        self.constant_elevation = constant_elevation

    def set_elevation(self, world_coordinate: WorldCoordinate) -> None:
        """
        Updates world coordinate's z to match the constant elevation.

        :param world_coordinate: the coordinate to update
        :return: None
        """
        world_coordinate.z = self.constant_elevation


# TODO: Add more complex digital elevation models here
