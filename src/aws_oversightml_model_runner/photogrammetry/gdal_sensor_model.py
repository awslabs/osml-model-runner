from math import degrees, radians
from typing import List

from .coordinates import GeodeticWorldCoordinate, ImageCoordinate
from .sensor_model import SensorModel


class GDALAffineSensorModel(SensorModel):
    """
    GDAL provides a simple affine transform used to convert XY pixel values to longitude,
    latitude. See https://gdal.org/tutorials/geotransforms_tut.html

    transform[0] x-coordinate of the upper-left corner of the upper-left pixel.
    transform[1] w-e pixel resolution / pixel width.
    transform[2] row rotation (typically zero).
    transform[3] y-coordinate of the upper-left corner of the upper-left pixel.
    transform[4] column rotation (typically zero).
    transform[5] n-s pixel resolution / pixel height (negative value for a north-up image).

    The necessary transform matrix can be obtained from a dataset using the GetGeoTransform() operation.
    """

    def __init__(self, transform: List) -> None:
        """
        Construct the sensor model from the affine transform provided by transform

        :param transform: the 6 coefficients of the affine transform
        """
        super().__init__()
        self.transform = transform
        self.inv_transform = GDALAffineSensorModel.invert_geo_transform(transform)

    def image_to_world(self, image_coordinate: ImageCoordinate) -> GeodeticWorldCoordinate:
        longitude = (
            self.transform[0]
            + image_coordinate.x * self.transform[1]
            + image_coordinate.y * self.transform[2]
        )
        latitude = (
            self.transform[3]
            + image_coordinate.x * self.transform[4]
            + image_coordinate.y * self.transform[5]
        )

        return GeodeticWorldCoordinate([radians(longitude), radians(latitude), 0.0])

    def world_to_image(self, world_coordinate: GeodeticWorldCoordinate) -> ImageCoordinate:
        x = (
            self.inv_transform[0]
            + degrees(world_coordinate.longitude) * self.inv_transform[1]
            + degrees(world_coordinate.latitude) * self.inv_transform[2]
        )
        y = (
            self.inv_transform[3]
            + degrees(world_coordinate.longitude) * self.inv_transform[4]
            + degrees(world_coordinate.latitude) * self.inv_transform[5]
        )
        return ImageCoordinate([x, y])

    @staticmethod
    def invert_geo_transform(gt_in: List) -> List:
        # we assume a 3rd row that is [1 0 0]

        # Compute determinate
        det = gt_in[1] * gt_in[5] - gt_in[2] * gt_in[4]

        if abs(det) < 0.000000000000001:
            raise ValueError(
                "GeoTransform can not be inverted. Not a valid matrix for a sensor model."
            )

        inv_det = 1.0 / det

        # compute adjoint, and divide by determinate
        gt_out = [0, 0, 0, 0, 0, 0]
        gt_out[1] = gt_in[5] * inv_det
        gt_out[4] = -gt_in[4] * inv_det

        gt_out[2] = -gt_in[2] * inv_det
        gt_out[5] = gt_in[1] * inv_det

        gt_out[0] = (gt_in[2] * gt_in[3] - gt_in[0] * gt_in[5]) * inv_det
        gt_out[3] = (-gt_in[1] * gt_in[3] + gt_in[0] * gt_in[4]) * inv_det

        return gt_out
