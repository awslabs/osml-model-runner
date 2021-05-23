from abc import ABC, abstractmethod
from typing import List

from geojson import Feature, Point


class CameraModel(ABC):

    def __init__(self):
        pass

    @abstractmethod
    def image_to_world(self, xy_coord):
        pass

    # TODO: Add world_to_image

    def geolocate_features(self, features: List[Feature]):
        for feature in features:
            bbox = feature['properties']['bounds_imcoords']
            center_xy = bbox[0] + (bbox[2] - bbox[0]) / 2, bbox[1] + (bbox[3] - bbox[1]) / 2
            center_lonlat = self.image_to_world(center_xy)
            feature['geometry'] = Point(center_lonlat)


class GDALAffineCameraModel(CameraModel):

    def __init__(self, transform):
        """
        GDAL provides a simple affine transform used to convert XY pixel values to longitude, latitude. See
        https://gdal.org/tutorials/geotransforms_tut.html

        transform[0] x-coordinate of the upper-left corner of the upper-left pixel.
        transform[1] w-e pixel resolution / pixel width.
        transform[2] row rotation (typically zero).
        transform[3] y-coordinate of the upper-left corner of the upper-left pixel.
        transform[4] column rotation (typically zero).
        transform[5] n-s pixel resolution / pixel height (negative value for a north-up image).

        The necessary transform matrix can be obtained from a dataset using the GetGeoTransform() operation.

        :param transform: the 6 coefficients of the affine transform
        """
        self.transform = transform

    def image_to_world(self, xy_coord):
        longitude = self.transform[0] + xy_coord[0] * self.transform[1] + xy_coord[1] * self.transform[2]
        latitude = self.transform[3] + xy_coord[0] * self.transform[4] + xy_coord[1] * self.transform[5]
        return longitude, latitude

# TODO: Add better camera models, RPC etc.
