import functools
import logging
import math
from abc import ABC, abstractmethod
from typing import List, Tuple

import geojson
from osgeo import gdal, osr


logger = logging.getLogger(__name__)


class CameraModel(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def image_to_world(self, xy_coord: Tuple) -> Tuple:
        pass

    @abstractmethod
    def world_to_image(self, lonlat_coord: Tuple) -> Tuple:
        pass

    def geolocate_detections(self, features: List[geojson.Feature]):
        for feature in features:
            bbox = feature["properties"]["bounds_imcoords"]
            center_xy = (
                bbox[0] + (bbox[2] - bbox[0]) / 2,
                bbox[1] + (bbox[3] - bbox[1]) / 2,
            )
            center_lonlat = self.image_to_world(center_xy)

            # This converts the coordinates of the bounding box in the image into a polygon in
            # lat/lon space. We may want to make this calculation a little smarter and detect
            # cases where the camera model returns several near identical points for this bounding
            # box. In that case it may be better to create a single simplified point geometry e.g.
            # feature['geometry'] = geojson.Point(center_lonlat)

            polygon_lonlat = (
                self.image_to_world((bbox[0], bbox[1])),
                self.image_to_world((bbox[0], bbox[3])),
                self.image_to_world((bbox[2], bbox[3])),
                self.image_to_world((bbox[2], bbox[1])),
            )
            # Note that for geojson polygons the "coordinates" member must be an array of
            # LinearRing coordinate arrays. For Polygons with multiple rings, the first must be
            # the exterior ring and any others must be interior rings or holes. We only have an
            # exterior ring hence creating an array of the latlon array is appropriate here.
            feature["geometry"] = geojson.Polygon([polygon_lonlat])

            # Geojson features can optionally have a bounding box that contains [min lon, min lat,
            # max lon, max lat]. This code computes that bounding box from the polygon boundary
            # coordinates
            feature["bbox"] = functools.reduce(
                lambda prev, coord: [
                    min(coord[0], prev[0]),
                    min(coord[1], prev[1]),
                    max(coord[0], prev[2]),
                    max(coord[1], prev[3]),
                ],
                polygon_lonlat,
                [math.inf, math.inf, -math.inf, -math.inf],
            )

            # Adding these because some visualization tools (e.g. kepler.gl) can perform more
            # advanced rendering (e.g. cluster layers) if the data points have single coordinates.
            feature["properties"]["center_longitude"] = center_lonlat[0]
            feature["properties"]["center_latitude"] = center_lonlat[1]


class GDALAffineCameraModel(CameraModel):
    def __init__(self, transform):
        """
        GDAL provides a simple affine transform used to convert XY pixel values to longitude,
        latitude. See https://gdal.org/tutorials/geotransforms_tut.html

        transform[0] x-coordinate of the upper-left corner of the upper-left pixel.
        transform[1] w-e pixel resolution / pixel width.
        transform[2] row rotation (typically zero).
        transform[3] y-coordinate of the upper-left corner of the upper-left pixel.
        transform[4] column rotation (typically zero).
        transform[5] n-s pixel resolution / pixel height (negative value for a north-up image).

        The necessary transform matrix can be obtained from a dataset using the GetGeoTransform()
        operation.

        :param transform: the 6 coefficients of the affine transform
        """
        super().__init__()
        self.transform = transform
        self.inv_transform = GDALAffineCameraModel.invert_geo_transform(transform)

    def image_to_world(self, xy_coord: Tuple) -> Tuple:
        longitude = (
            self.transform[0] + xy_coord[0] * self.transform[1] + xy_coord[1] * self.transform[2]
        )
        latitude = (
            self.transform[3] + xy_coord[0] * self.transform[4] + xy_coord[1] * self.transform[5]
        )
        return longitude, latitude

    def world_to_image(self, lonlat_coord: Tuple) -> Tuple:
        x = (
            self.inv_transform[0]
            + lonlat_coord[0] * self.inv_transform[1]
            + lonlat_coord[1] * self.inv_transform[2]
        )
        y = (
            self.inv_transform[3]
            + lonlat_coord[0] * self.inv_transform[4]
            + lonlat_coord[1] * self.inv_transform[5]
        )
        return x, y

    @staticmethod
    def invert_geo_transform(gt_in: List) -> List:
        # we assume a 3rd row that is [1 0 0]

        # Compute determinate
        det = gt_in[1] * gt_in[5] - gt_in[2] * gt_in[4]

        if abs(det) < 0.000000000000001:
            return []

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

class GCPCameraModel(CameraModel): 
    def __init__(self, ds):
        """
        Camera model to allow us to cacluate geocoordinates with GCP coordinates 
        """
        super().__init__()
        self.pszProjection = ds.GetProjectionRef()
        self.gcps = ds.GetGCPs()
        self.adfGeoTransform = gdal.GCPsToGeoTransform(); 

        self.hProj = osr.SpatialReference(self.pszProjection)
        self.hLatLong = self.hProj.CloneGeogCS()
        self.hTransform = osr.CoordinateTransformation(self.hProj, self.hLatLong)

    def image_to_world(self, xy_coord: Tuple) -> Tuple:
        longitude = (
            self.adfGeoTransform[0] + self.adfGeoTransform[1] * xy_coord[0] + self.adfGeoTransform[2] * xy_coord[1]
        )
        latitude = (
            self.adfGeoTransform[3] + self.adfGeoTransform[4] * xy_coord[0] + self.adfGeoTransform[5] * xy_coord[1]
        )

        return longitude, latitude

    def world_to_image(self, lonlat_coord: Tuple) -> Tuple:
        # To implement. 
        # x = (
        #     self.inv_transform[0]
        #     + lonlat_coord[0] * self.inv_transform[1]
        #     + lonlat_coord[1] * self.inv_transform[2]
        # )
        # y = (
        #     self.inv_transform[3]
        #     + lonlat_coord[0] * self.inv_transform[4]
        #     + lonlat_coord[1] * self.inv_transform[5]
        # )
        # return x, y