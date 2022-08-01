from typing import List

import geojson
import numpy as np
import shapely
from shapely.geometry.base import BaseGeometry

from aws_oversightml_model_runner.classes.camera_model import CameraModel


def features_to_image_shapes(
    camera_model: CameraModel, features: List[geojson.Feature]
) -> List[BaseGeometry]:
    shapes: List[BaseGeometry] = []

    if not features or len(features) == 0:
        return []

    for feature in features:
        if "geometry" not in feature:
            raise ValueError("Feature does not contain a valid geometry")

        feature_geometry = feature["geometry"]

        image_coords = convert_nested_coordinate_lists(
            feature_geometry["coordinates"], camera_model.world_to_image
        )

        if isinstance(feature_geometry, geojson.Point):
            shapes.append(shapely.geometry.asPoint(image_coords))
        elif isinstance(feature_geometry, geojson.LineString):
            shapes.append(shapely.geometry.asLineString(image_coords))
        elif isinstance(feature_geometry, geojson.Polygon):
            shapes.append(shapely.geometry.asPolygon(image_coords))
        elif isinstance(feature_geometry, geojson.MultiPoint):
            shapes.append(shapely.geometry.asMultiPoint(image_coords))
        elif isinstance(feature_geometry, geojson.MultiLineString):
            shapes.append(shapely.geometry.asMultiLineString(image_coords))
        elif isinstance(feature_geometry, geojson.MultiPolygon):
            shapes.append(shapely.geometry.asMultiPolygon(image_coords))
        else:
            # Unlikely to get here as we're handling all valid geojson types but if the spec
            # ever changes or if a consumer passes in a custom dictionary that isn't coeracable
            # we want to handle it gracefully
            raise ValueError("Unable to convert feature due to unrecognized or invalid geometry")

    return shapes


def convert_nested_coordinate_lists(coordinates_or_lists, conversion_function):
    if not isinstance(coordinates_or_lists[0], List):
        # This appears to be a single coordinate so run it through the supplied conversion
        # function (image_to_world or world_to_image)
        return conversion_function(coordinates_or_lists)
    else:
        # This appears to be a list of lists (i.e. a LineString, Polygon, etc.) so invoke this
        # conversion routine recursively to preserve the nesting structure of the input
        output_list = []
        for coordinate_list in coordinates_or_lists:
            output_list.append(
                convert_nested_coordinate_lists(coordinate_list, conversion_function)
            )
        return output_list


def feature_nms(feature_list: List[geojson.Feature], threshold=0.75) -> List[geojson.Feature]:
    """
    NMS implementation adapted from https://gist.github.com/quantombone/1144423

    :param threshold:
    :param feature_list: a list of geojson features with a property of bounds_imcoords
    :return: the filtered list of features
    """

    if not feature_list or len(feature_list) == 0:
        return []

    pick = []

    # a numpy array of objects with columns [detect_id, x1, y1, x2, y2]
    selected_feature_properties: List[List[str]] = []
    for feature in feature_list:
        id_bbox_row: List[str] = [feature["id"]]
        id_bbox_row.extend(feature["properties"]["bounds_imcoords"])
        selected_feature_properties.append(id_bbox_row)
    feature_bbox_array = np.array(selected_feature_properties)

    x1 = feature_bbox_array[:, 1].astype("float")
    y1 = feature_bbox_array[:, 2].astype("float")
    x2 = feature_bbox_array[:, 3].astype("float")
    y2 = feature_bbox_array[:, 4].astype("float")

    area = (x2 - x1 + 1) * (y2 - y1 + 1)
    indices = np.argsort(y2)

    while len(indices) > 0:
        last = len(indices) - 1
        i = indices[last]
        pick.append(i)
        xx1 = np.maximum(x1[i], x1[indices[:last]])
        yy1 = np.maximum(y1[i], y1[indices[:last]])
        xx2 = np.minimum(x2[i], x2[indices[:last]])
        yy2 = np.minimum(y2[i], y2[indices[:last]])

        w = np.maximum(0, xx2 - xx1 + 1)
        h = np.maximum(0, yy2 - yy1 + 1)

        overlap = (w * h) / area[indices[:last]]

        indices = np.delete(
            indices, np.concatenate(([last], np.asarray(overlap > threshold).nonzero()[0]))
        )

    selected_feature_ids = feature_bbox_array[pick][:, 0]
    feature_list[:] = [feature for feature in feature_list if feature["id"] in selected_feature_ids]

    return feature_list
