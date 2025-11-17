#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

import json
import os
from secrets import token_hex
from typing import Dict, List

from flask import Response, request
from matplotlib.patches import CirclePolygon
from osgeo import gdal

from aws.osml.test_models.server_utils import build_flask_app, build_logger, detect_to_feature, setup_server

# Enable exceptions for GDAL
gdal.UseExceptions()

# Create logger instance
logger = build_logger()

# Create our default flask app
app = build_flask_app(logger)

# Optional ENV configurations
BBOX_PERCENTAGE = float(os.environ.get("BBOX_PERCENTAGE", 0.1))
ENABLE_SEGMENTATION = os.environ.get("ENABLE_SEGMENTATION", "False").lower() == "true"


def gen_center_bbox(width: int, height: int, bbox_percentage: float) -> List[float]:
    """
    Create a single detection bbox that is at the center of and sized proportionally to the image
    :param bbox_percentage: the size of the bounding box and poly, relative to the image, to return
    :param width: Raster width of the image passed in
    :param height: Raster height of the image passed in
    :return:bbox: Segmented bbox array for center detection
    """
    center_xy = width / 2, height / 2
    fixed_object_size_xy = width * bbox_percentage, height * bbox_percentage
    return [
        center_xy[0] - fixed_object_size_xy[0],
        center_xy[1] - fixed_object_size_xy[1],
        center_xy[0] + fixed_object_size_xy[0],
        center_xy[1] + fixed_object_size_xy[1],
    ]


def gen_center_polygon(width: int, height: int, bbox_percentage: float) -> List[List[float]]:
    center = 0, 0
    center_xy = width / 2, height / 2
    radius = bbox_percentage
    # 20 is a nice circle, 3 is a triangle, etc
    number_of_vertices = 6
    circle = CirclePolygon(center, radius, resolution=number_of_vertices)
    poly_path = circle.get_path().vertices.tolist()
    # This is part of CV model requirements, to have (only) closed polygons
    poly_path.append(poly_path[0])
    # This moves poly to nonzero 0-1 coords
    nonzero_circle = [((x + 1) / 2, (y + 1) / 2) for (x, y) in poly_path]
    # Project to the correct percentage of our image coordinates
    scale = [bbox_percentage * width, bbox_percentage * height]
    # Do final scaling of coordinates, and w/h translation to ensure within bounds of the bbox
    center_polygon = [
        [round(x * scale[0] + center_xy[0], 4), round(y * scale[1] + center_xy[1], 4)] for (x, y) in nonzero_circle
    ]
    return center_polygon


def gen_center_detect(width: int, height: int, bbox_percentage: float) -> Dict:
    """
    Create circular polygon that is at the center of and sized proportionally to the bbox

    :param bbox_percentage: the size of the bounding box and poly, relative to the image, to return
    :param width: Raster width of the image passed in
    :param height: Raster height of the image passed in
    :return: geojson: Segmented polygon for center detection

    This draws a polygon with the same center and width and height percentage polygon can be a circle,
    or a hexagon, or triangle, etc. - based on the number_of_vertices there is a chance this is not
    centered as we'd like - meanwhile this will work as-is for initial OSML segmentation 'passthrough'
    """
    center_polygon = None
    if ENABLE_SEGMENTATION:
        center_polygon = gen_center_polygon(width, height, bbox_percentage)

    geojson_feature = detect_to_feature(gen_center_bbox(width, height, bbox_percentage), center_polygon)
    return {"type": "FeatureCollection", "features": [geojson_feature]}


@app.route("/ping", methods=["GET"])
def healthcheck() -> Response:
    """
    This is a health check that will always pass since this is a stub model.

    :return: Response: Status code (200) indicates all is well
    """
    app.logger.debug("Responding to health check")
    return Response(response="\n", status=200)


@app.route("/invocations", methods=["POST"])
def predict() -> Response:
    """
    This is the model invocation endpoint for the model container's REST
    API. The binary payload, in this case an image, is taken from the request
    parsed to ensure it is a valid image. This is a stub implementation that
    will always return the fixed set of detections for a valid input image.

    :return: Response: Contains the GeoJSON results or an error status
    """
    app.logger.debug("Invoking centerpoint model endpoint")
    temp_ds_name = "/vsimem/" + token_hex(16)
    gdal_dataset = None
    try:
        # Load the file from the request memory buffer
        gdal.FileFromMemBuffer(temp_ds_name, request.get_data())
        try:
            gdal_dataset = gdal.Open(temp_ds_name)
        # If it failed to load return the failed Response
        except RuntimeError:
            return Response(response="Unable to parse image from request!", status=400)

        geojson_feature_collection = gen_center_detect(gdal_dataset.RasterXSize, gdal_dataset.RasterYSize, BBOX_PERCENTAGE)
        app.logger.debug(json.dumps(geojson_feature_collection))
        # Send back the detections
        return Response(response=json.dumps(geojson_feature_collection), status=200)
    except Exception as err:
        app.logger.warning("Image could not be processed by the centerpoint model server.", exc_info=True)
        app.logger.warning(err)
        return Response(response="Unable to process request.", status=500)
    finally:
        if gdal_dataset is not None:
            if temp_ds_name is not None:
                gdal.Unlink(temp_ds_name)
            del gdal_dataset


# pragma: no cover
if __name__ == "__main__":
    setup_server(app)
