#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

import json
import time
from secrets import token_hex
from typing import Tuple

import numpy as np
from flask import Response, request
from osgeo import gdal

from aws.osml.test_models.server_utils import (
    build_flask_app,
    build_logger,
    detect_to_feature,
    setup_server,
)

# Enable exceptions for GDAL
gdal.UseExceptions()

# Create logger instance
logger = build_logger()

# Create our default flask app
app = build_flask_app(logger)


def get_dominant_color(image_array: np.ndarray) -> Tuple[int, int, int]:
    """
    Analyze the image to find the dominant color by averaging all pixels.

    :param image_array: NumPy array of the image
    :param gdal_dataset: GDAL dataset to check for color table
    :return: RGB tuple of dominant color
    """
    if image_array.ndim == 3:
        # GDAL format is (channels, height, width)
        avg_color = np.mean(image_array, axis=(1, 2))  # Average over height and width
        app.logger.info(f"Average color again: {avg_color}")
        if len(avg_color) >= 3:
            return tuple(avg_color[:3].astype(int))
        else:
            avg = int(avg_color[0])
            return (avg, avg, avg)
    else:
        return (0, 0, 0)


def trigger_behavior_by_color(color: Tuple[int, int, int]) -> Response:
    """
    Trigger specific error behaviors based on dominant color in the tile.

    :param color: RGB tuple of dominant color
    :return: Response with behavior determined by color
    """
    r, g, b = color
    app.logger.debug(f"Color values: R={r}, G={g}, B={b}")

    # Red dominant - Server Side failure in Model
    if r > 200 and g < 50 and b < 50:
        app.logger.debug("Red detected - server side error")
        return Response(response="Unable to process request.", status=500)

    # Green dominant - Malformed JSON (data corruption)
    elif g > 200 and r < 50 and b < 50:
        app.logger.debug("Green detected - returning malformed JSON")
        return Response(response='{"type": "FeatureCollection", "features": [invalid}', status=200)

    # Purple Dominant - JSON with wrong keys
    elif r > 200 and b > 200 and g < 50:
        app.logger.debug("Purple detected - returning JSON with invalid keys")
        return Response(
            response=json.dumps(
                {
                    "type": "FeatureCollection",
                    "invalid_key": [  # Schema validation issue RFC7946
                        {
                            "type": "Feature",
                            "geometry": None,
                            "properties": {
                                "imageGeometry": {"type": "Point", "coordinates": [0.0, 0.0]},
                                "imageBBox": [100, 100, 200, 200],
                                "featureClasses": [{"iri": "test-type", "score": 0.94}],
                                "modelMetadata": {
                                    "modelName": "failure",
                                    "ontologyName": "test",
                                    "ontologyVersion": "1.0.0",
                                },
                            },
                        }
                    ],
                }
            ),
            status=200,
        )

    # Blue dominant - Timeout request
    elif b > 200 and r < 50 and g < 50:
        app.logger.debug("Blue detected - simulating timeout")
        time.sleep(5)  # Simulate long processing
        return Response(response="Request timeout", status=408)

    # Fallback - Successful processing (2_planes.tiff)
    else:
        app.logger.debug("Normal color detected - returning standard detection")
        normal_detection = detect_to_feature([100, 100, 200, 200], detection_type="test_object")
        return Response(response=json.dumps({"type": "FeatureCollection", "features": [normal_detection]}), status=200)


@app.route("/ping", methods=["GET"])
def healthcheck() -> Response:
    """Health check endpoint."""
    app.logger.debug("Responding to health check")
    return Response(response="\n", status=200)


@app.route("/invocations", methods=["POST"])
def predict() -> Response:
    """
    Model invocation endpoint that triggers different behaviors based on image color.

    :return: Response with behavior determined by dominant color in image
    """
    app.logger.debug("Invoking failure model endpoint")
    temp_ds_name = "/vsimem/" + token_hex(16)
    gdal_dataset = None

    try:
        # Load image from request
        gdal.FileFromMemBuffer(temp_ds_name, request.get_data())
        gdal_dataset = gdal.Open(temp_ds_name)

        if gdal_dataset is None:
            raise RuntimeError("Failed to open image")

        # Read image as array
        image_array = gdal_dataset.ReadAsArray()

        # Get dominant color
        dominant_color = get_dominant_color(image_array)
        app.logger.debug(f"Dominant color detected: {dominant_color}")

        # Trigger behavior based on color
        return trigger_behavior_by_color(dominant_color)

    except Exception as err:
        app.logger.error(f"Error processing request: {err}")
        return Response(response=f"Unable to process request: {err}", status=500)

    finally:
        if gdal_dataset is not None:
            if temp_ds_name is not None:
                gdal.Unlink(temp_ds_name)
            del gdal_dataset


if __name__ == "__main__":
    setup_server(app)
