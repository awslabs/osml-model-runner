#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

import json

from aws.osml.model_runner_validation_tool.common.logging_config import configure_logging

logger = configure_logging(__name__)


class ValidateGeoJSON:
    """
    Validates GeoJSON data against the GeoJSON schema

    Args:
        raw_response (str): The raw JSON string to validate

    Returns:
        tuple: (is_valid, error_message)
    """

    @staticmethod
    def validate(raw_response: str) -> tuple:
        """
        Validates that the provided string is valid GeoJSON

        Args:
            raw_response (str): The raw JSON string to validate

        Returns:
            tuple: (is_valid, error_message)
        """
        is_valid, error_message = _validate_geojson(raw_response)
        return is_valid, error_message


def _validate_geojson(raw_response):
    """
    Validates that the provided string is valid GeoJSON

    Args:
        raw_response (str): The raw JSON string to validate

    Returns:
        tuple: (is_valid, error_message)
    """
    # GeoJSON schema based on RFC 7946
    geojson_schema = {
        "type": "object",
        "required": ["type"],
        "properties": {
            "type": {
                "enum": [
                    "FeatureCollection",
                    "Feature",
                    "Point",
                    "LineString",
                    "Polygon",
                    "MultiPoint",
                    "MultiLineString",
                    "MultiPolygon",
                    "GeometryCollection",
                ]
            },
            "features": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["type", "geometry", "properties"],
                    "properties": {
                        "type": {"enum": ["Feature"]},
                        # TODO
                        # The geometry should be null. The detections should not have real-life coordinates because it's
                        # just a tile. Instead there should be a imageGeometry key with pixels relative to the tile top
                        # left corner (x,y)
                        "geometry": {
                            "type": "object",
                            "required": ["type", "coordinates"],
                            "properties": {
                                "type": {
                                    "enum": [
                                        "Point",
                                        "LineString",
                                        "Polygon",
                                        "MultiPoint",
                                        "MultiLineString",
                                        "MultiPolygon",
                                        "GeometryCollection",
                                    ]
                                },
                                "coordinates": {"type": "array"},
                            },
                        },
                        "imageGeometry": {
                            "type": "object",
                            "required": ["type", "coordinates"],
                            "properties": {
                                "type": {
                                    "enum": [
                                        "Point",
                                        "LineString",
                                        "Polygon",
                                        "MultiPoint",
                                        "MultiLineString",
                                        "MultiPolygon",
                                        "GeometryCollection",
                                    ]
                                },
                                "coordinates": {"type": "array"},
                            },
                        },
                        "properties": {"type": "object"},
                    },
                },
            },
        },
    }
    # logging schema to make flake8 happy for the time being
    logger.debug(f"GeoJSON schema: {geojson_schema}")

    try:
        geojson_data = json.loads(raw_response)

        # Basic structure checks
        if geojson_data.get("type") != "FeatureCollection":
            return False, "Root type should be FeatureCollection"

        if "features" not in geojson_data:
            return False, "FeatureCollection must have features array"

        if not isinstance(geojson_data["features"], list):
            return False, "Features must be an array"

        # Validate against GeoJSON schema
        # validate(instance=geojson_data, schema=geojson_schema)

        # Check each feature
        for feature in geojson_data["features"]:
            if feature.get("type") != "Feature":
                return False, "Feature type must be 'Feature'"

            if "geometry" not in feature:
                return False, "Feature must have geometry"

            if "properties" not in feature:
                return False, "Feature must have properties"

            # Check geometry
            geometry = feature["geometry"]
            if "type" not in geometry:
                return False, "Geometry must have type"

            if "coordinates" not in geometry:
                return False, "Geometry must have coordinates"

            # For Point geometries, coordinates should be [longitude, latitude]
            if geometry["type"] == "Point" and len(geometry["coordinates"]) != 2:
                return False, "Point coordinates should have 2 values"

            # TODO: Verify these are correct and look for other response possibilities
            # Check OSML-specific properties
            properties = feature["properties"]
            if "detection_score" not in properties:
                return False, "Properties should include detection_score"

            if "feature_types" not in properties:
                return False, "Properties should include feature_types"

            if "image_id" not in properties:
                return False, "Properties should include image_id"

        return True, "GeoJSON validation passed!"

    except json.JSONDecodeError as e:
        return False, f"Failed to decode JSON: {e}"
    except Exception as e:
        return False, f"GeoJSON validation failed: {e}"
