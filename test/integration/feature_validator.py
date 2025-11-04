#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
Feature validation utilities for integration tests.

This module provides utilities for validating GeoJSON features and feature
collections against expected results in integration tests.
"""

import base64
import json
import logging
from math import isclose
from typing import Any, Dict, List, Optional

import geojson
from boto3 import dynamodb
from geojson import Feature


class FeatureValidator:
    """
    Validates GeoJSON features and feature collections.

    Provides methods for comparing GeoJSON features and collections
    with detailed logging of mismatches.
    """

    def __init__(self):
        """Initialize the feature validator."""
        self.logger = logging.getLogger(__name__)

    def _check_and_return_kinesis_result(
        self,
        kinesis_features: List[Feature],
        expected_features: List[Feature],
        cache: Optional[Dict[str, List[Feature]]],
        stream: str,
    ) -> bool:
        """
        Check if kinesis features match expected features and cache if needed.

        Args:
            kinesis_features: Features read from Kinesis
            expected_features: Expected features
            cache: Optional cache dict
            stream: Stream name

        Returns:
            True if features match, False otherwise
        """
        if cache is not None and kinesis_features:
            cache[stream] = kinesis_features

        if self.feature_collections_equal(expected_features, kinesis_features):
            self.logger.info(f"  ✓ Kinesis: {len(kinesis_features)} features validated")
            return True
        return False

    # ============================================================================
    # Feature Comparison Methods
    # ============================================================================

    def check_center_coord(self, expected: Optional[float], actual: Optional[float]) -> bool:
        """
        Check if center coordinates match, handling None values.

        Args:
            expected: Expected coordinate value
            actual: Actual coordinate value

        Returns:
            True if coordinates match or both are None
        """
        if expected is None and actual is None:
            return True
        if expected is None or actual is None:
            return False

        return isclose(expected, actual, abs_tol=10 ** (-8))

    def source_metadata_equal(self, expected: List, actual: List) -> bool:
        """
        Compare source metadata lists for equality.

        Args:
            expected: Expected source metadata
            actual: Actual source metadata

        Returns:
            True if metadata lists are equivalent, False otherwise
        """
        # Handle None cases: None and empty list should be considered equivalent
        # Convert None to empty list for easier comparison
        expected_list = expected if expected is not None else []
        actual_list = actual if actual is not None else []

        # If both are empty (None or empty list), they match
        if len(expected_list) == 0 and len(actual_list) == 0:
            return True

        # If one is empty and the other is not, they don't match
        if len(expected_list) == 0 or len(actual_list) == 0:
            self.logger.info("Expected and actual source metadata don't match (one is empty)")
            return False

        # At this point, both are non-empty lists, so we can safely compare lengths
        if not len(expected_list) == len(actual_list):
            self.logger.info(f"Expected {len(expected_list)} source metadata but found {len(actual_list)}")
            return False

        for expected_source_metadata, actual_source_metadata in zip(expected_list, actual_list):
            is_equal = set({"location", "sourceDT"}).issuperset(
                k for (k, v) in expected_source_metadata.items() ^ actual_source_metadata.items()
            )

            if not is_equal:
                self.logger.info(
                    f"Source metadata {expected_source_metadata} does not match actual {actual_source_metadata}"
                )
                return False

        return True

    def feature_equal(self, expected: geojson.Feature, actual: geojson.Feature) -> bool:
        """
        Compare two GeoJSON features for equality.

        Args:
            expected: Expected feature
            actual: Actual feature to compare

        Returns:
            True if features are equivalent, False otherwise
        """
        actual_pixel_coords = actual.get("properties", {}).get("detection", {}).get("pixelCoordinates")
        expected_pixel_coords = expected.get("properties", {}).get("detection", {}).get("pixelCoordinates")

        geojson_checks = [
            ("Feature type matches", expected.type == actual.type),
            ("Geometry matches", expected.geometry == actual.geometry),
            ("Pixel coordinates match", expected_pixel_coords == actual_pixel_coords),
            ("Inference metadata exists", expected.properties.get("inferenceMetadata") is not None),
            (
                "Source metadata matches",
                self.source_metadata_equal(
                    expected.properties.get("sourceMetadata"), actual.properties.get("sourceMetadata")
                ),
            ),
            (
                "Feature detection class matches",
                expected.properties.get("featureClasses") == actual.properties.get("featureClasses"),
            ),
            ("Image geometry matches", expected.properties.get("imageGeometry") == actual.properties.get("imageGeometry")),
            (
                "Center longitude matches",
                self.check_center_coord(
                    expected.properties.get("center_longitude"), actual.properties.get("center_longitude")
                ),
            ),
            (
                "Center latitude matches",
                self.check_center_coord(
                    expected.properties.get("center_latitude"), actual.properties.get("center_latitude")
                ),
            ),
        ]

        failed_checks = []
        for check, result in geojson_checks:
            if not result:
                failed_checks.append(check)

        if len(failed_checks) > 0:
            self.logger.info(f"Failed feature equality checks: {', '.join(failed_checks)}")
            self.logger.info("Expected feature:")
            self.logger.info(geojson.dumps(expected, indent=2))
            self.logger.info("Actual feature:")
            self.logger.info(geojson.dumps(actual, indent=2))
            return False

        return True

    def feature_collections_equal(self, expected: List[geojson.Feature], actual: List[geojson.Feature]) -> bool:
        """
        Compare two feature collections for equality.

        Args:
            expected: Expected feature collection
            actual: Actual feature collection

        Returns:
            True if collections are equivalent, False otherwise
        """
        if not len(expected) == len(actual):
            self.logger.info(f"Expected {len(expected)} features but found {len(actual)}")
            return False

        # Sort features by image geometry for consistent comparison
        expected.sort(key=lambda x: str(x.get("properties", {}).get("imageGeometry", {})))
        actual.sort(key=lambda x: str(x.get("properties", {}).get("imageGeometry", {})))

        for expected_feature, actual_feature in zip(expected, actual):
            if not self.feature_equal(expected_feature, actual_feature):
                self.logger.info(expected_feature)
                self.logger.info("does not match actual")
                self.logger.info(actual_feature)
                return False

        return True

    # ============================================================================
    # S3 Validation Methods
    # ============================================================================

    def get_matching_s3_keys(self, s3_client: Any, bucket: str, prefix: str = "", suffix: str = ""):
        """
        Generate S3 object keys matching the given criteria.

        Args:
            s3_client: Boto3 S3 client
            bucket: S3 bucket name
            prefix: Object key prefix filter
            suffix: Object key suffix filter

        Yields:
            str: Matching S3 object keys
        """
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        while True:
            resp = s3_client.list_objects_v2(**kwargs)
            for obj in resp["Contents"]:
                key = obj["Key"]
                if key.endswith(suffix):
                    yield key

            try:
                kwargs["ContinuationToken"] = resp["NextContinuationToken"]
            except KeyError:
                break

    def validate_s3_features(self, s3_client: Any, bucket: str, prefix: str, expected_features: List[Feature]) -> bool:
        """
        Validate S3 output results against expected features.

        Args:
            s3_client: Boto3 S3 client
            bucket: S3 bucket name
            prefix: S3 object prefix
            expected_features: List of expected features

        Returns:
            True if features match, False otherwise
        """
        for object_key in self.get_matching_s3_keys(s3_client, bucket, prefix=prefix, suffix=".geojson"):
            s3_output = s3_client.get_object(Bucket=bucket, Key=object_key)
            contents = s3_output["Body"].read()
            s3_features = geojson.loads(contents.decode("utf-8"))["features"]

            if self.feature_collections_equal(expected_features, s3_features):
                self.logger.info(f"  ✓ S3: {len(s3_features)} features validated")
                return True

        return False

    # ============================================================================
    # Kinesis Validation Methods
    # ============================================================================

    def validate_kinesis_features(
        self,
        kinesis_client: Any,
        job_id: str,
        stream: str,
        shard_iter: Optional[str],
        expected_features: List[Feature],
        cache: Optional[Dict[str, List[Feature]]] = None,
    ) -> bool:
        """
        Validate Kinesis output results against expected features.

        Args:
            kinesis_client: Boto3 Kinesis client
            job_id: Job ID for result correlation
            stream: Kinesis stream name
            shard_iter: Shard iterator for reading records
            expected_features: List of expected features
            cache: Optional dict to cache the features we read

        Returns:
            True if features match, False otherwise
        """
        if shard_iter is None:
            return False

        try:
            kinesis_features = []
            current_shard_iter = shard_iter
            max_iterations = 100  # Prevent infinite loops

            # Iterate through records until we find what we're looking for or exhaust the stream
            for iteration in range(max_iterations):
                response = kinesis_client.get_records(ShardIterator=current_shard_iter, Limit=10000)
                records = response.get("Records", [])

                for record in records:
                    partition_key = record.get("PartitionKey")
                    if partition_key == job_id:
                        try:
                            # Kinesis records may be bytes, base64-encoded strings, or JSON strings
                            data = record["Data"]

                            # Handle different data types from Kinesis
                            if isinstance(data, bytes):
                                if len(data) == 0:
                                    continue
                                data_str = data.decode("utf-8")
                            elif isinstance(data, str):
                                # Check if it's base64-encoded
                                try:
                                    # Try to decode as base64 first
                                    decoded = base64.b64decode(data)
                                    data_str = decoded.decode("utf-8")
                                except Exception:
                                    # If it fails, treat as plain string
                                    data_str = data
                            else:
                                data_str = str(data)

                            record_data = geojson.loads(data_str)
                            if "features" in record_data:
                                kinesis_features.extend(record_data["features"])

                        except (json.JSONDecodeError, KeyError, UnicodeDecodeError) as e:
                            self.logger.debug(f"Failed to parse Kinesis record data: {e}")

                # Check if we have all expected features
                if len(kinesis_features) >= len(expected_features):
                    if self._check_and_return_kinesis_result(kinesis_features, expected_features, cache, stream):
                        return True

                # Check for next shard iterator
                next_shard_iter = response.get("NextShardIterator")
                if not next_shard_iter:
                    break
                current_shard_iter = next_shard_iter

            # If we exited the loop without finding a match, return False
            return False

        except Exception as e:
            self.logger.debug(f"Error reading from Kinesis stream: {e}")
            return False

    # ============================================================================
    # Count-Based Validation Methods
    # ============================================================================

    def validate_by_count(
        self,
        image_id: str,
        expected_feature_counts: List[int],
        expected_region_count: int,
        ddb_clients: Any = None,
        config: Any = None,
    ) -> Dict[str, Any]:
        """
        Generic count-based validation for any model.

        Validates model results by comparing actual feature and region request counts
        against expected values. This can be used for any model that supports
        count-based validation (flood, and future models).

        Args:
            image_id: Image ID to validate
            expected_feature_counts: List of acceptable feature counts
            expected_region_count: Expected number of region requests
            ddb_clients: DynamoDB resource for querying
            config: Configuration object with table names

        Returns:
            Dict with validation results including:
                - success: bool indicating validation result
                - feature_count: actual feature count
                - region_request_count: actual region request count
                - message: validation message
        """
        # Count features and region requests
        feature_count = self.count_features(ddb_clients, config, image_id)
        region_request_count = self.count_region_requests(ddb_clients, config, image_id)

        # Validate feature count
        if feature_count not in expected_feature_counts:
            expected_str = " | ".join(map(str, expected_feature_counts))
            message = f"Feature count mismatch: found {feature_count}, expected {expected_str}"
            self.logger.error(f"❌ {message}")
            return {
                "success": False,
                "feature_count": feature_count,
                "region_request_count": region_request_count,
                "message": message,
            }

        # Validate region request count
        if region_request_count != expected_region_count:
            message = f"Region request count mismatch: found {region_request_count}, expected {expected_region_count}"
            self.logger.error(f"❌ {message}")
            return {
                "success": False,
                "feature_count": feature_count,
                "region_request_count": region_request_count,
                "message": message,
            }

        self.logger.info(f"✅ Feature count validation passed: {feature_count} features")
        self.logger.info(f"✅ Region request validation passed: {region_request_count} requests")

        return {
            "success": True,
            "feature_count": feature_count,
            "region_request_count": region_request_count,
            "message": "Count-based validation passed!",
        }

    def count_features(self, ddb_clients: Any, config: Any, image_id: str) -> int:
        """
        Count features in DynamoDB for a given image ID.

        Args:
            ddb_clients: DynamoDB resource
            config: Configuration object
            image_id: Image ID to count features for

        Returns:
            Number of features found
        """
        ddb_table = ddb_clients.Table(config.DDB_FEATURES_TABLE)

        items = self._query_items(ddb_table, "hash_key", image_id, True)

        features: List[Feature] = []
        for item in items:
            for feature in item["features"]:
                features.append(geojson.loads(feature))

        total_features = len(features)
        return total_features

    def count_region_requests(self, ddb_clients: Any, config: Any, image_id: str) -> int:
        """
        Count successful region requests for a given image.

        Args:
            ddb_clients: DynamoDB resource
            config: Configuration object
            image_id: Image ID to count region requests for

        Returns:
            Number of successful region requests
        """
        ddb_table = ddb_clients.Table(config.DDB_REGION_REQUEST_TABLE)
        items = self._query_items(ddb_table, "image_id", image_id, False)

        total_count = 0
        for item in items:
            if item.get("region_status") == "SUCCESS":
                total_count += 1

        self.logger.info(f"Found {total_count} successful region requests!")
        return total_count

    # ============================================================================
    # DynamoDB Helper Methods
    # ============================================================================

    def _query_items(self, ddb_table: Any, hash_key: str, hash_value: str, is_feature_count: bool) -> List[Dict[str, Any]]:
        """
        Query DynamoDB table for items with given hash key/value.

        Args:
            ddb_table: DynamoDB table resource
            hash_key: Hash key attribute name
            hash_value: Hash key value to query for
            is_feature_count: Whether to append index to hash value

        Returns:
            List of matching items
        """
        items: List[dict] = []
        max_hash_salt = 50

        for index in range(1, max_hash_salt + 1):
            hash_value_index = f"{hash_value}-{index}" if is_feature_count else hash_value
            all_items_retrieved = False

            response = ddb_table.query(
                ConsistentRead=True,
                KeyConditionExpression=dynamodb.conditions.Key(hash_key).eq(hash_value_index),
            )

            while not all_items_retrieved:
                items.extend(response["Items"])

                if "LastEvaluatedKey" in response:
                    response = ddb_table.query(
                        ConsistentRead=True,
                        KeyConditionExpression=dynamodb.conditions.Key(hash_key).eq(hash_value_index),
                        ExclusiveStartKey=response["LastEvaluatedKey"],
                    )
                else:
                    all_items_retrieved = True

            if not is_feature_count:
                break

        return items
