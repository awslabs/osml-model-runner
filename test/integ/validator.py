#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
Integration validation utilities for integration tests.

This module provides utilities for validating GeoJSON features and feature
collections against expected results in integration tests.
"""

import base64
import json
import logging
from math import isclose
from typing import TYPE_CHECKING, Dict, Iterator, List, Optional, Union

import geojson
from boto3 import dynamodb
from geojson import Feature

if TYPE_CHECKING:
    from test.integ.config import Config

# Type aliases for boto3 clients/resources (using object as base since boto3 types vary)
# These represent boto3.client() and boto3.resource() return types
S3ClientType = object  # boto3.client("s3")
KinesisClientType = object  # boto3.client("kinesis")
DynamoDBResourceType = object  # boto3.resource("dynamodb")
DynamoDBTableType = object  # DynamoDB Table resource
ConfigType = Union["Config", object]


class Validator:
    """
    Validates GeoJSON features and feature collections.

    Provides methods for comparing GeoJSON features and collections
    with detailed logging of mismatches.
    """

    def __init__(self) -> None:
        """
        Initialize the integration validator.

        Sets up logging for validation operations.
        """
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

        :param kinesis_features: Features read from Kinesis.
        :param expected_features: Expected features.
        :param cache: Optional cache dict to store features.
        :param stream: Stream name.
        :returns: True if features match, False otherwise.
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

        :param expected: Expected coordinate value.
        :param actual: Actual coordinate value.
        :returns: True if coordinates match or both are None.
        """
        if expected is None and actual is None:
            return True
        if expected is None or actual is None:
            return False

        return isclose(expected, actual, abs_tol=10 ** (-8))

    def source_metadata_equal(self, expected: List, actual: List) -> bool:
        """
        Compare source metadata lists for equality.

        :param expected: Expected source metadata list.
        :param actual: Actual source metadata list.
        :returns: True if metadata lists are equivalent, False otherwise.
        """
        expected_list = expected if expected is not None else []
        actual_list = actual if actual is not None else []

        if len(expected_list) == 0 and len(actual_list) == 0:
            return True

        if len(expected_list) == 0 or len(actual_list) == 0:
            self.logger.info("Expected and actual source metadata don't match (one is empty)")
            return False

        if len(expected_list) != len(actual_list):
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

        :param expected: Expected feature.
        :param actual: Actual feature to compare.
        :returns: True if features are equivalent, False otherwise.
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

        :param expected: Expected feature collection.
        :param actual: Actual feature collection.
        :returns: True if collections are equivalent, False otherwise.
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

    def get_matching_s3_keys(
        self, s3_client: S3ClientType, bucket: str, prefix: str = "", suffix: str = ""
    ) -> Iterator[str]:
        """
        Generate S3 object keys matching the given criteria.

        :param s3_client: Boto3 S3 client.
        :param bucket: S3 bucket name.
        :param prefix: Object key prefix filter.
        :param suffix: Object key suffix filter.
        :yields: Matching S3 object keys.
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

    def validate_s3_features(
        self, s3_client: S3ClientType, bucket: str, prefix: str, expected_features: List[Feature]
    ) -> bool:
        """
        Validate S3 output results against expected features.

        :param s3_client: Boto3 S3 client.
        :param bucket: S3 bucket name.
        :param prefix: S3 object prefix.
        :param expected_features: List of expected features.
        :returns: True if features match, False otherwise.
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
        kinesis_client: KinesisClientType,
        job_id: str,
        stream: str,
        shard_iter: Optional[str],
        expected_features: List[Feature],
        cache: Optional[Dict[str, List[Feature]]] = None,
    ) -> bool:
        """
        Validate Kinesis output results against expected features.

        :param kinesis_client: Boto3 Kinesis client.
        :param job_id: Job ID for result correlation.
        :param stream: Kinesis stream name.
        :param shard_iter: Shard iterator for reading records.
        :param expected_features: List of expected features.
        :param cache: Optional dict to cache the features read.
        :returns: True if features match, False otherwise.
        """
        if shard_iter is None:
            return False

        try:
            kinesis_features = []
            current_shard_iter = shard_iter
            max_iterations = 100  # Prevent infinite loops

            for iteration in range(max_iterations):
                response = kinesis_client.get_records(ShardIterator=current_shard_iter, Limit=10000)
                records = response.get("Records", [])

                for record in records:
                    if record.get("PartitionKey") != job_id:
                        continue

                    try:
                        data = record["Data"]
                        # Handle different data types from Kinesis
                        if isinstance(data, bytes):
                            if len(data) == 0:
                                continue
                            data_str = data.decode("utf-8")
                        elif isinstance(data, str):
                            try:
                                data_str = base64.b64decode(data).decode("utf-8")
                            except Exception:
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
        ddb_clients: Optional[DynamoDBResourceType] = None,
        config: Optional[ConfigType] = None,
    ) -> Dict[str, Union[bool, int, List[int], str]]:
        """
        Generic count-based validation for any model.

        Validates model results by comparing actual feature and region request counts
        against expected values. This can be used for any model that supports
        count-based validation (flood, and future models).

        :param image_id: Image ID to validate.
        :param expected_feature_counts: List of acceptable feature counts.
        :param expected_region_count: Expected number of region requests.
        :param ddb_clients: DynamoDB resource for querying.
        :param config: Configuration object with table names.
        :returns: Dict with validation results including success, feature_count, region_request_count, and message.
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

        self.logger.info(f"  → Feature count: {feature_count} features")
        self.logger.info(f"  → Region requests: {region_request_count} validated")

        return {
            "success": True,
            "feature_count": feature_count,
            "region_request_count": region_request_count,
            "message": "Count-based validation passed!",
        }

    def count_features(self, ddb_clients: DynamoDBResourceType, config: ConfigType, image_id: str) -> int:
        """
        Count features in DynamoDB for a given image ID.

        :param ddb_clients: DynamoDB resource.
        :param config: Configuration object.
        :param image_id: Image ID to count features for.
        :returns: Number of features found.
        """
        ddb_table = ddb_clients.Table(config.FEATURE_TABLE)

        items = self._query_items(ddb_table, "hash_key", image_id, True)

        features: List[Feature] = []
        for item in items:
            for feature in item["features"]:
                features.append(geojson.loads(feature))

        total_features = len(features)
        return total_features

    def count_region_requests(self, ddb_clients: DynamoDBResourceType, config: ConfigType, image_id: str) -> int:
        """
        Count successful region requests for a given image.

        :param ddb_clients: DynamoDB resource.
        :param config: Configuration object.
        :param image_id: Image ID to count region requests for.
        :returns: Number of successful region requests.
        """
        ddb_table = ddb_clients.Table(config.REGION_REQUEST_TABLE)
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

    def _query_items(
        self, ddb_table: DynamoDBTableType, hash_key: str, hash_value: str, is_feature_count: bool
    ) -> List[Dict[str, Union[str, int, float, bool, List[str]]]]:
        """
        Query DynamoDB table for items with given hash key/value.

        :param ddb_table: DynamoDB table resource.
        :param hash_key: Hash key attribute name.
        :param hash_value: Hash key value to query for.
        :param is_feature_count: Whether to append index to hash value.
        :returns: List of matching items.
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
