#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
Integration test utilities for OSML Model Runner.

This module provides comprehensive utilities for integration testing including:
- Image processing workflow management
- Result validation and comparison
- Feature counting and analysis
- Job monitoring and status tracking
"""

import base64
import json
import logging
import time
from math import isclose
from secrets import token_hex
from typing import Any, Dict, List, Optional

import boto3
import geojson
from boto3 import dynamodb
from botocore.exceptions import ClientError, ParamValidationError
from geojson import Feature

from .config import OSMLConfig

# Global config instance to avoid repeated initialization
_config_instance = None


def get_config() -> OSMLConfig:
    """Get a properly initialized OSMLConfig instance."""
    global _config_instance
    if _config_instance is None:
        _config_instance = OSMLConfig()
    return _config_instance


# Configure logging
logger = logging.getLogger(__name__)


def queue_image_processing_job(sqs_client: boto3.resource, image_processing_request: Dict[str, Any]) -> Optional[str]:
    """
    Submit an image processing request to the SQS queue.

    Args:
        sqs_client: SQS client for queue operations
        image_processing_request: The request to submit

    Returns:
        Message ID of the queued message

    Raises:
        ClientError: If SQS operation fails
        ParamValidationError: If request validation fails
    """
    logger.info(f"Sending request: jobId={image_processing_request['jobId']}")

    config = get_config()
    try:
        queue = sqs_client.get_queue_by_name(QueueName=config.SQS_IMAGE_REQUEST_QUEUE, QueueOwnerAWSAccountId=config.ACCOUNT)
        response = queue.send_message(MessageBody=json.dumps(image_processing_request))

        message_id = response.get("MessageId")
        logger.info(f"Message queued to SQS with messageId={message_id}")

        return message_id

    except ClientError as error:
        logger.error(f"Unable to send job request to SQS queue: {config.SQS_IMAGE_REQUEST_QUEUE}")
        logger.error(f"{error}")
        raise

    except ParamValidationError as error:
        logger.error("Invalid SQS API request; validation failed")
        logger.error(f"{error}")
        raise


def monitor_job_status(sqs_client: boto3.resource, image_id: str, timeout_minutes: int = 30) -> None:
    """
    Monitor job status until completion or timeout.

    Args:
        sqs_client: SQS client for status monitoring
        image_id: Image ID to monitor
        timeout_minutes: Maximum time to wait for completion

    Raises:
        TimeoutError: If job doesn't complete within timeout
        AssertionError: If job fails
    """
    done = False
    max_retries = timeout_minutes * 12  # 12 retries per minute (5 second intervals)
    retry_interval = 5

    config = get_config()
    queue = sqs_client.get_queue_by_name(QueueName=config.SQS_IMAGE_STATUS_QUEUE, QueueOwnerAWSAccountId=config.ACCOUNT)

    logger.info(f"Listening to SQS ImageStatusQueue for progress updates... (timeout: {timeout_minutes} minutes)")

    start_time = time.time()

    while not done and max_retries > 0:
        try:
            # Use WaitTimeSeconds for long polling to reduce API calls
            logger.info(f"Attempting to receive messages from SQS (retries left: {max_retries})")
            messages = queue.receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=5, VisibilityTimeout=30)
            logger.info(f"Received {len(messages)} messages from SQS")

            # Process all messages in the batch
            for message in messages:
                try:
                    logger.info(f"Processing message: {message.body[:200]}...")  # Log first 200 chars

                    # Parse the SNS message format - SNS messages delivered to SQS have this structure:
                    # {
                    #   "Type": "Notification",
                    #   "MessageId": "...",
                    #   "Message": "actual message content",
                    #   "MessageAttributes": { "key": {"Type": "String", "Value": "value"} }
                    # }
                    message_body = json.loads(message.body)
                    message_attributes = message_body.get("MessageAttributes", {})

                    # Extract values from SNS MessageAttributes format
                    message_image_id = message_attributes.get("image_id", {}).get("Value")
                    message_image_status = message_attributes.get("status", {}).get("Value")

                    logger.info(
                        f"Message - image_id: {message_image_id}, status: {message_image_status}, looking for: {image_id}"
                    )

                    if message_image_status == "IN_PROGRESS" and message_image_id == image_id:
                        elapsed = int(time.time() - start_time)
                        logger.info(f"\tIN_PROGRESS message found! Waiting for SUCCESS message... (elapsed: {elapsed}s)")

                    elif message_image_status == "SUCCESS" and message_image_id == image_id:
                        processing_duration = message_attributes.get("processing_duration", {}).get("Value")
                        if processing_duration is not None:
                            assert float(processing_duration) > 0
                        done = True
                        elapsed = int(time.time() - start_time)
                        if processing_duration is not None:
                            logger.info(f"\tSUCCESS message found! Image took {processing_duration} seconds to process.")
                        else:
                            logger.info("\tSUCCESS message found!")
                        logger.info(f"Total wait: {elapsed}s")

                    elif (
                        message_image_status == "FAILED" or message_image_status == "PARTIAL"
                    ) and message_image_id == image_id:
                        failure_message = ""
                        try:
                            message_body = json.loads(message.body).get("Message", "")
                            failure_message = str(message_body)
                        except Exception:
                            pass
                        logger.error(
                            f"Failed to process image {image_id}. Status: {message_image_status}. {failure_message}"
                        )
                        raise AssertionError(f"Image processing failed with status: {message_image_status}")

                    else:
                        # Only log every 30 seconds to reduce noise
                        if max_retries % 6 == 0:  # 6 retries = 30 seconds
                            elapsed = int(time.time() - start_time)
                            logger.info(f"\tWaiting for {image_id}... (elapsed: {elapsed}s, retries left: {max_retries})")

                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse message body as JSON: {e}")
                except Exception as e:
                    logger.warning(f"Error processing message: {e}")

            # Delete all messages after processing the batch
            for message in messages:
                try:
                    message.delete()
                except Exception as e:
                    logger.warning(f"Failed to delete message: {e}")

            # If we found success, break out of the main retry loop
            if done:
                break

        except ClientError as err:
            logger.warning(f"ClientError in monitor_job_status: {err}")
            # Don't raise immediately, continue retrying

        except Exception as err:
            logger.error(f"Unexpected error in monitor_job_status: {err}")
            raise

        max_retries -= 1
        time.sleep(retry_interval)

    if not done:
        elapsed = int(time.time() - start_time)
        logger.error(f"Maximum retries reached waiting for {image_id}.")
        logger.error(f"Total time waited: {elapsed} seconds ({timeout_minutes} minutes)")
        raise TimeoutError(f"Image processing timed out after {timeout_minutes} minutes for image {image_id}")

    assert done


def get_kinesis_shard(kinesis_client: boto3.client) -> Dict[str, Any]:
    """
    Get a Kinesis shard iterator for result monitoring.

    Args:
        kinesis_client: Kinesis client for stream operations

    Returns:
        Shard iterator for monitoring results
    """
    config = get_config()
    stream_name = f"{config.KINESIS_RESULTS_STREAM_PREFIX}-{config.ACCOUNT}"
    stream_desc = kinesis_client.describe_stream(StreamName=stream_name)["StreamDescription"]
    return kinesis_client.get_shard_iterator(
        StreamName=stream_name, ShardId=stream_desc["Shards"][0]["ShardId"], ShardIteratorType="LATEST"
    )["ShardIterator"]


def validate_features_match(
    image_processing_request: Dict[str, Any],
    job_id: str,
    shard_iter: Optional[str] = None,
    s3_client: Optional[boto3.client] = None,
    kinesis_client: Optional[boto3.client] = None,
    result_file: Optional[str] = None,
    kinesis_features_cache: Optional[Dict[str, List[Feature]]] = None,
) -> None:
    """
    Validate that processing results match expected features.

    Args:
        image_processing_request: The original processing request
        job_id: Job ID for result correlation
        shard_iter: Kinesis shard iterator for streaming results
        s3_client: S3 client for bucket result validation
        kinesis_client: Kinesis client for stream result validation
        result_file: Path to expected results file

    Raises:
        AssertionError: If results don't match expected features
    """
    # Determine result file path
    if result_file is None:
        config = get_config()
        use_roi = ".roi" if config.REGION_OF_INTEREST else ""
        result_file = f"./test/data/{config.TARGET_MODEL}.{config.TARGET_IMAGE.split('/')[-1]}{use_roi}.geojson"

    logger.info(f"Validating against {result_file}")

    with open(result_file, "r") as geojson_file:
        expected_features = geojson.load(geojson_file)["features"]

    max_retries = 24  # 2 minutes with 5-second intervals
    retry_interval = 5
    done = False

    while not done and max_retries > 0:
        outputs: List[Dict[str, Any]] = image_processing_request["outputs"]
        found_outputs = 0

        for output in outputs:
            if output["type"] == "S3" and s3_client:
                if validate_s3_features_match(output["bucket"], output["prefix"], expected_features, s3_client):
                    found_outputs += 1
            elif output["type"] == "Kinesis":
                # Check if we have cached features first
                stream_name = output["stream"]
                if kinesis_features_cache and stream_name in kinesis_features_cache:
                    cached_features = kinesis_features_cache[stream_name]
                    if feature_collections_equal(expected_features, cached_features):
                        found_outputs += 1
                        logger.info("Kinesis validation passed using cached features")
                    else:
                        logger.info("Kinesis cached features don't match, will retry")
                elif kinesis_client:
                    # Try to read from Kinesis if we don't have cached features
                    try:
                        if validate_kinesis_features_match(
                            job_id, stream_name, shard_iter, expected_features, kinesis_client, kinesis_features_cache
                        ):
                            found_outputs += 1
                    except Exception as e:
                        logger.warning(f"Kinesis validation failed, will retry: {e}")
                else:
                    logger.warning("No Kinesis client available and no cached features")

        if found_outputs == len(outputs):
            done = True
            logger.info(f"{found_outputs} output sinks validated, tests succeeded!")
        else:
            max_retries -= 1
            time.sleep(retry_interval)
            logger.info(f"Not all output sinks were validated, retrying. Retries remaining: {max_retries}")

    if not done:
        logger.error(
            f"Validation failed after {24 * 5} seconds. Found {found_outputs} out of {len(outputs)} expected outputs."
        )
        raise AssertionError(
            f"Feature validation failed - only {found_outputs} out of {len(outputs)} output sinks validated"
        )


def validate_s3_features_match(bucket: str, prefix: str, expected_features: List[Feature], s3_client: boto3.client) -> bool:
    """
    Validate S3 output results against expected features.

    Args:
        bucket: S3 bucket name
        prefix: S3 object prefix
        expected_features: List of expected features
        s3_client: S3 client for operations

    Returns:
        True if features match, False otherwise
    """
    logger.info(f"Checking S3 at '{bucket}/{prefix}' for results.")

    for object_key in get_matching_s3_keys(s3_client, bucket, prefix=prefix, suffix=".geojson"):
        logger.info(f"Output: {object_key} found!")
        s3_output = s3_client.get_object(Bucket=bucket, Key=object_key)
        contents = s3_output["Body"].read()
        s3_features = geojson.loads(contents.decode("utf-8"))["features"]
        logger.info(f"S3 file contains {len(s3_features)} features")

        if feature_collections_equal(expected_features, s3_features):
            logger.info("S3 feature set matched expected features!")
            return True

    logger.info("S3 feature set didn't match expected features...")
    return False


def validate_kinesis_features_match(
    job_id: str,
    stream: str,
    shard_iter: Optional[str],
    expected_features: List[Feature],
    kinesis_client: boto3.client,
    cache: Optional[Dict[str, List[Feature]]] = None,
) -> bool:
    """
    Validate Kinesis output results against expected features.

    Args:
        job_id: Job ID for result correlation
        stream: Kinesis stream name
        shard_iter: Shard iterator for reading records
        expected_features: List of expected features
        kinesis_client: Kinesis client for operations
        cache: Optional dict to cache the features we read

    Returns:
        True if features match, False otherwise
    """
    logger.info(f"Checking Kinesis Stream '{stream}' for results.")

    if shard_iter is None:
        logger.warning("No shard iterator provided, skipping Kinesis validation")
        return False

    try:
        kinesis_features = []
        current_shard_iter = shard_iter
        max_iterations = 100  # Prevent infinite loops

        # Iterate through records until we find what we're looking for or exhaust the stream
        for iteration in range(max_iterations):
            response = kinesis_client.get_records(ShardIterator=current_shard_iter, Limit=10000)
            records = response.get("Records", [])
            logger.info(f"Found {len(records)} Kinesis records in iteration {iteration}")

            for record in records:
                partition_key = record.get("PartitionKey")
                logger.info(f"Kinesis record partition key: {partition_key}, looking for: {job_id}")
                if partition_key == job_id:
                    try:
                        # Kinesis records may be bytes, base64-encoded strings, or JSON strings
                        data = record["Data"]
                        data_length = len(data) if hasattr(data, "__len__") else "N/A"
                        logger.info(f"Kinesis record data type: {type(data)}, length: {data_length}")

                        # Handle different data types from Kinesis
                        if isinstance(data, bytes):
                            if len(data) == 0:
                                logger.warning("Received empty bytes from Kinesis record")
                                continue
                            data_str = data.decode("utf-8")
                        elif isinstance(data, str):
                            # Check if it's base64-encoded
                            try:
                                # Try to decode as base64 first
                                decoded = base64.b64decode(data)
                                data_str = decoded.decode("utf-8")
                                logger.info("Decoded base64-encoded data")
                            except Exception:
                                # If it fails, treat as plain string
                                data_str = data
                        else:
                            data_str = str(data)

                        logger.info(f"Data string (first 200 chars): {data_str[:200]}")
                        record_data = geojson.loads(data_str)
                        if "features" in record_data:
                            kinesis_features.extend(record_data["features"])
                            logger.info(f"Found {len(record_data['features'])} features in Kinesis record")
                        else:
                            data_keys = list(record_data.keys()) if record_data else "None"
                            logger.warning(f"Record data does not contain 'features' key. Keys: {data_keys}")
                    except (json.JSONDecodeError, KeyError, UnicodeDecodeError, Exception) as e:
                        logger.warning(f"Failed to parse Kinesis record data: {e}")
                        record_data_str = data[:200] if isinstance(data, str) else str(data)[:200]
                        logger.info(f"Record data type: {type(data)}, data: {record_data_str}")

            # Check if we have all expected features
            if len(kinesis_features) >= len(expected_features):
                # Cache the features for future use
                if cache is not None and kinesis_features:
                    cache[stream] = kinesis_features
                    logger.info(f"Cached {len(kinesis_features)} features from Kinesis stream '{stream}'")

                if feature_collections_equal(expected_features, kinesis_features):
                    logger.info(f"Kinesis record contains expected {len(kinesis_features)} features!")
                    return True

            # Check for next shard iterator
            next_shard_iter = response.get("NextShardIterator")
            if not next_shard_iter:
                break
            current_shard_iter = next_shard_iter

        # Final check even if we didn't read all expected features
        if kinesis_features:
            if cache is not None:
                cache[stream] = kinesis_features
                logger.info(f"Cached {len(kinesis_features)} features from Kinesis stream '{stream}'")

            if feature_collections_equal(expected_features, kinesis_features):
                logger.info(f"Kinesis record contains expected {len(kinesis_features)} features!")
                return True

        logger.info("Kinesis feature set didn't match expected features...")
        return False

    except Exception as e:
        logger.warning(f"Error reading from Kinesis stream: {e}")
        return False


def get_matching_s3_keys(s3_client: boto3.client, bucket: str, prefix: str = "", suffix: str = ""):
    """
    Generate S3 object keys matching the given criteria.

    Args:
        s3_client: S3 client for operations
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


def check_center_coord(expected: Optional[float], actual: Optional[float]) -> bool:
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


def feature_equal(expected: geojson.Feature, actual: geojson.Feature) -> bool:
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
            source_metadata_equal(expected.properties.get("sourceMetadata"), actual.properties.get("sourceMetadata")),
        ),
        (
            "Feature detection class matches",
            expected.properties.get("featureClasses") == actual.properties.get("featureClasses"),
        ),
        ("Image geometry matches", expected.properties.get("imageGeometry") == actual.properties.get("imageGeometry")),
        (
            "Center longitude matches",
            check_center_coord(expected.properties.get("center_longitude"), actual.properties.get("center_longitude")),
        ),
        (
            "Center latitude matches",
            check_center_coord(expected.properties.get("center_latitude"), actual.properties.get("center_latitude")),
        ),
    ]

    failed_checks = []
    for check, result in geojson_checks:
        if not result:
            failed_checks.append(check)

    if len(failed_checks) > 0:
        logger.info(f"Failed feature equality checks: {', '.join(failed_checks)}")
        logger.info("Expected feature:")
        logger.info(geojson.dumps(expected, indent=2))
        logger.info("Actual feature:")
        logger.info(geojson.dumps(actual, indent=2))
        return False

    return True


def source_metadata_equal(expected: List, actual: List) -> bool:
    """
    Compare source metadata lists for equality.

    Args:
        expected: Expected source metadata
        actual: Actual source metadata

    Returns:
        True if metadata lists are equivalent, False otherwise
    """
    # Handle case where one is None and the other is an empty list
    # These should be considered equivalent
    expected_len = 0 if expected is None else len(expected)
    actual_len = 0 if actual is None else len(actual)

    if expected_len == 0 and actual_len == 0:
        return True

    if expected is None or actual is None:
        logger.info("Expected and actual source metadata don't match (one is None)")
        return False

    if not len(expected) == len(actual):
        logger.info(f"Expected {len(expected)} source metadata but found {len(actual)}")
        return False

    for expected_source_metadata, actual_source_metadata in zip(expected, actual):
        is_equal = set({"location", "sourceDT"}).issuperset(
            k for (k, v) in expected_source_metadata.items() ^ actual_source_metadata.items()
        )

        if not is_equal:
            logger.info(f"Source metadata {expected_source_metadata} does not match actual {actual_source_metadata}")
            return False

    return True


def feature_collections_equal(expected: List[geojson.Feature], actual: List[geojson.Feature]) -> bool:
    """
    Compare two feature collections for equality.

    Args:
        expected: Expected feature collection
        actual: Actual feature collection

    Returns:
        True if collections are equivalent, False otherwise
    """
    if not len(expected) == len(actual):
        logger.info(f"Expected {len(expected)} features but found {len(actual)}")
        return False

    # Sort features by image geometry for consistent comparison
    expected.sort(key=lambda x: str(x.get("properties", {}).get("imageGeometry", {})))
    actual.sort(key=lambda x: str(x.get("properties", {}).get("imageGeometry", {})))

    for expected_feature, actual_feature in zip(expected, actual):
        if not feature_equal(expected_feature, actual_feature):
            logger.info(expected_feature)
            logger.info("does not match actual")
            logger.info(actual_feature)
            return False

    return True


def build_image_processing_request(
    endpoint: str,
    endpoint_type: str,
    image_url: str,
    model_variant: Optional[str] = None,
    target_container: Optional[str] = None,
    tile_size: int = 512,
    tile_overlap: int = 128,
    tile_format: str = "GTIFF",
    tile_compression: str = "NONE",
    post_processing: str = '[{"step": "FEATURE_DISTILLATION", "algorithm": {"algorithmType": "NMS", "iouThreshold": 0.75}}]',
    region_of_interest: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build an image processing request for submission to ModelRunner.

    Args:
        endpoint: Model endpoint name
        endpoint_type: Type of endpoint (SM/HTTP)
        image_url: URL of image to process
        model_variant: Optional SageMaker model variant
        target_container: Optional target container hostname
        tile_size: Size of image tiles for processing
        tile_overlap: Overlap between tiles
        tile_format: Format for tile output
        tile_compression: Compression for tile output
        post_processing: JSON string defining post-processing steps
        region_of_interest: Optional region of interest specification

    Returns:
        Complete image processing request dictionary
    """
    config = get_config()
    # Determine result destinations
    result_stream = f"{config.KINESIS_RESULTS_STREAM_PREFIX}-{config.ACCOUNT}"
    result_bucket = f"{config.S3_RESULTS_BUCKET_PREFIX}-{config.ACCOUNT}"

    logger.info(f"Starting ModelRunner image job in {config.REGION}")
    logger.info(f"Image: {image_url}")

    if model_variant:
        extra_info = f", Variant: {model_variant}"
    elif target_container:
        extra_info = f", Container: {target_container}"
    else:
        extra_info = ""

    logger.info(f"Type: {endpoint_type}, Model: {endpoint}{extra_info}")

    job_id = token_hex(16)
    job_name = f"test-{job_id}"
    logger.info(f"Creating request job_id={job_id}")

    # Debug: Log the image URL being used
    logger.debug(f"Image URL being processed: {image_url}")
    logger.debug(f"Image URL type: {type(image_url)}")
    logger.debug(f"Image URL starts with s3://: {image_url.startswith('s3://')}")

    image_processing_request: Dict[str, Any] = {
        "jobName": job_name,
        "jobId": job_id,
        "imageUrls": [image_url],
        "outputs": [
            {"type": "S3", "bucket": result_bucket, "prefix": f"{job_name}/"},
            {"type": "Kinesis", "stream": result_stream, "batchSize": 1000},
        ],
        "imageProcessor": {"name": endpoint, "type": endpoint_type},
        "imageProcessorTileSize": tile_size,
        "imageProcessorTileOverlap": tile_overlap,
        "imageProcessorTileFormat": tile_format,
        "imageProcessorTileCompression": tile_compression,
        "postProcessing": json.loads(post_processing),
        "regionOfInterest": region_of_interest,
    }

    if model_variant:
        image_processing_request["imageProcessorParameters"] = {"TargetVariant": model_variant}

    if target_container:
        image_processing_request["imageProcessorParameters"] = {"TargetContainerHostname": target_container}

    # Debug: Log the complete request structure
    logger.debug("Complete image processing request:")
    logger.debug(f"{json.dumps(image_processing_request, indent=2)}")

    return image_processing_request


def count_features(image_id: str, ddb_client: boto3.resource) -> int:
    """
    Count features in DynamoDB for a given image ID.

    Args:
        image_id: Image ID to count features for
        ddb_client: DynamoDB client for operations

    Returns:
        Number of features found
    """
    config = get_config()
    ddb_table = ddb_client.Table(config.DDB_FEATURES_TABLE)

    logger.info(f"Counting DDB items for image {image_id}...")
    items = query_items(ddb_table, "hash_key", image_id, True)

    features: List[Feature] = []
    for item in items:
        for feature in item["features"]:
            features.append(geojson.loads(feature))

    total_features = len(features)
    logger.info(f"Found {total_features} features!")
    return total_features


def validate_expected_feature_count(feature_count: int, model_variant: Optional[str] = None) -> None:
    """
    Validate feature count against expected values.

    Args:
        feature_count: Number of features found
        model_variant: Optional model variant for validation

    Raises:
        AssertionError: If feature count doesn't match expected values
    """
    config = get_config()
    expected_feature_counts = get_expected_image_feature_count(config.TARGET_IMAGE, variant=model_variant)
    test_succeeded = False

    if feature_count in expected_feature_counts:
        logger.info(f"Found expected features for image {config.TARGET_IMAGE}.")
        test_succeeded = True
    else:
        expected_feature_counts_str = " | ".join(map(str, expected_feature_counts))
        logger.info(f"Found {feature_count} features for image but expected {expected_feature_counts_str}!")

    assert test_succeeded


def get_expected_image_feature_count(image: str, variant: Optional[str] = None) -> List[int]:
    """
    Get expected feature counts for test images.

    Args:
        image: Image name/path
        variant: Optional model variant

    Returns:
        List of expected feature counts
    """
    if "large" in image:
        expected = 112200
    elif "tile" in image:
        expected = 2
    elif "sicd-capella-chip" in image or "sicd-umbra-chip" in image:
        expected = 100
    elif "sicd-interferometric" in image:
        expected = 15300
    else:
        raise Exception(f"Could not determine expected features for image: {image}")

    if variant:
        if variant == "flood-50":
            return [int(expected / 2)]
        else:
            return [expected]
    else:
        return [expected, int(expected / 2)]


def query_items(ddb_table: boto3.resource, hash_key: str, hash_value: str, is_feature_count: bool) -> List[Dict[str, Any]]:
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


def count_region_request_items(image_id: str, ddb_client: boto3.resource) -> int:
    """
    Count successful region requests for an image.

    Args:
        image_id: Image ID to count requests for
        ddb_client: DynamoDB client for operations

    Returns:
        Number of successful region requests
    """
    config = get_config()
    ddb_region_request_table = ddb_client.Table(config.DDB_REGION_REQUEST_TABLE)
    items = query_items(ddb_region_request_table, "image_id", image_id, False)

    total_count = 0
    for item in items:
        if item["region_status"] == "SUCCESS":
            total_count += 1

    logger.info(f"Found {total_count} Succeeded Region Request Items!")
    return total_count


def validate_expected_region_request_items(region_request_count: int) -> None:
    """
    Validate region request count against expected values.

    Args:
        region_request_count: Number of region requests found

    Raises:
        AssertionError: If count doesn't match expected values
    """
    config = get_config()
    expected_count = get_expected_region_request_count(config.TARGET_IMAGE)
    test_succeeded = False

    if region_request_count == expected_count:
        logger.info(f"Found expected region request for image {config.TARGET_IMAGE}.")
        test_succeeded = True
    else:
        logger.info(f"Found {region_request_count} region request for image but expected {expected_count}!")

    assert test_succeeded


def get_expected_region_request_count(image: str) -> int:
    """
    Get expected region request count for test images.

    Args:
        image: Image name/path

    Returns:
        Expected number of region requests
    """
    if "small" in image:
        return 1
    elif "meta" in image:
        return 1
    elif "large" in image:
        return 4
    elif "tile" in image:
        return 1
    elif "sicd-capella-chip" in image or "sicd-umbra-chip" in image:
        return 1
    elif "sicd-interferometric" in image:
        return 1
    elif "wbid" in image:
        return 1
    else:
        raise Exception(f"Could not determine expected region request for image: {image}")
