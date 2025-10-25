#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
Integration test utilities for OSML Model Runner.

This module provides comprehensive utilities for integration testing including:
- Image processing workflow management
- Result validation and comparison
- Feature counting and analysis
- Job monitoring and status tracking
"""

import json
import logging
import time
from math import isclose
from secrets import token_hex
from typing import Any, Dict, List, Optional, Tuple

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


def run_model_on_image(
    sqs_client: boto3.resource,
    endpoint: str,
    endpoint_type: str,
    kinesis_client: Optional[boto3.client] = None,
    model_variant: Optional[str] = None,
    target_container: Optional[str] = None,
    timeout_minutes: int = 30,
) -> Tuple[str, str, Dict[str, Any], Optional[Dict[str, Any]]]:
    """
    Execute a complete image processing workflow against a model endpoint.

    This function orchestrates the entire image processing workflow:
    1. Builds an image processing request
    2. Submits it to the SQS queue
    3. Monitors job status until completion
    4. Returns results for validation

    Args:
        sqs_client: SQS client for queue operations
        endpoint: Model endpoint name to process against
        endpoint_type: Type of endpoint (SM/HTTP)
        kinesis_client: Optional Kinesis client for result streaming
        model_variant: Optional SageMaker model variant name
        target_container: Optional target container hostname
        timeout_minutes: Maximum time to wait for completion

    Returns:
        Tuple containing:
        - image_id: Unique identifier for the processed image
        - job_id: Job identifier
        - image_processing_request: The request that was submitted
        - shard_iter: Kinesis shard iterator for result monitoring

    Raises:
        TimeoutError: If image processing exceeds timeout
        AssertionError: If job fails or validation fails
    """
    config = get_config()
    image_url = config.TARGET_IMAGE

    if not image_url:
        raise ValueError("TARGET_IMAGE must be set in environment")

    # Build the image processing request
    image_processing_request = build_image_processing_request(
        endpoint, endpoint_type, image_url, model_variant=model_variant, target_container=target_container
    )

    # Get Kinesis shard iterator if client provided
    shard_iter = get_kinesis_shard(kinesis_client) if kinesis_client else None

    logger.info(f"Image processing request: {image_processing_request}")

    # Submit the request to SQS
    queue_image_processing_job(sqs_client, image_processing_request)

    # Extract job ID and create image ID
    job_id = image_processing_request["jobId"]
    image_id = job_id + ":" + image_processing_request["imageUrls"][0]

    logger.info(f"Using timeout of {timeout_minutes} minutes for image processing")

    # Monitor job status until completion
    monitor_job_status(sqs_client, image_id, timeout_minutes)

    return image_id, job_id, image_processing_request, shard_iter


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
            messages = queue.receive_messages()
            for message in messages:
                message_attributes = json.loads(message.body).get("MessageAttributes", {})
                message_image_id = message_attributes.get("image_id", {}).get("Value")
                message_image_status = message_attributes.get("status", {}).get("Value")

                if message_image_status == "IN_PROGRESS" and message_image_id == image_id:
                    elapsed = int(time.time() - start_time)
                    logger.info(f"\tIN_PROGRESS message found! Waiting for SUCCESS message... (elapsed: {elapsed}s)")

                elif message_image_status == "SUCCESS" and message_image_id == image_id:
                    processing_duration = message_attributes.get("processing_duration", {}).get("Value")
                    assert float(processing_duration) > 0
                    done = True
                    elapsed = int(time.time() - start_time)
                    logger.info(f"\tSUCCESS message found! Image took {processing_duration} seconds to process.")
                    logger.info(f"Total wait: {elapsed}s")

                elif (
                    message_image_status == "FAILED" or message_image_status == "PARTIAL"
                ) and message_image_id == image_id:
                    failure_message = ""
                    try:
                        message = json.loads(message.body).get("Message", "")
                        failure_message = str(message)
                    except Exception:
                        pass
                    logger.error(f"Failed to process image {image_id}. Status: {message_image_status}. {failure_message}")
                    raise AssertionError(f"Image processing failed with status: {message_image_status}")

                else:
                    # Only log every 30 seconds to reduce noise
                    if max_retries % 6 == 0:  # 6 retries = 30 seconds
                        elapsed = int(time.time() - start_time)
                        logger.info(f"\tWaiting for {image_id}... (elapsed: {elapsed}s, retries left: {max_retries})")

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
    shard_iter: Optional[Dict[str, Any]] = None,
    s3_client: Optional[boto3.client] = None,
    kinesis_client: Optional[boto3.client] = None,
    result_file: Optional[str] = None,
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
        use_roi = ".roi" if OSMLConfig.REGION_OF_INTEREST else ""
        result_file = f"./test/data/{OSMLConfig.TARGET_MODEL}.{OSMLConfig.TARGET_IMAGE.split('/')[-1]}{use_roi}.geojson"

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
            elif output["type"] == "Kinesis" and kinesis_client and shard_iter:
                if validate_kinesis_features_match(job_id, output["stream"], shard_iter, expected_features, kinesis_client):
                    found_outputs += 1

        if found_outputs == len(outputs):
            done = True
            logger.info(f"{found_outputs} output sinks validated, tests succeeded!")
        else:
            max_retries -= 1
            time.sleep(retry_interval)
            logger.info(f"Not all output sinks were validated, retrying. Retries remaining: {max_retries}")

    assert done


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
    shard_iter: Dict[str, Any],
    expected_features: List[Feature],
    kinesis_client: boto3.client,
) -> bool:
    """
    Validate Kinesis output results against expected features.

    Args:
        job_id: Job ID for result correlation
        stream: Kinesis stream name
        shard_iter: Shard iterator for reading records
        expected_features: List of expected features
        kinesis_client: Kinesis client for operations

    Returns:
        True if features match, False otherwise
    """
    logger.info(f"Checking Kinesis Stream '{stream}' for results.")
    records = kinesis_client.get_records(ShardIterator=shard_iter, Limit=10000)["Records"]
    kinesis_features = []

    for record in records:
        if record["PartitionKey"] == job_id:
            kinesis_features.extend(geojson.loads(record["Data"])["features"])
        else:
            logger.warning(f"Found partition key: {record['PartitionKey']}")
            logger.warning(f"Looking for partition key: {job_id}")

    if feature_collections_equal(expected_features, kinesis_features):
        logger.info(f"Kinesis record contains expected {len(kinesis_features)} features!")
        return True

    logger.info("Kinesis feature set didn't match expected features...")
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
            isclose(
                expected.properties.get("center_longitude"), actual.properties.get("center_longitude"), abs_tol=10 ** (-8)
            ),
        ),
        (
            "Center latitude matches",
            isclose(
                expected.properties.get("center_latitude"), actual.properties.get("center_latitude"), abs_tol=10 ** (-8)
            ),
        ),
    ]

    failed_checks = []
    for check, result in geojson_checks:
        if not result:
            failed_checks.append(check)

    if len(failed_checks) > 0:
        logger.info(f"Failed feature equality checks: {', '.join(failed_checks)}")
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
    if expected is None and actual is None:
        return True

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
) -> Dict[str, Any]:
    """
    Build an image processing request for submission to ModelRunner.

    Args:
        endpoint: Model endpoint name
        endpoint_type: Type of endpoint (SM/HTTP)
        image_url: URL of image to process
        model_variant: Optional SageMaker model variant
        target_container: Optional target container hostname

    Returns:
        Complete image processing request dictionary
    """
    config = get_config()
    # Determine result destinations
    if config.KINESIS_RESULTS_STREAM:
        result_stream = config.KINESIS_RESULTS_STREAM
    else:
        result_stream = f"{config.KINESIS_RESULTS_STREAM_PREFIX}-{config.ACCOUNT}"

    if config.S3_RESULTS_BUCKET:
        result_bucket = config.S3_RESULTS_BUCKET
    else:
        result_bucket = f"{config.S3_RESULTS_BUCKET_PREFIX}-{config.ACCOUNT}"

    logger.info(f"Starting ModelRunner image job in {config.REGION}")
    logger.info(f"Image: {image_url}")

    if model_variant:
        logger.info(f"Type: {endpoint_type}, Model: {endpoint}, Variant: {model_variant}")
    elif target_container:
        logger.info(f"Type: {endpoint_type}, Model: {endpoint}, Container: {target_container}")
    else:
        logger.info(f"Type: {endpoint_type}, Model: {endpoint}")

    job_id = token_hex(16)
    job_name = f"test-{job_id}"
    logger.info(f"Creating request job_id={job_id}")

    # Debug: Log the image URL being used
    logger.info(f"DEBUG: Image URL being processed: {image_url}")
    logger.info(f"DEBUG: Image URL type: {type(image_url)}")
    logger.info(f"DEBUG: Image URL starts with s3://: {image_url.startswith('s3://')}")

    image_processing_request: Dict[str, Any] = {
        "jobName": job_name,
        "jobId": job_id,
        "imageUrls": [image_url],
        "outputs": [
            {"type": "S3", "bucket": result_bucket, "prefix": f"{job_name}/"},
            {"type": "Kinesis", "stream": result_stream, "batchSize": 1000},
        ],
        "imageProcessor": {"name": endpoint, "type": endpoint_type},
        "imageProcessorTileSize": config.TILE_SIZE,
        "imageProcessorTileOverlap": config.TILE_OVERLAP,
        "imageProcessorTileFormat": config.TILE_FORMAT,
        "imageProcessorTileCompression": config.TILE_COMPRESSION,
        "postProcessing": json.loads(config.POST_PROCESSING),
        "regionOfInterest": config.REGION_OF_INTEREST,
    }

    if model_variant:
        image_processing_request["imageProcessorParameters"] = {"TargetVariant": model_variant}

    if target_container:
        image_processing_request["imageProcessorParameters"] = {"TargetContainerHostname": target_container}

    # Debug: Log the complete request structure
    logger.info("DEBUG: Complete image processing request:")
    logger.info(f"DEBUG: {json.dumps(image_processing_request, indent=2)}")

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
    ddb_table = ddb_client.Table(OSMLConfig.DDB_FEATURES_TABLE)

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
    expected_feature_counts = get_expected_image_feature_count(OSMLConfig.TARGET_IMAGE, variant=model_variant)
    test_succeeded = False

    if feature_count in expected_feature_counts:
        logger.info(f"Found expected features for image {OSMLConfig.TARGET_IMAGE}.")
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
    ddb_region_request_table = ddb_client.Table(OSMLConfig.DDB_REGION_REQUEST_TABLE)
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
    expected_count = get_expected_region_request_count(OSMLConfig.TARGET_IMAGE)
    test_succeeded = False

    if region_request_count == expected_count:
        logger.info(f"Found expected region request for image {OSMLConfig.TARGET_IMAGE}.")
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
