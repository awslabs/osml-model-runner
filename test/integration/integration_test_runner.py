#!/usr/bin/env python3

#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
Unified integration test runner for OSML Model Runner.

This module provides a clean, simple interface for running integration tests
without external dependencies on scripts/integration/test.py.
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from secrets import token_hex
from typing import Any, Dict, List, Optional, Tuple

# Add the project root to Python path before importing other modules
_project_root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../..")
sys.path.insert(0, _project_root)

# Also add src directory to path so aws.osml modules can be imported
_src_dir = os.path.join(_project_root, "src")
if _src_dir not in sys.path:
    sys.path.insert(0, _src_dir)

# Get the script directory for resolving relative paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Set required environment variables for AWS resources
# These environment variables define the SQS queues, DynamoDB tables, and other AWS resources
# that the integration tests will interact with
import boto3  # noqa: E402
import geojson  # noqa: E402
from boto3 import dynamodb  # noqa: E402
from botocore.exceptions import ClientError, ParamValidationError  # noqa: E402
from geojson import Feature  # noqa: E402

if "AWS_DEFAULT_REGION" not in os.environ:
    os.environ["AWS_DEFAULT_REGION"] = os.environ.get("AWS_REGION") or boto3.Session().region_name or "us-east-1"
if "IMAGE_REQUEST_TABLE" not in os.environ:
    os.environ["IMAGE_REQUEST_TABLE"] = "ImageRequestTable"
if "OUTSTANDING_IMAGE_REQUEST_TABLE" not in os.environ:
    os.environ["OUTSTANDING_IMAGE_REQUEST_TABLE"] = "OutstandingImageRequestTable"
if "REGION_REQUEST_TABLE" not in os.environ:
    os.environ["REGION_REQUEST_TABLE"] = "RegionRequestTable"
if "ENDPOINT_TABLE" not in os.environ:
    os.environ["ENDPOINT_TABLE"] = "EndpointStatisticsTable"
if "FEATURE_TABLE" not in os.environ:
    os.environ["FEATURE_TABLE"] = "FeatureTable"
if "IMAGE_QUEUE" not in os.environ:
    os.environ["IMAGE_QUEUE"] = "ImageRequestQueue"
if "IMAGE_DLQ" not in os.environ:
    os.environ["IMAGE_DLQ"] = "ImageDLQ"
if "REGION_QUEUE" not in os.environ:
    os.environ["REGION_QUEUE"] = "RegionRequestQueue"
if "WORKERS_PER_CPU" not in os.environ:
    os.environ["WORKERS_PER_CPU"] = "1"
if "WORKERS" not in os.environ:
    os.environ["WORKERS"] = "1"

# Now import modules that depend on the project root being in path
from test.integration.config import OSMLConfig  # noqa: E402
from test.integration.feature_validator import FeatureValidator  # noqa: E402
from test.integration.integration_types import ImageRequest, ModelInvokeMode  # noqa: E402


class IntegrationTestRunner:
    """
    Unified integration test runner for OSML Model Runner.

    This class provides a clean interface for running integration tests
    with minimal configuration and maximum simplicity.
    """

    def __init__(self, verbose: bool = False):
        """Initialize the test runner."""
        self.setup_logging(verbose)
        self.logger = logging.getLogger(__name__)
        self.clients = self._get_aws_clients()
        self.config = OSMLConfig()
        self.validator = FeatureValidator()

    def setup_logging(self, verbose: bool = False) -> None:
        """Set up logging configuration."""
        level = logging.DEBUG if verbose else logging.INFO

        # Use a cleaner format: just time and message for INFO, full details for others
        class InfoFormatter(logging.Formatter):
            def format(self, record):
                if record.levelno == logging.INFO:
                    return record.getMessage()
                else:
                    return f"⚠️  {record.getMessage()}"

        handler = logging.StreamHandler()
        handler.setFormatter(InfoFormatter())

        root_logger = logging.getLogger()
        root_logger.setLevel(level)
        root_logger.handlers = []
        root_logger.addHandler(handler)

    def _get_aws_clients(self) -> Dict[str, Any]:
        """Get AWS clients for the test."""
        return {
            "sqs": boto3.resource("sqs"),
            "s3": boto3.client("s3"),
            "kinesis": boto3.client("kinesis"),
            "ddb": boto3.resource("dynamodb"),
        }

    def run_test(
        self,
        image_request: ImageRequest,
        expected_output_path: Optional[str] = None,
        timeout_minutes: int = 30,
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Run a complete integration test.

        Args:
            image_request: ImageRequest object containing all request parameters
            expected_output_path: Path to expected output file for validation
            timeout_minutes: Maximum time to wait for completion

        Returns:
            Tuple of (success: bool, results: dict)
        """
        self.logger.info(f"\n{'='*60}")
        self.logger.info("🧪  OSML Integration Test")
        self.logger.info(f"{'='*60}")
        self.logger.info(f"Image: {image_request.image_url}")
        self.logger.info(f"Model: {image_request.model_name}")
        self.logger.info(f"Timeout: {timeout_minutes} minutes")
        self.logger.info(f"{'='*60}\n")

        try:
            # Extract model_variant and target_container from endpoint parameters
            model_variant = None
            target_container = None
            if image_request.model_endpoint_parameters:
                model_variant = image_request.model_endpoint_parameters.get("TargetVariant")
                target_container = image_request.model_endpoint_parameters.get("TargetContainerHostname")

            # Determine endpoint type from model_invoke_mode
            endpoint_type = "SM_ENDPOINT" if image_request.model_invoke_mode.value == "SM_ENDPOINT" else "HTTP_ENDPOINT"

            image_processing_request = self._build_image_processing_request(
                endpoint=image_request.model_name,
                endpoint_type=endpoint_type,
                image_url=image_request.image_url,
                model_variant=model_variant,
                target_container=target_container,
                tile_size=image_request.tile_size_scalar,
                tile_overlap=image_request.tile_overlap_scalar,
                tile_format=image_request.tile_format,
                tile_compression=image_request.tile_compression,
            )

            # Get Kinesis shard iterator BEFORE submitting the job so we can read all records
            shard_iter = self._get_kinesis_shard()

            # Submit the request
            message_id = self._queue_image_processing_job(image_processing_request)
            self.logger.info(f"📤 Request submitted (message ID: {message_id[:16]}...)")

            # Monitor the job
            job_id = image_processing_request["jobId"]
            image_id = f"{job_id}:{image_request.image_url}"

            # Use the existing monitoring function
            self._monitor_job_status(image_id, timeout_minutes)

            # Collect results
            results = {
                "job_id": job_id,
                "image_id": image_id,
                "image_uri": image_request.image_url,
                "model_name": image_request.model_name,
                "endpoint_type": endpoint_type,
                "status": "completed",
                "timestamp": datetime.now().isoformat(),
            }

            # Validate results if expected output is provided
            if expected_output_path:
                # Resolve the expected output path relative to script directory
                resolved_expected_path = os.path.abspath(expected_output_path)
                if os.path.exists(resolved_expected_path):
                    self.logger.info("\n🔍 Validating results...")

                    # Create a cache to store Kinesis features
                    kinesis_cache: Dict[str, List] = {}

                    # Validate features match
                    self._validate_features_match(
                        image_processing_request=image_processing_request,
                        job_id=job_id,
                        shard_iter=shard_iter,
                        result_file=resolved_expected_path,
                        kinesis_features_cache=kinesis_cache,
                    )

                    # Count features in DynamoDB
                    feature_count = self._count_features(image_id)
                    results["feature_count"] = feature_count
                    results["validation"] = "passed"

                    self.logger.info(f"✅ Validation passed with {feature_count} features\n")
                else:
                    self.logger.warning(f"⚠️  Expected output file not found: {resolved_expected_path}")
                    results["validation"] = "skipped - file not found"
            else:
                self.logger.info("⏭️  No expected output provided - skipping validation")
                results["validation"] = "skipped"

            self.logger.info("🎉 Integration test completed successfully!")
            return True, results

        except Exception as e:
            self.logger.error(f"Integration test failed: {e}")
            return False, {"error": str(e), "timestamp": datetime.now().isoformat()}

    def _get_kinesis_shard(self) -> str:
        """Get a Kinesis shard iterator for result monitoring."""
        stream_name = f"{self.config.KINESIS_RESULTS_STREAM_PREFIX}-{self.config.ACCOUNT}"
        stream_desc = self.clients["kinesis"].describe_stream(StreamName=stream_name)["StreamDescription"]
        return self.clients["kinesis"].get_shard_iterator(
            StreamName=stream_name, ShardId=stream_desc["Shards"][0]["ShardId"], ShardIteratorType="LATEST"
        )["ShardIterator"]

    def _build_image_processing_request(
        self,
        endpoint: str,
        endpoint_type: str,
        image_url: str,
        model_variant: Optional[str] = None,
        target_container: Optional[str] = None,
        tile_size: int = 512,
        tile_overlap: int = 128,
        tile_format: str = "GTIFF",
        tile_compression: str = "NONE",
        post_processing: str = (
            '[{"step": "FEATURE_DISTILLATION", ' '"algorithm": {"algorithmType": "NMS", "iouThreshold": 0.75}}]'
        ),
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
        # Determine result destinations
        result_stream = f"{self.config.KINESIS_RESULTS_STREAM_PREFIX}-{self.config.ACCOUNT}"
        result_bucket = f"{self.config.S3_RESULTS_BUCKET_PREFIX}-{self.config.ACCOUNT}"

        job_id = token_hex(16)
        job_name = f"test-{job_id}"

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

        return image_processing_request

    def _queue_image_processing_job(self, image_processing_request: Dict[str, Any]) -> Optional[str]:
        """
        Submit an image processing request to the SQS queue.

        Args:
            image_processing_request: The request to submit

            Returns:
            Message ID of the queued message

        Raises:
            ClientError: If SQS operation fails
            ParamValidationError: If request validation fails
        """
        try:
            queue = self.clients["sqs"].get_queue_by_name(
                QueueName=self.config.SQS_IMAGE_REQUEST_QUEUE, QueueOwnerAWSAccountId=self.config.ACCOUNT
            )
            response = queue.send_message(MessageBody=json.dumps(image_processing_request))

            message_id = response.get("MessageId")

            return message_id

        except ClientError as error:
            self.logger.error(f"Unable to send job request to SQS queue: {self.config.SQS_IMAGE_REQUEST_QUEUE}")
            self.logger.error(f"{error}")
            raise

        except ParamValidationError as error:
            self.logger.error("Invalid SQS API request; validation failed")
            self.logger.error(f"{error}")
            raise

    def _monitor_job_status(self, image_id: str, timeout_minutes: int = 30) -> None:
        """
        Monitor job status until completion or timeout.

        Args:
            image_id: Image ID to monitor
            timeout_minutes: Maximum time to wait for completion

        Raises:
            TimeoutError: If job doesn't complete within timeout
            AssertionError: If job fails
        """
        done = False
        max_retries = timeout_minutes * 12  # 12 retries per minute (5 second intervals)
        retry_interval = 5

        queue = self.clients["sqs"].get_queue_by_name(
            QueueName=self.config.SQS_IMAGE_STATUS_QUEUE, QueueOwnerAWSAccountId=self.config.ACCOUNT
        )

        self.logger.info(f"⏳ Monitoring job progress (timeout: {timeout_minutes} minutes)...")

        start_time = time.time()

        while not done and max_retries > 0:
            try:
                # Use WaitTimeSeconds for long polling to reduce API calls
                messages = queue.receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=5, VisibilityTimeout=30)

                # Process all messages in the batch
                for message in messages:
                    try:
                        # Parse the SNS message format
                        message_body = json.loads(message.body)
                        message_attributes = message_body.get("MessageAttributes", {})

                        # Extract values from SNS MessageAttributes format
                        message_image_id = message_attributes.get("image_id", {}).get("Value")
                        message_image_status = message_attributes.get("status", {}).get("Value")

                        if message_image_status == "IN_PROGRESS" and message_image_id == image_id:
                            elapsed = int(time.time() - start_time)
                            self.logger.info(f"📊 IN_PROGRESS - Image processing started (elapsed: {elapsed}s)")

                        elif message_image_status == "SUCCESS" and message_image_id == image_id:
                            processing_duration = message_attributes.get("processing_duration", {}).get("Value")
                            if processing_duration is not None:
                                assert float(processing_duration) > 0
                            done = True
                            elapsed = int(time.time() - start_time)
                            if processing_duration is not None:
                                self.logger.info(
                                    f"\n✅ SUCCESS - Processing completed in "
                                    f"{processing_duration}s (total wait: {elapsed}s)\n"
                                )
                            else:
                                self.logger.info(f"\n✅ SUCCESS - Processing completed (total wait: {elapsed}s)\n")

                        elif (
                            message_image_status == "FAILED" or message_image_status == "PARTIAL"
                        ) and message_image_id == image_id:
                            failure_message = ""
                            try:
                                message_body = json.loads(message.body).get("Message", "")
                                failure_message = str(message_body)
                            except Exception:
                                pass
                            self.logger.error(
                                f"❌ FAILED - Image processing failed with status: {message_image_status}. {failure_message}"
                            )
                            raise AssertionError(f"Image processing failed with status: {message_image_status}")

                        else:
                            # Only log every 30 seconds to reduce noise
                            if max_retries % 12 == 0:  # 12 retries = 60 seconds
                                elapsed = int(time.time() - start_time)
                                self.logger.info(f"⏳ Still waiting... (elapsed: {elapsed//60}m {elapsed%60}s)")

                    except json.JSONDecodeError as e:
                        self.logger.warning(f"Failed to parse message body as JSON: {e}")
                    except Exception as e:
                        self.logger.warning(f"Error processing message: {e}")

                # Delete all messages after processing the batch
                for message in messages:
                    try:
                        message.delete()
                    except Exception as e:
                        self.logger.warning(f"Failed to delete message: {e}")

                # If we found success, break out of the main retry loop
                if done:
                    break

            except ClientError as err:
                self.logger.warning(f"ClientError in monitor_job_status: {err}")
                # Don't raise immediately, continue retrying

            except Exception as err:
                self.logger.error(f"Unexpected error in monitor_job_status: {err}")
                raise

            max_retries -= 1
            time.sleep(retry_interval)

        if not done:
            elapsed = int(time.time() - start_time)
            self.logger.error(f"Maximum retries reached waiting for {image_id}.")
            self.logger.error(f"Total time waited: {elapsed} seconds ({timeout_minutes} minutes)")
            raise TimeoutError(f"Image processing timed out after {timeout_minutes} minutes for image {image_id}")

        assert done

    def _validate_features_match(
        self,
        image_processing_request: Dict[str, Any],
        job_id: str,
        shard_iter: Optional[str] = None,
        result_file: Optional[str] = None,
        kinesis_features_cache: Optional[Dict[str, List[Feature]]] = None,
    ) -> None:
        """
        Validate that processing results match expected features.

        Args:
            image_processing_request: The original processing request
            job_id: Job ID for result correlation
            shard_iter: Kinesis shard iterator for streaming results
            result_file: Path to expected results file
            kinesis_features_cache: Optional cache for Kinesis features

        Raises:
            AssertionError: If results don't match expected features
        """
        # Determine result file path
        if result_file is None:
            use_roi = ".roi" if self.config.REGION_OF_INTEREST else ""
            result_file = (
                f"./test/data/{self.config.TARGET_MODEL}.{self.config.TARGET_IMAGE.split('/')[-1]}{use_roi}.geojson"
            )

        with open(result_file, "r") as geojson_file:
            expected_features = geojson.load(geojson_file)["features"]

        max_retries = 24  # 2 minutes with 5-second intervals
        retry_interval = 5
        done = False

        while not done and max_retries > 0:
            outputs: List[Dict[str, Any]] = image_processing_request["outputs"]
            found_outputs = 0

            for output in outputs:
                if output["type"] == "S3" and self.clients["s3"]:
                    if self.validator.validate_s3_features(
                        self.clients["s3"], output["bucket"], output["prefix"], expected_features
                    ):
                        found_outputs += 1
                elif output["type"] == "Kinesis":
                    # Check if we have cached features first
                    stream_name = output["stream"]
                    if kinesis_features_cache and stream_name in kinesis_features_cache:
                        cached_features = kinesis_features_cache[stream_name]
                        if self.validator.feature_collections_equal(expected_features, cached_features):
                            found_outputs += 1
                    elif self.clients["kinesis"]:
                        # Try to read from Kinesis if we don't have cached features
                        try:
                            if self.validator.validate_kinesis_features(
                                self.clients["kinesis"],
                                job_id,
                                stream_name,
                                shard_iter,
                                expected_features,
                                kinesis_features_cache,
                            ):
                                found_outputs += 1
                        except Exception:
                            pass
                    else:
                        pass

            if found_outputs == len(outputs):
                done = True
            else:
                max_retries -= 1
                time.sleep(retry_interval)

        if not done:
            self.logger.error(
                f"Validation failed after {24 * 5} seconds. Found {found_outputs} out of {len(outputs)} expected outputs."
            )
            raise AssertionError(
                f"Feature validation failed - only {found_outputs} out of {len(outputs)} output sinks validated"
            )

    def _count_features(self, image_id: str) -> int:
        """
        Count features in DynamoDB for a given image ID.

        Args:
            image_id: Image ID to count features for

        Returns:
            Number of features found
        """
        ddb_table = self.clients["ddb"].Table(self.config.DDB_FEATURES_TABLE)

        items = self._query_items(ddb_table, "hash_key", image_id, True)

        features: List[Feature] = []
        for item in items:
            for feature in item["features"]:
                features.append(geojson.loads(feature))

        total_features = len(features)
        return total_features

    def _query_items(
        self, ddb_table: boto3.resource, hash_key: str, hash_value: str, is_feature_count: bool
    ) -> List[Dict[str, Any]]:
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

    def _create_image_request_from_test_case(self, test_case: Dict[str, Any]) -> ImageRequest:
        """
        Create an ImageRequest object from a test case dictionary.

        Args:
            test_case: Test case dictionary containing image_uri, model_name, etc.

        Returns:
            ImageRequest object
        """
        from secrets import token_hex

        job_id = token_hex(16)
        image_url = test_case["image_uri"]
        image_id = f"{job_id}:{image_url}"

        # Determine model invoke mode from endpoint type
        endpoint_type = test_case.get("endpoint_type", "SM_ENDPOINT")

        model_invoke_mode = ModelInvokeMode.SM_ENDPOINT if endpoint_type == "SM_ENDPOINT" else ModelInvokeMode.HTTP_ENDPOINT

        # Build endpoint parameters if needed
        model_endpoint_parameters = None
        if test_case.get("model_variant"):
            model_endpoint_parameters = {"TargetVariant": test_case["model_variant"]}
        elif test_case.get("target_container"):
            model_endpoint_parameters = {"TargetContainerHostname": test_case["target_container"]}

        return ImageRequest(
            job_id=job_id,
            image_id=image_id,
            image_url=image_url,
            model_name=test_case["model_name"],
            model_invoke_mode=model_invoke_mode,
            model_endpoint_parameters=model_endpoint_parameters,
        )

    def run_test_suite(
        self, test_cases: List[Dict[str, Any]], timeout_minutes: int = 30, delay_between_tests: int = 5
    ) -> Dict[str, Any]:
        """
        Run a suite of integration tests.

        Args:
            test_cases: List of test case dictionaries
            timeout_minutes: Maximum time to wait for each test
            delay_between_tests: Delay in seconds between tests

        Returns:
            Dictionary with test results
        """
        self.logger.info(f"\n🧪 Starting test suite: {len(test_cases)} test(s)")
        self.logger.info("=" * 60 + "\n")

        results = {"total_tests": len(test_cases), "passed": 0, "failed": 0, "test_results": []}

        for i, test_case in enumerate(test_cases, 1):
            self.logger.info(f"\n[{i}/{len(test_cases)}] {test_case.get('name', 'Unnamed test')}")

            # Add a delay between tests to avoid overwhelming the system
            if i > 1:
                time.sleep(delay_between_tests)

            # Create ImageRequest from test case
            image_request = self._create_image_request_from_test_case(test_case)

            success, test_result = self.run_test(
                image_request=image_request,
                expected_output_path=test_case.get("expected_output"),
                timeout_minutes=test_case.get("timeout_minutes", timeout_minutes),
            )

            test_result["test_name"] = test_case.get("name", f"Test {i}")
            test_result["success"] = success

            results["test_results"].append(test_result)

            if success:
                results["passed"] += 1
                self.logger.info("  ✅ PASSED")
            else:
                results["failed"] += 1
                self.logger.error("  ❌ FAILED")

        self.logger.info("\n" + "=" * 60)
        self.logger.info(f"Test suite completed: {results['passed']} passed, {results['failed']} failed")
        self.logger.info("=" * 60 + "\n")
        return results


def main():
    """Main entry point for the integration test runner."""
    parser = argparse.ArgumentParser(
        description="OSML Integration Test Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
            Examples:
            # Run a single test
            python integration_test_runner.py s3://mr-test-imagery-975050113711/small.tif centerpoint

            # Run a test with expected output validation
            python integration_test_runner.py s3://mr-test-imagery-975050113711/small.tif centerpoint expected.json

            # Run a test with HTTP endpoint
            python integration_test_runner.py s3://my-bucket/image.tif my-model expected.json --http

            # Run a test suite
            python integration_test_runner.py --suite centerpoint_tests.json

            # Run with custom timeout and delay
            python integration_test_runner.py --suite centerpoint_tests.json --timeout 1 --delay 10
        """,
    )

    # Positional arguments for single test (backward compatibility)
    parser.add_argument("image_uri", nargs="?", help="S3 URI to the test image")
    parser.add_argument("model_name", nargs="?", help="Name of the model to test")
    parser.add_argument("expected_output", nargs="?", help="Path to expected output file (optional)")

    # Test suite option
    parser.add_argument("--suite", help="Path to test suite JSON file")

    # Optional arguments
    parser.add_argument("--http", action="store_true", help="Use HTTP endpoint instead of SageMaker")
    parser.add_argument("--timeout", type=int, default=30, help="Timeout in minutes (default: 30)")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument("--output", help="Output file for test results (JSON format)")
    parser.add_argument("--delay", type=int, default=5, help="Delay in seconds between tests (default: 5)")
    parser.add_argument("--model-variant", help="SageMaker model variant (e.g., 'AllTraffic', 'flood-50')")
    parser.add_argument("--target-container", help="Target container hostname for multi-container endpoints")

    args = parser.parse_args()

    # Validate arguments
    if args.suite:
        if args.image_uri or args.model_name:
            parser.error("Cannot specify both --suite and individual test parameters")
    else:
        if not args.image_uri or not args.model_name:
            parser.error("Either --suite or both image_uri and model_name must be provided")

    # Initialize test runner
    runner = IntegrationTestRunner(verbose=args.verbose)

    # Run tests
    if args.suite:
        # Load test suite - resolve path relative to script directory
        resolved_suite_path = os.path.abspath(args.suite)
        with open(resolved_suite_path, "r") as f:
            test_cases = json.load(f)

        results = runner.run_test_suite(test_cases, args.timeout, args.delay)
    else:
        # Run single test
        # Create ImageRequest from command-line arguments
        from secrets import token_hex

        job_id = token_hex(16)
        image_id = f"{job_id}:{args.image_uri}"
        model_invoke_mode = ModelInvokeMode.HTTP_ENDPOINT if args.http else ModelInvokeMode.SM_ENDPOINT

        # Build endpoint parameters if needed
        model_endpoint_parameters = None
        if args.model_variant:
            model_endpoint_parameters = {"TargetVariant": args.model_variant}
        elif args.target_container:
            model_endpoint_parameters = {"TargetContainerHostname": args.target_container}

        image_request = ImageRequest(
            job_id=job_id,
            image_id=image_id,
            image_url=args.image_uri,
            model_name=args.model_name,
            model_invoke_mode=model_invoke_mode,
            model_endpoint_parameters=model_endpoint_parameters,
        )

        success, test_result = runner.run_test(
            image_request=image_request,
            expected_output_path=args.expected_output,
            timeout_minutes=args.timeout,
        )

        results = {
            "total_tests": 1,
            "passed": 1 if success else 0,
            "failed": 0 if success else 1,
            "test_results": [test_result],
        }

    # Output results
    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to {args.output}")
    else:
        print("\n=== Test Results ===")
        print(json.dumps(results, indent=2))

    # Exit with appropriate code
    if results["failed"] > 0:
        print(f"\n❌ {results['failed']} test(s) FAILED")
        sys.exit(1)
    else:
        print(f"\n✅ All {results['passed']} test(s) PASSED")
        sys.exit(0)


if __name__ == "__main__":
    main()
