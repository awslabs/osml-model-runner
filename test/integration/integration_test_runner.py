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
from typing import Any, Dict, List, Optional, Tuple

# Add the project root to Python path before importing other modules
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "../.."))

# Get the script directory for resolving relative paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Set required environment variables for ServiceConfig before importing OSML modules
# These are needed because ServiceConfig accesses os.environ[] at class definition time
import boto3  # noqa: E402

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
from test.integration.utils.integ_utils import (  # noqa: E402
    build_image_processing_request,
    count_features,
    get_config,
    monitor_job_status,
    queue_image_processing_job,
    validate_features_match,
)

from aws.osml.model_runner.api.image_request import ImageRequest  # noqa: E402


def resolve_path(path: str) -> str:
    """
    Resolve a path relative to the script directory.

    If the path is already absolute, return it as-is.
    If the path is relative, resolve it relative to the script directory.

    Args:
        path: The path to resolve

    Returns:
        The resolved absolute path
    """
    if os.path.isabs(path):
        return path

    # If the path starts with 'test/integration/', it's already relative to project root
    # so we need to resolve it relative to project root, not script directory
    if path.startswith("test/integration/"):
        project_root = os.path.join(SCRIPT_DIR, "../..")
        return os.path.join(project_root, path)

    # Otherwise, resolve relative to script directory
    return os.path.join(SCRIPT_DIR, path)


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
        self.config = get_config()

    def setup_logging(self, verbose: bool = False) -> None:
        """Set up logging configuration."""
        level = logging.DEBUG if verbose else logging.INFO
        logging.basicConfig(level=level, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

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
        self.logger.info("=== OSML Integration Test Runner ===")
        self.logger.info(f"Image URI: {image_request.image_url}")
        self.logger.info(f"Model Name: {image_request.model_name}")
        self.logger.info(f"Expected Output: {expected_output_path}")
        self.logger.info(f"Timeout: {timeout_minutes} minutes")
        self.logger.info("=====================================")

        try:
            # Build the image processing request from ImageRequest
            self.logger.info("Building image processing request...")

            # Extract model_variant and target_container from endpoint parameters
            model_variant = None
            target_container = None
            if image_request.model_endpoint_parameters:
                model_variant = image_request.model_endpoint_parameters.get("TargetVariant")
                target_container = image_request.model_endpoint_parameters.get("TargetContainerHostname")

            # Determine endpoint type from model_invoke_mode
            endpoint_type = "SM_ENDPOINT" if image_request.model_invoke_mode.name == "SM_ENDPOINT" else "HTTP_ENDPOINT"

            image_processing_request = build_image_processing_request(
                endpoint=image_request.model_name,
                endpoint_type=endpoint_type,
                image_url=image_request.image_url,
                model_variant=model_variant,
                target_container=target_container,
                tile_size=(
                    image_request.tile_size[0] if isinstance(image_request.tile_size, tuple) else image_request.tile_size
                ),
                tile_overlap=(
                    image_request.tile_overlap[0]
                    if isinstance(image_request.tile_overlap, tuple)
                    else image_request.tile_overlap
                ),
                tile_format=image_request.tile_format,
                tile_compression=image_request.tile_compression,
            )

            # Get Kinesis shard iterator BEFORE submitting the job so we can read all records
            shard_iter = self._get_kinesis_shard()

            # Submit the request
            self.logger.info("Submitting image processing request...")
            message_id = queue_image_processing_job(self.clients["sqs"], image_processing_request)
            self.logger.info(f"Request submitted with message ID: {message_id}")

            # Monitor the job
            self.logger.info("Monitoring job progress...")
            job_id = image_processing_request["jobId"]
            image_id = f"{job_id}:{image_request.image_url}"

            # Use the existing monitoring function
            monitor_job_status(self.clients["sqs"], image_id, timeout_minutes)

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
                resolved_expected_path = resolve_path(expected_output_path)
                if os.path.exists(resolved_expected_path):
                    self.logger.info(f"Validating against expected output: {resolved_expected_path}")

                    # Create a cache to store Kinesis features
                    kinesis_cache: Dict[str, List] = {}

                    # Validate features match
                    validate_features_match(
                        image_processing_request=image_processing_request,
                        job_id=job_id,
                        shard_iter=shard_iter,
                        s3_client=self.clients["s3"],
                        kinesis_client=self.clients["kinesis"],
                        result_file=resolved_expected_path,
                        kinesis_features_cache=kinesis_cache,
                    )

                    # Count features in DynamoDB
                    feature_count = count_features(image_id, self.clients["ddb"])
                    results["feature_count"] = feature_count
                    results["validation"] = "passed"

                    self.logger.info(f"Validation passed with {feature_count} features")
                else:
                    self.logger.warning(f"Expected output file not found: {resolved_expected_path}")
                    results["validation"] = "skipped - file not found"
            else:
                self.logger.info("No expected output provided - skipping validation")
                results["validation"] = "skipped"

            self.logger.info("Integration test completed successfully!")
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
        from aws.osml.model_runner.api.inference import ModelInvokeMode

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
        self.logger.info(f"Running test suite with {len(test_cases)} test cases")

        results = {"total_tests": len(test_cases), "passed": 0, "failed": 0, "test_results": []}

        for i, test_case in enumerate(test_cases, 1):
            self.logger.info(f"Running test {i}/{len(test_cases)}: {test_case.get('name', 'Unnamed test')}")

            # Add a delay between tests to avoid overwhelming the system
            if i > 1:
                self.logger.info(f"Waiting {delay_between_tests} seconds before starting next test...")
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
                self.logger.info(f"✅ Test {i} PASSED")
            else:
                results["failed"] += 1
                self.logger.error(f"❌ Test {i} FAILED")

            self.logger.info("-" * 50)

        self.logger.info(f"Test suite completed: {results['passed']} passed, {results['failed']} failed")
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
        resolved_suite_path = resolve_path(args.suite)
        with open(resolved_suite_path, "r") as f:
            test_cases = json.load(f)

        results = runner.run_test_suite(test_cases, args.timeout, args.delay)
    else:
        # Run single test
        # Create ImageRequest from command-line arguments
        from secrets import token_hex

        from aws.osml.model_runner.api.inference import ModelInvokeMode

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
