#!/usr/bin/env python3

#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
Simplified integration test runner for OSML Model Runner.

This module provides a clean, simple interface for running integration tests
that leverages the working test.py script approach with minimal configuration.
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from test.integration.utils.integ_utils import (
    build_image_processing_request,
    count_features,
    get_config,
    monitor_job_status,
    queue_image_processing_job,
    validate_features_match,
)
from typing import Any, Dict, List, Optional, Tuple

import boto3

# Add the project root to Python path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "../.."))


class SimpleTestRunner:
    """
    Simplified test runner that leverages the working test.py approach.

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
        image_uri: str,
        model_name: str,
        expected_output_path: Optional[str] = None,
        endpoint_type: str = "SM_ENDPOINT",
        timeout_minutes: int = 30,
        validate_results: bool = True,
        model_variant: Optional[str] = None,
        target_container: Optional[str] = None,
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Run a complete integration test.

        Args:
            image_uri: Full S3 URI to the test image
            model_name: Name of the model to test
            expected_output_path: Path to expected output file for validation
            endpoint_type: Type of endpoint (SM_ENDPOINT or HTTP_ENDPOINT)
            timeout_minutes: Maximum time to wait for completion
            validate_results: Whether to validate results against expected output
            model_variant: Optional SageMaker model variant
            target_container: Optional target container hostname

        Returns:
            Tuple of (success: bool, results: dict)
        """
        self.logger.info("=== Simple Integration Test Runner ===")
        self.logger.info(f"Image URI: {image_uri}")
        self.logger.info(f"Model Name: {model_name}")
        self.logger.info(f"Expected Output: {expected_output_path}")
        self.logger.info(f"Endpoint Type: {endpoint_type}")
        self.logger.info(f"Timeout: {timeout_minutes} minutes")
        self.logger.info("=====================================")

        try:
            # Build the image processing request
            self.logger.info("Building image processing request...")
            image_processing_request = build_image_processing_request(
                endpoint=model_name,
                endpoint_type=endpoint_type,
                image_url=image_uri,
                model_variant=model_variant,
                target_container=target_container,
            )

            # Submit the request
            self.logger.info("Submitting image processing request...")
            message_id = queue_image_processing_job(self.clients["sqs"], image_processing_request)
            self.logger.info(f"Request submitted with message ID: {message_id}")

            # Monitor the job
            self.logger.info("Monitoring job progress...")
            job_id = image_processing_request["jobId"]
            image_id = f"{job_id}:{image_uri}"

            # Use the existing monitoring function
            monitor_job_status(self.clients["sqs"], image_id, timeout_minutes)

            # Collect results
            results = {
                "job_id": job_id,
                "image_id": image_id,
                "image_uri": image_uri,
                "model_name": model_name,
                "endpoint_type": endpoint_type,
                "status": "completed",
                "timestamp": datetime.now().isoformat(),
            }

            # Validate results if requested
            if validate_results and expected_output_path and os.path.exists(expected_output_path):
                self.logger.info(f"Validating against expected output: {expected_output_path}")

                # Get Kinesis shard iterator for validation
                shard_iter = self._get_kinesis_shard()

                # Validate features match
                validate_features_match(
                    image_processing_request=image_processing_request,
                    job_id=job_id,
                    shard_iter=shard_iter,
                    s3_client=self.clients["s3"],
                    kinesis_client=self.clients["kinesis"],
                    result_file=expected_output_path,
                )

                # Count features in DynamoDB
                feature_count = count_features(image_id, self.clients["ddb"])
                results["feature_count"] = feature_count
                results["validation"] = "passed"

                self.logger.info(f"Validation passed with {feature_count} features")
            else:
                self.logger.warning("Skipping validation - no expected output provided or file not found")
                results["validation"] = "skipped"

            self.logger.info("Integration test completed successfully!")
            return True, results

        except Exception as e:
            self.logger.error(f"Integration test failed: {e}")
            return False, {"error": str(e), "timestamp": datetime.now().isoformat()}

    def _get_kinesis_shard(self) -> Dict[str, Any]:
        """Get a Kinesis shard iterator for result monitoring."""
        stream_name = f"{self.config.KINESIS_RESULTS_STREAM_PREFIX}-{self.config.ACCOUNT}"
        stream_desc = self.clients["kinesis"].describe_stream(StreamName=stream_name)["StreamDescription"]
        return self.clients["kinesis"].get_shard_iterator(
            StreamName=stream_name, ShardId=stream_desc["Shards"][0]["ShardId"], ShardIteratorType="LATEST"
        )["ShardIterator"]

    def run_test_suite(self, test_cases: List[Dict[str, Any]], timeout_minutes: int = 30) -> Dict[str, Any]:
        """
        Run a suite of integration tests.

        Args:
            test_cases: List of test case dictionaries
            timeout_minutes: Maximum time to wait for each test

        Returns:
            Dictionary with test results
        """
        self.logger.info(f"Running test suite with {len(test_cases)} test cases")

        results = {"total_tests": len(test_cases), "passed": 0, "failed": 0, "test_results": []}

        for i, test_case in enumerate(test_cases, 1):
            self.logger.info(f"Running test {i}/{len(test_cases)}: {test_case.get('name', 'Unnamed test')}")

            success, test_result = self.run_test(
                image_uri=test_case["image_uri"],
                model_name=test_case["model_name"],
                expected_output_path=test_case.get("expected_output"),
                endpoint_type=test_case.get("endpoint_type", "SM_ENDPOINT"),
                timeout_minutes=timeout_minutes,
                validate_results=test_case.get("validate_results", True),
                model_variant=test_case.get("model_variant"),
                target_container=test_case.get("target_container"),
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

        self.logger.info(f"Test suite completed: {results['passed']} passed, {results['failed']} failed")
        return results


def main():
    """Main entry point for the simple test runner."""
    parser = argparse.ArgumentParser(
        description="Simple integration test runner for OSML Model Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run a single test
  python3 simple_test_runner.py \\
    --image-uri "s3://mr-test-imagery-975050113711/small.tif" \\
    --model-name "centerpoint" \\
    --expected-output "expected_results.json"

  # Run a test suite
  python3 simple_test_runner.py \\
    --test-suite "test_suite.json" \\
    --timeout 45
        """,
    )

    # Single test options
    parser.add_argument("--image-uri", help="Full S3 URI to the test image (e.g., s3://bucket/image.tif)")

    parser.add_argument("--model-name", help="Name of the model to test")

    parser.add_argument("--expected-output", help="Path to expected output file for validation")

    parser.add_argument(
        "--endpoint-type", choices=["SM_ENDPOINT", "HTTP_ENDPOINT"], default="SM_ENDPOINT", help="Type of endpoint to test"
    )

    # Test suite options
    parser.add_argument("--test-suite", help="Path to test suite JSON file")

    # Common options
    parser.add_argument("--timeout", type=int, default=30, help="Timeout in minutes (default: 30)")

    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")

    parser.add_argument("--output", help="Output file for test results (JSON format)")

    parser.add_argument("--model-variant", help="SageMaker model variant (e.g., 'AllTraffic', 'flood-50')")

    parser.add_argument("--target-container", help="Target container hostname for multi-container endpoints")

    args = parser.parse_args()

    # Validate arguments
    if not args.test_suite and (not args.image_uri or not args.model_name):
        parser.error("Either --test-suite or both --image-uri and --model-name must be provided")

    # Initialize test runner
    runner = SimpleTestRunner(verbose=args.verbose)

    # Run tests
    if args.test_suite:
        # Load test suite
        with open(args.test_suite, "r") as f:
            test_cases = json.load(f)

        results = runner.run_test_suite(test_cases, args.timeout)
    else:
        # Run single test
        success, results = runner.run_test(
            image_uri=args.image_uri,
            model_name=args.model_name,
            expected_output_path=args.expected_output,
            endpoint_type=args.endpoint_type,
            timeout_minutes=args.timeout,
            model_variant=args.model_variant,
            target_container=args.target_container,
        )

        results = {"total_tests": 1, "passed": 1 if success else 0, "failed": 0 if success else 1, "test_results": [results]}

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
