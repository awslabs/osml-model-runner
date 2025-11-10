#!/usr/bin/env python3

#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
CLI entry point for OSML Model Runner integration tests.

This script provides a command-line interface for running integration tests.
The test logic is implemented in the Runner class in test.integ.runner.
"""

import argparse
import json
import os
import sys

# Add the project root to Python path before importing other modules
_project_root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.insert(0, _project_root)

from test.integ.runner import Runner  # noqa: E402
from test.integ.types import ModelInvokeMode  # noqa: E402


def resolve_suite_path(suite_path: str) -> str:
    """
    Resolve test suite file path with multiple fallback strategies.

    Tries the following locations in order:
    1. Replace 'test/integration' with 'test/integ' (backward compatibility)
    2. Path as-is (absolute or relative to current directory)
    3. Relative to test/integ directory

    :param suite_path: Path to test suite JSON file.
    :returns: Resolved absolute path to the file.
    :raises FileNotFoundError: If file cannot be found in any location.
    """
    # Get the test/integ directory path
    test_integ_dir = os.path.join(_project_root, "test", "integ")

    # Special case: Handle backward compatibility for 'test/integration' paths
    if "test/integration" in suite_path:
        corrected_path = suite_path.replace("test/integration", "test/integ")
        # Handle both absolute and relative paths
        if os.path.isabs(corrected_path):
            resolved = corrected_path
        else:
            resolved = os.path.join(_project_root, corrected_path)
        if os.path.exists(resolved):
            return resolved

    # Try 1: Use path as-is (absolute or relative to CWD)
    if os.path.isabs(suite_path):
        if os.path.exists(suite_path):
            return suite_path
    else:
        # Try relative to current directory
        resolved = os.path.abspath(suite_path)
        if os.path.exists(resolved):
            return resolved

        # Try 2: Relative to test/integ directory (for paths like "suites/default.json")
        resolved = os.path.join(test_integ_dir, suite_path)
        if os.path.exists(resolved):
            return resolved

    # If we get here, file wasn't found - provide helpful error
    suggestions = [
        f"  - {os.path.abspath(suite_path)}",
        f"  - {os.path.join(test_integ_dir, suite_path)}",
    ]
    if "test/integration" in suite_path:
        corrected = suite_path.replace("test/integration", "test/integ")
        if os.path.isabs(corrected):
            suggestions.append(f"  - {corrected}")
        else:
            suggestions.append(f"  - {os.path.join(_project_root, corrected)}")

    raise FileNotFoundError(
        f"Test suite file not found: {suite_path}\n"
        f"Tried the following paths:\n" + "\n".join(suggestions) + "\n"
        f"Please checkthat {suite_path} exists and try again."
    )


def validate_args(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    """
    Validate command-line arguments.

    :param args: Parsed arguments namespace.
    :param parser: Argument parser instance for error reporting.
    :raises SystemExit: If arguments are invalid.
    """
    if args.suite:
        if args.image_uri or args.model_name:
            parser.error("Cannot specify both --suite and individual test parameters")
    else:
        if not args.image_uri or not args.model_name:
            parser.error("Either --suite or both image_uri and model_name must be provided")


def main() -> int:
    """
    Main entry point for the integration test runner.

    Provides command-line interface for running single tests or test suites.

    :returns: Exit code (0 for success, 1 for failure).
    """
    parser = argparse.ArgumentParser(
        description="OSML Integration Test Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
            Examples:
            # Run a single test
            python run-integration-tests.py s3://mr-test-imagery-${ACCOUNT}/small.tif centerpoint

            # Run a test with expected output validation
            python run-integration-tests.py s3://mr-test-imagery-${ACCOUNT}/small.tif centerpoint expected.json

            # Run a test with HTTP endpoint
            python run-integration-tests.py s3://my-bucket/image.tif my-model expected.json --http

            # Run a test suite
            python run-integration-tests.py --suite centerpoint_tests.json

            # Run with custom timeout and delay
            python run-integration-tests.py --suite centerpoint_tests.json --timeout 1 --delay 10
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
    validate_args(args, parser)

    # Initialize test runner
    runner = Runner(verbose=args.verbose)

    # Run tests
    if args.suite:
        # Load test suite - resolve path with multiple fallback strategies
        resolved_suite_path = resolve_suite_path(args.suite)
        with open(resolved_suite_path, "r") as f:
            test_cases = json.load(f)

        results = runner.run_test_suite(test_cases, args.timeout, args.delay)
    else:
        # Run single test
        model_invoke_mode = ModelInvokeMode.HTTP_ENDPOINT if args.http else ModelInvokeMode.SM_ENDPOINT

        image_request = runner._create_image_request(
            image_url=args.image_uri,
            model_name=args.model_name,
            model_invoke_mode=model_invoke_mode,
            model_variant=args.model_variant,
            target_container=args.target_container,
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
        print(f"\n[FAILED] {results['failed']} test(s) FAILED")
        return 1
    else:
        print(f"\n[PASSED] All {results['passed']} test(s) PASSED")
        return 0


if __name__ == "__main__":
    sys.exit(main())
