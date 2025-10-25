#!/usr/bin/env python3

#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
Ultra-simple test runner wrapper.

This script provides the simplest possible interface for running integration tests,
leveraging the working test.py approach with minimal configuration.
"""

import argparse
import os
import subprocess
import sys

# Add the project root to Python path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "../.."))


def run_test_with_suite(suite_file, timeout=30, verbose=False, output=None):
    """
    Run a test suite using the working test.py approach.

    Args:
        suite_file: Path to test suite JSON file
        timeout: Timeout in minutes
        verbose: Enable verbose logging
        output: Output file for results

    Returns:
        Dictionary with test results
    """
    import json

    # Load test suite
    with open(suite_file, "r") as f:
        test_cases = json.load(f)

    results = {"total_tests": len(test_cases), "passed": 0, "failed": 0, "test_results": []}

    for i, test_case in enumerate(test_cases, 1):
        print(f"Running test {i}/{len(test_cases)}: {test_case.get('name', 'Unnamed test')}")

        # Build command for this test
        cmd = [sys.executable, "scripts/integration/test.py", test_case["image_uri"], test_case["model_name"]]

        if test_case.get("expected_output"):
            cmd.append(test_case["expected_output"])

        if test_case.get("endpoint_type") == "HTTP_ENDPOINT":
            cmd.append("--http")

        if test_case.get("timeout_minutes", timeout) != timeout:
            cmd.extend(["--timeout", str(test_case.get("timeout_minutes", timeout))])

        if verbose:
            cmd.append("--verbose")

        try:
            # Run the test
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout * 60)

            test_result = {
                "test_name": test_case.get("name", f"Test {i}"),
                "success": result.returncode == 0,
                "image_uri": test_case["image_uri"],
                "model_name": test_case["model_name"],
                "output": result.stdout,
                "error": result.stderr if result.returncode != 0 else None,
            }

            if result.returncode == 0:
                results["passed"] += 1
                print(f"✅ Test {i} PASSED")
            else:
                results["failed"] += 1
                print(f"❌ Test {i} FAILED")

            results["test_results"].append(test_result)

        except subprocess.TimeoutExpired:
            test_result = {
                "test_name": test_case.get("name", f"Test {i}"),
                "success": False,
                "image_uri": test_case["image_uri"],
                "model_name": test_case["model_name"],
                "error": f"Test timed out after {timeout} minutes",
            }
            results["failed"] += 1
            results["test_results"].append(test_result)
            print(f"❌ Test {i} TIMED OUT")

        except Exception as e:
            test_result = {
                "test_name": test_case.get("name", f"Test {i}"),
                "success": False,
                "image_uri": test_case["image_uri"],
                "model_name": test_case["model_name"],
                "error": str(e),
            }
            results["failed"] += 1
            results["test_results"].append(test_result)
            print(f"❌ Test {i} FAILED: {e}")

    return results


def main():
    """Ultra-simple test interface."""
    parser = argparse.ArgumentParser(
        description="Ultra-simple OSML integration test runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test with your image and model
  python3 run_test.py s3://mr-test-imagery-975050113711/small.tif centerpoint

  # Test with expected output validation
  python3 run_test.py s3://mr-test-imagery-975050113711/small.tif centerpoint expected.json

  # Test HTTP endpoint
  python3 run_test.py s3://my-bucket/image.tif my-model expected.json --http

  # Run test suite
  python3 run_test.py --suite centerpoint_tests.json
        """,
    )

    # Positional arguments for single test
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

    args = parser.parse_args()

    # Validate arguments
    if args.suite:
        if args.image_uri or args.model_name:
            parser.error("Cannot specify both --suite and individual test parameters")
    else:
        if not args.image_uri or not args.model_name:
            parser.error("Either --suite or both image_uri and model_name must be provided")

    if args.suite:
        # Run test suite
        results = run_test_with_suite(args.suite, args.timeout, args.verbose, args.output)

        # Output results
        if args.output:
            import json

            with open(args.output, "w") as f:
                json.dump(results, f, indent=2)
            print(f"Results saved to {args.output}")
        else:
            print("\n=== Test Suite Results ===")
            import json

            print(json.dumps(results, indent=2))

        # Exit with appropriate code
        if results["failed"] > 0:
            print(f"\n❌ {results['failed']} test(s) FAILED")
            sys.exit(1)
        else:
            print(f"\n✅ All {results['passed']} test(s) PASSED")
            sys.exit(0)
    else:
        # Run single test using the working test.py script
        cmd = [sys.executable, "scripts/integration/test.py", args.image_uri, args.model_name]

        if args.expected_output:
            cmd.append(args.expected_output)

        if args.http:
            cmd.append("--http")

        if args.timeout != 30:
            cmd.extend(["--timeout", str(args.timeout)])

        if args.verbose:
            cmd.append("--verbose")

        try:
            result = subprocess.run(cmd, timeout=args.timeout * 60)
            sys.exit(result.returncode)
        except subprocess.TimeoutExpired:
            print("❌ Test TIMED OUT")
            sys.exit(1)
        except Exception as e:
            print(f"❌ Test FAILED: {e}")
            sys.exit(1)


if __name__ == "__main__":
    main()
