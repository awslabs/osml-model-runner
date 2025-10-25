#!/usr/bin/env python3

#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
Ultra-simple integration test wrapper.

This script provides the simplest possible interface for running integration tests
by directly using the working test.py approach without complex dependencies.
"""

import argparse
import os
import subprocess
import sys

# Add the project root to Python path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "../.."))


def run_simple_test(image_uri, model_name, expected_output=None, http=False, timeout=30, verbose=False):
    """
    Run a simple integration test using the working test.py approach.

    Args:
        image_uri: S3 URI to the test image
        model_name: Name of the model to test
        expected_output: Path to expected output file (optional)
        http: Use HTTP endpoint instead of SageMaker
        timeout: Timeout in minutes
        verbose: Enable verbose logging

    Returns:
        Tuple of (success: bool, results: dict)
    """
    # Build the command
    cmd = [sys.executable, "test/integration/run_test.py", image_uri, model_name]

    if expected_output:
        cmd.append(expected_output)

    if http:
        cmd.append("--http")

    if timeout != 30:
        cmd.extend(["--timeout", str(timeout)])

    if verbose:
        cmd.append("--verbose")

    try:
        # Run the test
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout * 60)

        if result.returncode == 0:
            return True, {"status": "completed", "output": result.stdout}
        else:
            return False, {"error": result.stderr, "output": result.stdout}

    except subprocess.TimeoutExpired:
        return False, {"error": f"Test timed out after {timeout} minutes"}
    except Exception as e:
        return False, {"error": str(e)}


def main():
    """Ultra-simple test interface."""
    parser = argparse.ArgumentParser(
        description="Ultra-simple OSML integration test",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test with your image and model
  python3 test.py s3://mr-test-imagery-975050113711/small.tif centerpoint

  # Test with expected output validation
  python3 test.py s3://mr-test-imagery-975050113711/small.tif centerpoint expected.json

  # Test HTTP endpoint
  python3 test.py s3://my-bucket/image.tif my-model expected.json --http
        """,
    )

    parser.add_argument("image_uri", help="S3 URI to the test image")

    parser.add_argument("model_name", help="Name of the model to test")

    parser.add_argument("expected_output", nargs="?", help="Path to expected output file (optional)")

    parser.add_argument("--http", action="store_true", help="Use HTTP endpoint instead of SageMaker")

    parser.add_argument("--timeout", type=int, default=30, help="Timeout in minutes (default: 30)")

    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    # Run the test
    success, results = run_simple_test(
        image_uri=args.image_uri,
        model_name=args.model_name,
        expected_output=args.expected_output,
        http=args.http,
        timeout=args.timeout,
        verbose=args.verbose,
    )

    if success:
        print("✅ Test PASSED")
        if args.verbose and "output" in results:
            print(results["output"])
        sys.exit(0)
    else:
        print("❌ Test FAILED")
        if "error" in results:
            print(f"Error: {results['error']}")
        if args.verbose and "output" in results:
            print(f"Output: {results['output']}")
        sys.exit(1)


if __name__ == "__main__":
    main()
