#!/usr/bin/env python3

#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
CLI entry point for OSML Model Runner load tests.

This script provides a command-line interface for running load tests using Locust.
The test logic is implemented using Locust for concurrent load generation.
"""

import argparse
import os
import subprocess
import sys

# Add the project root to Python path before importing other modules
_project_root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.insert(0, _project_root)


def main() -> int:
    """
    Main entry point for the load test runner.

    Provides command-line interface for running load tests using Locust.

    :returns: Exit code (0 for success, 1 for failure).
    """
    parser = argparse.ArgumentParser(
        description="OSML Load Test Runner (Locust-based)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
            Examples:
            # Run load test with command-line arguments (headless mode)
            python run-load-tests.py \\
              --source-bucket s3://my-source-bucket \\
              --result-bucket s3://my-result-bucket \\
              --model-name centerpoint \\
              --users 5 \\
              --spawn-rate 1 \\
              --processing-window-min 10

            # Run with web UI (interactive mode)
            python run-load-tests.py \\
              --source-bucket s3://my-bucket \\
              --result-bucket s3://my-results \\
              --model-name centerpoint

            # Run with environment variables
            export S3_LOAD_TEST_SOURCE_IMAGE_BUCKET=s3://my-source-bucket
            export S3_LOAD_TEST_RESULT_BUCKET=s3://my-result-bucket
            python run-load-tests.py --users 5 --spawn-rate 1

            Note: This script wraps Locust. For advanced options, use Locust directly:
            locust -f test/load/locustfile.py --headless --users 5 --spawn-rate 1 \\
              --source-bucket s3://my-bucket --result-bucket s3://my-results
        """,
    )

    # Required arguments
    parser.add_argument(
        "--source-bucket",
        help="S3 bucket containing source images (or set S3_LOAD_TEST_SOURCE_IMAGE_BUCKET env var)",
    )
    parser.add_argument(
        "--result-bucket",
        help="S3 bucket for storing results (or set S3_LOAD_TEST_RESULT_BUCKET env var)",
    )

    # Optional arguments
    parser.add_argument(
        "--model-name",
        default="centerpoint",
        help="SageMaker model name (default: centerpoint)",
    )
    parser.add_argument(
        "--users",
        type=int,
        default=1,
        help="Number of concurrent users (default: 1)",
    )
    parser.add_argument(
        "--spawn-rate",
        type=float,
        default=1.0,
        help="Rate to spawn users per second (default: 1.0)",
    )
    parser.add_argument(
        "--processing-window-min",
        type=int,
        default=1,
        help="Processing window duration in minutes (default: 1)",
    )
    parser.add_argument(
        "--max-queue-depth",
        type=int,
        default=3,
        help="Maximum queue depth before throttling (default: 3)",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run in headless mode (no web UI)",
    )
    parser.add_argument(
        "--host",
        default="http://localhost",
        help="Host URL for Locust (default: http://localhost)",
    )
    parser.add_argument(
        "--web-host",
        default="0.0.0.0",
        help="Web UI host (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--web-port",
        type=int,
        default=8089,
        help="Web UI port (default: 8089)",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument(
        "--output",
        help="Output file prefix for CSV results (requires --headless)",
    )
    parser.add_argument(
        "--stats-interval",
        type=int,
        default=30,
        help="Interval in seconds for displaying statistics (default: 30)",
    )
    parser.add_argument(
        "--log-dir",
        type=str,
        default="logs",
        help="Directory for writing log files (default: logs)",
    )
    parser.add_argument(
        "--wait-for-completion",
        action="store_true",
        help="Wait for all jobs to complete after time window expires",
    )

    args = parser.parse_args()

    # Get bucket names from args or environment
    source_bucket = args.source_bucket or os.getenv("S3_LOAD_TEST_SOURCE_IMAGE_BUCKET")
    result_bucket = args.result_bucket or os.getenv("S3_LOAD_TEST_RESULT_BUCKET")

    if not source_bucket:
        parser.error("--source-bucket is required (or set S3_LOAD_TEST_SOURCE_IMAGE_BUCKET env var)")
    if not result_bucket:
        parser.error("--result-bucket is required (or set S3_LOAD_TEST_RESULT_BUCKET env var)")

    # Build Locust command
    locustfile = os.path.join(_project_root, "test", "load", "locustfile.py")
    cmd = [
        sys.executable,
        "-m",
        "locust",
        "-f",
        locustfile,
        "--source-bucket",
        source_bucket,
        "--result-bucket",
        result_bucket,
        "--model-name",
        args.model_name,
        "--processing-window-min",
        str(args.processing_window_min),
        "--max-queue-depth",
        str(args.max_queue_depth),
        "--host",
        args.host,
    ]

    # Add new tracking/logging arguments
    cmd.extend(
        [
            "--stats-interval",
            str(args.stats_interval),
            "--log-dir",
            args.log_dir,
        ]
    )
    if args.wait_for_completion:
        cmd.append("--wait-for-completion")

    if args.headless:
        cmd.extend(
            [
                "--headless",
                "--users",
                str(args.users),
                "--spawn-rate",
                str(args.spawn_rate),
            ]
        )
        if args.output:
            cmd.extend(["--csv", args.output.replace(".json", "")])

    else:
        cmd.extend(
            [
                "--web-host",
                args.web_host,
                "--web-port",
                str(args.web_port),
            ]
        )

    if args.verbose:
        cmd.append("--loglevel")
        cmd.append("DEBUG")

    # Run Locust
    try:
        return subprocess.call(cmd)
    except KeyboardInterrupt:
        print("\n[INTERRUPTED] Load test interrupted by user")
        return 130
    except Exception as e:
        print(f"\n[FAILED] Load test failed: {e}", file=sys.stderr)
        if args.verbose:
            import traceback

            traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
