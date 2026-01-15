# Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

"""
Locust setup and configuration hooks.

Pattern: keep this directory self-contained and runnable via:

    locust -f ./test/load --class-picker ...
"""

import logging
from typing import Optional

import boto3
from _image_job_status_monitor import ImageJobStatusMonitor
from _load_utils import safe_add_argument
from job_tracker import get_job_tracker
from locust import events

logger = logging.getLogger(__name__)

_shared_status_monitor: Optional[ImageJobStatusMonitor] = None


def get_shared_status_monitor() -> Optional[ImageJobStatusMonitor]:
    return _shared_status_monitor


@events.init_command_line_parser.add_listener
def add_custom_arguments(parser) -> None:
    """
    Register Model Runner load test CLI arguments.

    :param parser: Locust/argparse parser instance.
    :returns: None
    """
    safe_add_argument(parser, "--aws-account", type=str, default="", help="AWS Account ID")
    safe_add_argument(
        parser,
        "--aws-region",
        type=str,
        env_var="AWS_DEFAULT_REGION",
        default="us-west-2",
        help="AWS Region for Testing",
    )
    safe_add_argument(
        parser,
        "--mr-input-queue",
        type=str,
        default="ImageRequestQueue",
        help="Name of ModelRunner image request queue",
    )
    safe_add_argument(
        parser,
        "--mr-status-queue",
        type=str,
        default="ImageStatusQueue",
        help="Name of ModelRunner image status queue",
    )
    safe_add_argument(
        parser,
        "--test-imagery-location",
        type=str,
        default="s3://osml-test-images-<account>",
        help="S3 location of test images",
    )
    safe_add_argument(
        parser,
        "--test-results-location",
        type=str,
        default="s3://mr-bucket-sink-<account>",
        help="S3 location of image results",
    )
    safe_add_argument(
        parser,
        "--request-file",
        type=str,
        default="./test/load/sample-requests.json",
        help="Path to JSON file containing predefined requests (for PredefinedRequestsUser)",
    )


@events.test_start.add_listener
def on_test_start(environment, **_kwargs) -> None:
    """
    Initialize shared state needed by the load tests.

    This sets up the boto3 default session region, initializes the run's job tracker,
    and starts the shared SQS status monitor.

    :param environment: Locust `Environment` instance.
    :returns: None
    """
    global _shared_status_monitor

    boto3.setup_default_session(region_name=environment.parsed_options.aws_region)

    # Track test window for job_summary.json
    tracker = getattr(environment, "osml_job_tracker", None) or get_job_tracker()
    environment.osml_job_tracker = tracker
    tracker.mark_start()

    # Store monitor on the Locust Environment to avoid issues if this module is imported
    # multiple times under different names (which can happen when using `-f <directory>`).
    existing = getattr(environment, "osml_status_monitor", None)
    if existing is not None:
        _shared_status_monitor = existing
        return

    if _shared_status_monitor is None:
        # Note: this singleton monitor makes distributed Locust runs harder; acceptable for our expected profile.
        _shared_status_monitor = ImageJobStatusMonitor(
            status_queue_name=environment.parsed_options.mr_status_queue,
            status_queue_account=environment.parsed_options.aws_account,
            max_size=1000,
        )
        _shared_status_monitor.start()
        environment.osml_status_monitor = _shared_status_monitor
        logger.info(
            "Started status monitor for queue=%s account=%s",
            environment.parsed_options.mr_status_queue,
            environment.parsed_options.aws_account,
        )


@events.test_stop.add_listener
def on_test_stop(environment, **_kwargs) -> None:
    """
    Tear down shared state and write run artifacts.

    This writes job tracking artifacts and stops the shared status monitor.

    :param environment: Locust `Environment` instance.
    :returns: None
    """
    global _shared_status_monitor

    # Track end of test window and write job summary outputs
    tracker = getattr(environment, "osml_job_tracker", None) or get_job_tracker()
    tracker.mark_stop()
    tracker.write_outputs(environment)

    env_monitor = getattr(environment, "osml_status_monitor", None)
    if env_monitor is not None:
        try:
            env_monitor.stop()
            env_monitor.join(timeout=5)
        finally:
            environment.osml_status_monitor = None

    if _shared_status_monitor is not None:
        _shared_status_monitor.stop()
        _shared_status_monitor.join(timeout=5)
        _shared_status_monitor = None
