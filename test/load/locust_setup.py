#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
Locust test setup and configuration.

This module sets up the Locust test environment, including custom command-line
arguments and the shared status monitor.
"""

import logging
import os
import time
from datetime import datetime, timedelta
from test.config import LoadTestConfig
from test.load.locust_job_tracker import (
    display_statistics,
    get_job_tracker,
    write_job_status_file,
    write_job_summary_file,
)
from test.load.locust_status_monitor import ImageJobStatusMonitor
from threading import Thread
from typing import Optional

import boto3
from botocore.exceptions import ClientError
from locust import events

logger = logging.getLogger(__name__)

_shared_status_monitor: Optional[ImageJobStatusMonitor] = None
_stats_display_thread: Optional[Thread] = None
_start_time: Optional[datetime] = None


@events.init_command_line_parser.add_listener
def add_custom_arguments(parser) -> None:
    """
    Add custom command line arguments for the load test.

    :param parser: ArgumentParser instance to add arguments to
    """
    parser.add_argument(
        "--source-bucket",
        type=str,
        required=True,
        help="S3 bucket containing source images (or set S3_LOAD_TEST_SOURCE_IMAGE_BUCKET env var)",
    )
    parser.add_argument(
        "--result-bucket",
        type=str,
        required=True,
        help="S3 bucket for storing results (or set S3_LOAD_TEST_RESULT_BUCKET env var)",
    )
    parser.add_argument(
        "--model-name",
        type=str,
        default="centerpoint",
        help="SageMaker model name (default: centerpoint)",
    )
    parser.add_argument(
        "--aws-account",
        type=str,
        default=None,
        help="AWS Account ID (auto-detected if not provided)",
    )
    parser.add_argument(
        "--aws-region",
        type=str,
        default=None,
        help="AWS Region (auto-detected if not provided)",
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
    parser.add_argument(
        "--test-imagery-location",
        type=str,
        default="s3://osml-test-images-<account>",
        help="S3 location of test images (for RandomRequestUser and PredefinedRequestsUser)",
    )
    parser.add_argument(
        "--test-results-location",
        type=str,
        default="s3://mr-bucket-sink-<account>",
        help="S3 location of image results (for RandomRequestUser and PredefinedRequestsUser)",
    )
    parser.add_argument(
        "--request-file",
        type=str,
        default="./bin/locust/sample-requests.json",
        help="Path to JSON file containing predefined requests (for PredefinedRequestsUser)",
    )
    parser.add_argument(
        "--mr-input-queue",
        type=str,
        default=None,
        help="Name of ModelRunner image request queue (overrides config)",
    )


def _setup_file_logging(log_dir: str) -> None:
    """
    Set up file logging handler for job_log.log.

    :param log_dir: Directory for log files
    """
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "job_log.log")

    # Create file handler if it doesn't exist
    file_handler = logging.FileHandler(log_file, mode="w")
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)

    # Add handler to root logger
    root_logger = logging.getLogger()
    root_logger.addHandler(file_handler)


def _check_s3_bucket(bucket_name: str, s3_client) -> bool:
    """
    Check if S3 bucket exists and is accessible.

    :param bucket_name: Name of the bucket to check
    :param s3_client: Boto3 S3 client
    :return: True if bucket exists and is accessible, False otherwise
    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        return True
    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code", "")
        if error_code == "404":
            logger.error(f"S3 bucket '{bucket_name}' does not exist")
        elif error_code == "403":
            logger.error(f"Do not have permission to access S3 bucket '{bucket_name}'")
        else:
            logger.error(f"Error accessing S3 bucket '{bucket_name}': {error}")
        return False


def _get_model_instance_type(model_name: str, sm_client) -> Optional[str]:
    """
    Get SageMaker model instance type.

    :param model_name: Name of the SageMaker model
    :param sm_client: Boto3 SageMaker client
    :return: Instance type string or None if not found
    """
    try:
        list_endpoints_response = sm_client.list_endpoint_configs(NameContains=f"{model_name}")
        if not list_endpoints_response.get("EndpointConfigs"):
            logger.warning(f"No endpoint config found for model '{model_name}'")
            return None

        endpoint_name = list_endpoints_response["EndpointConfigs"][0]["EndpointConfigName"]
        endpoint_config_response = sm_client.describe_endpoint_config(EndpointConfigName=endpoint_name)

        if endpoint_config_response.get("ProductionVariants"):
            instance_type = endpoint_config_response["ProductionVariants"][0].get("InstanceType")
            return instance_type
        return None
    except ClientError as error:
        logger.warning(f"Error retrieving SageMaker instance type: {error}")
        return None


def _periodic_stats_display(environment, interval: int):
    """
    Periodically display statistics during test execution.

    :param environment: Locust environment instance
    :param interval: Interval in seconds between displays
    """
    job_tracker = get_job_tracker()
    while True:
        time.sleep(interval)
        if not environment.runner or not environment.runner.state:
            break
        if environment.runner.state == "stopping":
            break
        try:
            results = job_tracker.calculate_statistics()
            display_statistics(results)
            # Write status file periodically
            log_dir = getattr(environment.parsed_options, "log_dir", "logs")
            write_job_status_file(job_tracker, log_dir)
        except Exception as e:
            logger.warning(f"Error displaying statistics: {e}")


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """
    Initialize test environment when the load test starts.

    Sets up AWS session, configuration, and the shared status monitor.

    :param environment: Locust environment instance
    :param kwargs: Additional keyword arguments
    """
    global _shared_status_monitor, _stats_display_thread, _start_time

    _start_time = datetime.now()

    # Set up file logging
    log_dir = getattr(environment.parsed_options, "log_dir", "logs")
    _setup_file_logging(log_dir)

    # Set up AWS session
    config = LoadTestConfig()
    region = environment.parsed_options.aws_region or config.REGION or "us-west-2"
    boto3.setup_default_session(region_name=region)

    # Override config with command-line arguments if provided
    if environment.parsed_options.aws_account:
        config.ACCOUNT = environment.parsed_options.aws_account

    if not config.ACCOUNT:
        try:
            config.ACCOUNT = boto3.client("sts").get_caller_identity().get("Account")
        except Exception as e:
            logger.error(f"Failed to get AWS account: {e}")
            environment.runner.quit()
            return

    # Get test parameters
    source_bucket = getattr(environment.parsed_options, "source_bucket", None)
    result_bucket = getattr(environment.parsed_options, "result_bucket", None)
    model_name = getattr(environment.parsed_options, "model_name", "centerpoint")
    processing_window_min = getattr(environment.parsed_options, "processing_window_min", 1)

    # Validate S3 buckets exist
    s3_client = boto3.client("s3")
    if source_bucket:
        source_bucket_normalized = source_bucket.replace("s3://", "").rstrip("/")
        if not _check_s3_bucket(source_bucket_normalized, s3_client):
            logger.error(f"Source bucket '{source_bucket}' validation failed")
            environment.runner.quit()
            return

    if result_bucket:
        result_bucket_normalized = result_bucket.replace("s3://", "").rstrip("/")
        if not _check_s3_bucket(result_bucket_normalized, s3_client):
            logger.error(f"Result bucket '{result_bucket}' validation failed")
            environment.runner.quit()
            return

    # Get SageMaker instance type
    sm_client = boto3.client("sagemaker")
    instance_type = _get_model_instance_type(model_name, sm_client)

    # Calculate expected end time
    expected_end_time = _start_time + timedelta(minutes=processing_window_min)

    # Log startup parameters
    logger.info(
        f"""Starting load test with the following parameters:
            Start time: {_start_time}
            Expected end time: {expected_end_time}
            Processing window: {processing_window_min} minutes
            Input S3 Image Bucket: {source_bucket}
            Output S3 Result Bucket: {result_bucket}
            SageMaker Model: {model_name}
            SageMaker Instance Type: {instance_type or 'Unknown'}
            AWS Account: {config.ACCOUNT}
            AWS Region: {region}
            """
    )

    # Start the shared status monitor
    if _shared_status_monitor is None and config.IMAGE_STATUS_QUEUE_NAME:
        _shared_status_monitor = ImageJobStatusMonitor(
            status_queue_name=config.IMAGE_STATUS_QUEUE_NAME,
            status_queue_account=config.ACCOUNT,
        )
        _shared_status_monitor.start()
        logger.info("Started background status monitor")

    # Start periodic statistics display
    stats_interval = getattr(environment.parsed_options, "stats_interval", 30)
    _stats_display_thread = Thread(
        target=_periodic_stats_display, args=(environment, stats_interval), daemon=True, name="StatsDisplay"
    )
    _stats_display_thread.start()

    # Validate required configuration
    if not config.IMAGE_QUEUE_NAME:
        logger.error("IMAGE_QUEUE_NAME not configured. Ensure Model Runner is deployed.")
        environment.runner.quit()


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    """
    Clean up resources when the load test stops.

    Stops the status monitor, waits for completion if requested,
    calculates final statistics, and writes log files.

    :param environment: Locust environment instance
    :param kwargs: Additional keyword arguments
    """
    global _shared_status_monitor

    job_tracker = get_job_tracker()
    log_dir = getattr(environment.parsed_options, "log_dir", "logs")
    wait_for_completion = getattr(environment.parsed_options, "wait_for_completion", False)

    # Wait for completion if requested
    if wait_for_completion:
        logger.info("Waiting for all jobs to complete...")
        while not job_tracker.is_complete():
            time.sleep(5)
            results = job_tracker.calculate_statistics()
            display_statistics(results)
            write_job_status_file(job_tracker, log_dir)
        logger.info("All jobs completed!")

    # Calculate final statistics
    results = job_tracker.calculate_statistics()
    display_statistics(results)

    # Calculate stop time
    stop_time = datetime.now()

    # Write final log files
    write_job_status_file(job_tracker, log_dir)
    write_job_summary_file(results, log_dir, start_time=_start_time, stop_time=stop_time)

    # Log timing information
    if _start_time:
        actual_end_time = stop_time
        processing_window_min = getattr(environment.parsed_options, "processing_window_min", 1)
        expected_end_time = _start_time + timedelta(minutes=processing_window_min)
        total_elapsed_time = actual_end_time - _start_time
        logger.info(f"Start time: {_start_time}")
        logger.info(f"Expected end time: {expected_end_time}")
        logger.info(f"Actual end time: {actual_end_time}")
        logger.info(f"Total elapsed time: {total_elapsed_time.total_seconds()} seconds")

    # Stop the status monitor
    if _shared_status_monitor is not None:
        logger.info("Stopping status monitor...")
        _shared_status_monitor.stop()
        _shared_status_monitor.join(timeout=5)
        _shared_status_monitor = None
