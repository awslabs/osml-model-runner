#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

import os

import boto3
import pytest
from moto import mock_aws

from aws.osml.model_runner.app_config import BotoConfig, ServiceConfig
from aws.osml.model_runner.common import RequestStatus
from aws.osml.model_runner.database.region_request_table import RegionRequestItem
from aws.osml.model_runner.status.exceptions import StatusMonitorException
from aws.osml.model_runner.status.region_status_monitor import RegionStatusMonitor


@pytest.fixture
def region_status_monitor_setup():
    """Sets up the SNS mock and test data."""
    with mock_aws():
        # Mock the SNS topic creation
        sns = boto3.client("sns", config=BotoConfig.default)
        sns_response = sns.create_topic(Name=os.environ["REGION_STATUS_TOPIC"])

        # Create an instance of RegionStatusMonitor for testing
        monitor = RegionStatusMonitor(sns_response.get("TopicArn"))

        # Set up test region request item
        test_request_item = RegionRequestItem(
            job_id="test-job",
            image_id="test-image",
            region_id="test-region",
            processing_duration=1000,
            failed_tile_count=0,
            failed_tiles=[],
            succeeded_tile_count=0,
            succeeded_tiles=[],
            total_tiles=10,
        )

        yield monitor, test_request_item, sns


def test_process_event_success(region_status_monitor_setup):
    """Tests process_event for a successful region request item."""
    monitor, test_request_item, sns = region_status_monitor_setup
    status = RequestStatus.SUCCESS
    message = "Processing completed successfully."

    # No exception should be raised for a valid region request item
    monitor.process_event(test_request_item, status, message)

    # Check if message was published to SNS
    response = sns.list_topics()
    assert ServiceConfig.region_status_topic in response["Topics"][0]["TopicArn"]


def test_process_event_failure(region_status_monitor_setup):
    """Tests process_event for a failed region request item with missing fields."""
    monitor, _, _ = region_status_monitor_setup
    invalid_request_item = RegionRequestItem(
        job_id=None,  # Required field
        image_id="test-image",
        region_id="test-region",
        processing_duration=None,  # Required field
        failed_tiles=[],
        total_tiles=10,
    )
    status = RequestStatus.FAILED
    message = "Processing failed."

    with pytest.raises(StatusMonitorException):
        monitor.process_event(invalid_request_item, status, message)


def test_get_status_success(region_status_monitor_setup):
    """Tests get_status for a successful region request."""
    monitor, test_request_item, _ = region_status_monitor_setup
    status = monitor.get_status(test_request_item)
    assert status == RequestStatus.SUCCESS


def test_get_status_partial(region_status_monitor_setup):
    """Tests get_status for a partial region request."""
    monitor, test_request_item, _ = region_status_monitor_setup
    test_request_item.failed_tile_count = 3  # Some tiles failed
    status = monitor.get_status(test_request_item)
    assert status == RequestStatus.PARTIAL


def test_get_status_failed(region_status_monitor_setup):
    """Tests get_status for a failed region request."""
    monitor, test_request_item, _ = region_status_monitor_setup
    test_request_item.failed_tile_count = 10  # All tiles failed
    status = monitor.get_status(test_request_item)
    assert status == RequestStatus.FAILED
