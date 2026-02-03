#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

import os

import boto3
import pytest
from moto import mock_aws

from aws.osml.model_runner.app_config import BotoConfig, ServiceConfig
from aws.osml.model_runner.common import RequestStatus
from aws.osml.model_runner.database.image_request_table import ImageRequestItem
from aws.osml.model_runner.status.exceptions import StatusMonitorException
from aws.osml.model_runner.status.image_status_monitor import ImageStatusMonitor


@pytest.fixture
def image_status_monitor_setup():
    """Sets up the SNS mock and test data."""
    with mock_aws():
        # Mock the SNS topic creation
        sns = boto3.client("sns", config=BotoConfig.default)
        sns_response = sns.create_topic(Name=os.environ["IMAGE_STATUS_TOPIC"])

        # Create an instance of ImageStatusMonitor for testing
        monitor = ImageStatusMonitor(sns_response.get("TopicArn"))

        # Set up test job item
        test_image_request_item = ImageRequestItem(
            job_id="test-job",
            image_id="test-image",
            processing_duration=1000,
            region_success=5,
            region_error=0,
            region_count=5,
        )

        yield monitor, test_image_request_item, sns


def test_process_event_success(image_status_monitor_setup):
    """Tests process_event for a successful image request item."""
    monitor, test_image_request_item, sns = image_status_monitor_setup
    status = RequestStatus.SUCCESS
    message = "Processing completed successfully."

    # No exception should be raised for a valid job item
    monitor.process_event(test_image_request_item, status, message)

    # Check if message was published to SNS
    response = sns.list_topics()
    assert ServiceConfig.image_status_topic in response["Topics"][0]["TopicArn"]


def test_process_event_failure(image_status_monitor_setup):
    """Tests process_event for a failed image request item with missing fields."""
    monitor, _, _ = image_status_monitor_setup
    invalid_image_request_item = ImageRequestItem(
        job_id=None,
        image_id="test-image",
        processing_duration=None,
        region_success=0,
        region_error=5,
        region_count=5,
    )
    status = RequestStatus.FAILED
    message = "Processing failed."

    with pytest.raises(StatusMonitorException):
        monitor.process_event(invalid_image_request_item, status, message)


def test_get_status_success(image_status_monitor_setup):
    """Tests get_status for a successful image request."""
    monitor, test_image_request_item, _ = image_status_monitor_setup
    status = monitor.get_status(test_image_request_item)
    assert status == RequestStatus.SUCCESS


def test_get_status_partial(image_status_monitor_setup):
    """Tests get_status for a partial image request."""
    monitor, test_image_request_item, _ = image_status_monitor_setup
    test_image_request_item.region_success = 3
    test_image_request_item.region_error = 2
    status = monitor.get_status(test_image_request_item)
    assert status == RequestStatus.PARTIAL


def test_get_status_failed(image_status_monitor_setup):
    """Tests get_status for a failed image request."""
    monitor, test_image_request_item, _ = image_status_monitor_setup
    test_image_request_item.region_success = 0
    test_image_request_item.region_error = 5  # All regions failed
    status = monitor.get_status(test_image_request_item)
    assert status == RequestStatus.FAILED


def test_get_status_in_progress(image_status_monitor_setup):
    """Tests get_status for an in-progress image request."""
    monitor, test_image_request_item, _ = image_status_monitor_setup
    test_image_request_item.region_success = 2
    test_image_request_item.region_error = 1
    test_image_request_item.region_count = 5  # Still in progress
    status = monitor.get_status(test_image_request_item)
    assert status == RequestStatus.IN_PROGRESS
