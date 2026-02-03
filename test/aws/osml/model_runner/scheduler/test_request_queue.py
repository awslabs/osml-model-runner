#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

# Mock messages for testing
TEST_MOCK_MESSAGE = {
    "Type": "Notification",
    "MessageId": "63077f04-26ef-5d12-824f-23059ffa2596",
    "TopicArn": "arn:aws:sns:us-west-2:012345678910:user-SNS-ImageStatusTopic9DE4DAE6-LFLJincx1Hka",
    "Message": "StatusMonitor update: IN_PROGRESS 07ac729f-a43d-4c96-952b-8126366fb298: Processing regions",
    "Timestamp": "2022-11-30T20:02:29.796Z",
    "MessageAttributes": {
        "job_id": {"Type": "String", "Value": "0"},
        "processing_duration": {"Type": "String", "Value": "0.290897369384765625"},
        "image_status": {"Type": "String", "Value": "IN_PROGRESS"},
        "image_id": {"Type": "String", "Value": "0:s3://test-images-012345678910/images/small.ntf"},
    },
}

TEST_MOCK_INVALID_MESSAGE = {"BAD_MESSAGE": "INVALID_MESSAGE"}


def get_mock_client_exception(mocker):
    """Create a mock exception to simulate client errors"""
    return mocker.Mock(side_effect=ClientError({"Error": {"Code": 500, "Message": "ClientError"}}, "send_message"))


@pytest.fixture
def request_queue_setup():
    """
    Sets up the test environment by creating a mock SQS queue
    and initializing the RequestQueue object.
    """
    from aws.osml.model_runner.app_config import BotoConfig
    from aws.osml.model_runner.scheduler.request_queue import RequestQueue

    with mock_aws():
        # Set up SQS resource
        sqs = boto3.resource("sqs", config=BotoConfig.default)
        sqs_response = sqs.create_queue(QueueName="mock_queue")
        mock_queue_url = sqs_response.url
        request_queue = RequestQueue(queue_url=mock_queue_url, wait_seconds=0)

        yield request_queue, sqs_response


def test_send_request_succeed(request_queue_setup):
    """
    Test that a message can be successfully sent to the SQS queue.
    """
    request_queue, sqs_response = request_queue_setup
    request_queue.send_request(TEST_MOCK_MESSAGE)

    # Verify that the message was added to the queue
    sqs_messages = sqs_response.receive_messages()
    assert sqs_messages[0].receipt_handle is not None


def test_reset_request_succeed(request_queue_setup):
    """
    Test that a message visibility timeout can be successfully reset.
    """
    request_queue, sqs_response = request_queue_setup
    request_queue.send_request(TEST_MOCK_MESSAGE)
    sqs_messages = sqs_response.receive_messages()
    receipt_handle = sqs_messages[0].receipt_handle

    # Reset the visibility timeout
    request_queue.reset_request(receipt_handle)
    sqs_messages = sqs_response.receive_messages()
    assert sqs_messages[0].receipt_handle is not None


def test_finish_request_succeed(request_queue_setup):
    """
    Test that a message can be successfully deleted from the SQS queue.
    """
    request_queue, sqs_response = request_queue_setup
    request_queue.send_request(TEST_MOCK_MESSAGE)
    sqs_messages = sqs_response.receive_messages()
    receipt_handle = sqs_messages[0].receipt_handle

    # Delete the message
    request_queue.finish_request(receipt_handle)
    sqs_messages = sqs_response.receive_messages()
    assert len(sqs_messages) == 0


def test_send_request_failure(request_queue_setup, mocker):
    """
    Test that the send_request method handles client errors gracefully.
    """
    request_queue, sqs_response = request_queue_setup
    request_queue.sqs_client.send_message = get_mock_client_exception(mocker)
    # Should not raise an exception
    request_queue.send_request(TEST_MOCK_MESSAGE)


def test_reset_request_failure(request_queue_setup, mocker):
    """
    Test that the reset_request method handles client errors gracefully.
    """
    request_queue, sqs_response = request_queue_setup
    request_queue.send_request(TEST_MOCK_MESSAGE)
    sqs_messages = sqs_response.receive_messages()
    receipt_handle = sqs_messages[0].receipt_handle

    request_queue.sqs_client.change_message_visibility = get_mock_client_exception(mocker)
    # Should not raise an exception
    request_queue.reset_request(receipt_handle)


def test_finish_request_failure(request_queue_setup, mocker):
    """
    Test that the finish_request method handles client errors gracefully.
    """
    request_queue, sqs_response = request_queue_setup
    request_queue.send_request(TEST_MOCK_MESSAGE)
    sqs_messages = sqs_response.receive_messages()
    receipt_handle = sqs_messages[0].receipt_handle

    request_queue.sqs_client.delete_message = get_mock_client_exception(mocker)
    # Should not raise an exception
    request_queue.finish_request(receipt_handle)


def test_iter_request_queue(request_queue_setup):
    """
    Test that the RequestQueue iterator correctly retrieves messages.
    """
    request_queue, sqs_response = request_queue_setup
    request_queue_iter = iter(request_queue)

    # Check if there's no pending request in the queue
    receipt_handle, request_message = next(request_queue_iter)
    assert receipt_handle is None
    assert request_message is None

    # Add a pending request to the queue
    request_queue.send_request(TEST_MOCK_MESSAGE)
    receipt_handle, request_message = next(request_queue_iter)
    assert receipt_handle is not None
    assert request_message is not None


def test_iter_request_queue_exception(request_queue_setup, mocker):
    """
    Test that the iterator handles exceptions when receiving messages.
    """
    request_queue, sqs_response = request_queue_setup
    request_queue_iter = iter(request_queue)

    # Simulate client exception
    request_queue.sqs_client.receive_message = get_mock_client_exception(mocker)
    # Should not raise an exception
    receipt_handle, request_message = next(request_queue_iter)
    assert receipt_handle is None
    assert request_message is None


def test_iter_request_queue_invalid_json(request_queue_setup):
    """
    Test that the iterator skips messages with invalid JSON bodies.
    """
    request_queue, sqs_response = request_queue_setup
    # Send a non-JSON message directly to the queue
    request_queue.sqs_client.send_message(QueueUrl=sqs_response.url, MessageBody="not-json")

    request_queue_iter = iter(request_queue)
    receipt_handle, request_message = next(request_queue_iter)
    assert receipt_handle is None
    assert request_message is None
