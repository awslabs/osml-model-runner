#  Copyright 2025-2026 Amazon.com, Inc. or its affiliates.

import pytest

from aws.osml.model_runner.api.image_request import ImageRequest
from aws.osml.model_runner.api.inference import ModelInvokeMode
from aws.osml.model_runner.scheduler.fifo_image_scheduler import FIFOImageScheduler


class MockRequestQueue:
    def __init__(self):
        self.messages = []
        self.finished_receipts = set()
        self.reset_receipts = set()

    def add_message(self, receipt_handle, message):
        self.messages.append((receipt_handle, message))

    def finish_request(self, receipt_handle):
        self.finished_receipts.add(receipt_handle)

    def reset_request(self, receipt_handle, visibility_timeout=0):
        self.reset_receipts.add(receipt_handle)

    def __iter__(self):
        return iter(self.messages)


@pytest.fixture
def fifo_scheduler_setup():
    """Set up FIFO scheduler with mock queue"""
    mock_queue = MockRequestQueue()
    scheduler = FIFOImageScheduler(mock_queue)
    return scheduler, mock_queue


def test_get_next_scheduled_request_success(fifo_scheduler_setup):
    """Test successful retrieval of next scheduled request"""
    scheduler, mock_queue = fifo_scheduler_setup

    # Setup
    test_receipt_handle = "receipt-123"
    test_message = {
        "jobName": "test-job-name",
        "jobId": "job-123",
        "imageUrls": ["test-image-url"],
        "outputs": [
            {"type": "S3", "bucket": "test-bucket", "prefix": "test-bucket-prefix"},
            {"type": "Kinesis", "stream": "test-stream", "batchSize": 1000},
        ],
        "imageProcessor": {"name": "test-model", "type": "SM_ENDPOINT"},
        "imageProcessorTileSize": 1024,
        "imageProcessorTileOverlap": 50,
    }
    mock_queue.add_message(test_receipt_handle, test_message)

    # Execute
    result = scheduler.get_next_scheduled_request()

    # Assert
    assert isinstance(result, ImageRequest)
    assert result.is_valid()
    assert result.job_id == "job-123"


def test_get_next_scheduled_request_empty_queue(fifo_scheduler_setup):
    """Test behavior when queue is empty"""
    scheduler, mock_queue = fifo_scheduler_setup

    # Execute
    result = scheduler.get_next_scheduled_request()

    # Assert
    assert result is None


def test_get_next_scheduled_request_invalid_request(fifo_scheduler_setup):
    """Test handling of invalid image request"""
    scheduler, mock_queue = fifo_scheduler_setup

    # Setup
    test_receipt_handle = "receipt-123"
    test_message = {
        "jobId": "job-123",
        # Missing required fields to make it invalid
    }
    mock_queue.add_message(test_receipt_handle, test_message)

    # Execute
    result = scheduler.get_next_scheduled_request()

    # Assert
    assert result is None
    assert test_receipt_handle in mock_queue.finished_receipts


def test_finish_request_success(fifo_scheduler_setup):
    """Test successful completion of request"""
    scheduler, mock_queue = fifo_scheduler_setup

    # Setup
    test_receipt_handle = "receipt-123"
    test_image_request = ImageRequest(
        job_id="job-123",
        image_id="image-123",
        image_url="s3://bucket/image.tif",
        image_read_role="arn:aws:iam::123456789012:role/read-role",
        outputs=[{"sink_type": "s3", "url": "s3://bucket/output/"}],
        model_name="test-model",
        model_invoke_mode=ModelInvokeMode.SM_ENDPOINT,
        model_invocation_role="arn:aws:iam::123456789012:role/invoke-role",
    )
    scheduler.job_id_to_message_handle["job-123"] = test_receipt_handle

    # Execute
    scheduler.finish_request(test_image_request)

    # Assert
    assert test_receipt_handle in mock_queue.finished_receipts
    assert "job-123" not in scheduler.job_id_to_message_handle


def test_finish_request_with_retry(fifo_scheduler_setup):
    """Test finishing request with retry flag"""
    scheduler, mock_queue = fifo_scheduler_setup

    # Setup
    test_receipt_handle = "receipt-123"
    test_image_request = ImageRequest(
        job_id="job-123",
        image_id="image-123",
        image_url="s3://bucket/image.tif",
        image_read_role="arn:aws:iam::123456789012:role/read-role",
        outputs=[{"sink_type": "s3", "url": "s3://bucket/output/"}],
        model_name="test-model",
        model_invoke_mode=ModelInvokeMode.SM_ENDPOINT,
        model_invocation_role="arn:aws:iam::123456789012:role/invoke-role",
    )
    scheduler.job_id_to_message_handle["job-123"] = test_receipt_handle

    # Execute
    scheduler.finish_request(test_image_request, should_retry=True)

    # Assert
    assert test_receipt_handle in mock_queue.reset_receipts
    assert "job-123" not in scheduler.job_id_to_message_handle


def test_multiple_requests_in_queue(fifo_scheduler_setup):
    """Test handling multiple requests in the queue"""
    scheduler, mock_queue = fifo_scheduler_setup

    # Setup
    test_messages = [
        (
            "receipt-1",
            {
                "jobName": "test-job-name",
                "jobId": "job-1",
                "imageUrls": ["test-image-url"],
                "outputs": [
                    {"type": "S3", "bucket": "test-bucket", "prefix": "test-bucket-prefix"},
                    {"type": "Kinesis", "stream": "test-stream", "batchSize": 1000},
                ],
                "imageProcessor": {"name": "test-model", "type": "SM_ENDPOINT"},
                "imageProcessorTileSize": 1024,
                "imageProcessorTileOverlap": 50,
            },
        ),
        (
            "receipt-2",
            {
                "jobName": "test-job-name",
                "jobId": "job-2",
                "imageUrls": ["test-image-url"],
                "outputs": [
                    {"type": "S3", "bucket": "test-bucket", "prefix": "test-bucket-prefix"},
                    {"type": "Kinesis", "stream": "test-stream", "batchSize": 1000},
                ],
                "imageProcessor": {"name": "test-model", "type": "SM_ENDPOINT"},
                "imageProcessorTileSize": 1024,
                "imageProcessorTileOverlap": 50,
            },
        ),
    ]

    for receipt, message in test_messages:
        mock_queue.add_message(receipt, message)

    # Execute and Assert first request
    first_request = scheduler.get_next_scheduled_request()
    assert isinstance(first_request, ImageRequest)
    assert first_request.job_id == "job-1"
    assert scheduler.job_id_to_message_handle["job-1"] == "receipt-1"

    # Execute and Assert second request
    second_request = scheduler.get_next_scheduled_request()
    assert isinstance(second_request, ImageRequest)
    assert second_request.job_id == "job-2"
    assert scheduler.job_id_to_message_handle["job-2"] == "receipt-2"
