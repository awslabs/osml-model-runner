#  Copyright 2025-2026 Amazon.com, Inc. or its affiliates.

import json
import time
from unittest.mock import MagicMock, Mock

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from aws.osml.model_runner.api import ImageRequest
from aws.osml.model_runner.database.requested_jobs_table import RequestedJobsTable
from aws.osml.model_runner.exceptions import LoadImageException
from aws.osml.model_runner.scheduler import BufferedImageRequestQueue, EndpointVariantSelector
from aws.osml.model_runner.tile_worker import RegionCalculator


def create_sample_image_request_message_body(job_name: str = "test-job") -> dict:
    """Helper function to create a sample image request message"""
    return {
        "jobName": job_name,
        "jobId": f"{job_name}-id",
        "imageUrls": ["s3://test-bucket/test.nitf"],
        "outputs": [
            {"type": "S3", "bucket": "test-bucket", "prefix": "results"},
            {"type": "Kinesis", "stream": "test-stream", "batchSize": 1000},
        ],
        "imageProcessor": {"name": "test-model", "type": "SM_ENDPOINT"},
        "imageProcessorTileSize": 2048,
        "imageProcessorTileOverlap": 50,
    }


def create_mock_metrics_logger():
    """Create a mock MetricsLogger that passes isinstance checks"""
    from aws_embedded_metrics.logger.metrics_logger import MetricsLogger

    mock_metrics = MagicMock(spec=MetricsLogger)
    mock_metrics.put_dimensions = Mock()
    mock_metrics.put_metric = Mock()
    mock_metrics.reset_dimensions = Mock()
    return mock_metrics


@pytest.fixture
def buffered_queue_setup():
    """Set up test fixtures for BufferedImageRequestQueue tests."""
    with mock_aws():
        # Set up mock AWS resources
        sqs = boto3.client("sqs")
        dynamodb = boto3.resource("dynamodb")

        # Create SQS queues
        queue_url = sqs.create_queue(QueueName="test-image-queue")["QueueUrl"]
        dlq_url = sqs.create_queue(QueueName="test-image-dlq")["QueueUrl"]

        # Create DynamoDB table
        table_name = "test-requested-jobs"
        dynamodb.create_table(
            TableName=table_name,
            KeySchema=[{"AttributeName": "endpoint_id", "KeyType": "HASH"}, {"AttributeName": "job_id", "KeyType": "RANGE"}],
            AttributeDefinitions=[
                {"AttributeName": "endpoint_id", "AttributeType": "S"},
                {"AttributeName": "job_id", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        # Create RequestedJobsTable instance
        jobs_table = RequestedJobsTable(table_name)

        # Create BufferedImageRequestQueue instance
        queue = BufferedImageRequestQueue(
            image_queue_url=queue_url,
            image_dlq_url=dlq_url,
            requested_jobs_table=jobs_table,
            max_jobs_lookahead=10,
            retry_time=60,
            max_retry_attempts=2,
        )

        yield queue, sqs, dynamodb, queue_url, dlq_url, jobs_table, table_name

        # Cleanup
        dynamodb.Table(table_name).delete()
        sqs.delete_queue(QueueUrl=queue_url)
        sqs.delete_queue(QueueUrl=dlq_url)


@pytest.fixture
def buffered_queue_metrics_setup():
    """Set up test fixtures for metrics emission tests."""
    with mock_aws():
        sqs = boto3.client("sqs", region_name="us-west-2")
        dynamodb = boto3.resource("dynamodb", region_name="us-west-2")

        # Create SQS queues
        queue_url = sqs.create_queue(QueueName="test-image-queue-metrics")["QueueUrl"]
        dlq_url = sqs.create_queue(QueueName="test-image-dlq-metrics")["QueueUrl"]

        # Create DynamoDB table
        table_name = "test-requested-jobs-metrics"
        dynamodb.create_table(
            TableName=table_name,
            KeySchema=[{"AttributeName": "endpoint_id", "KeyType": "HASH"}, {"AttributeName": "job_id", "KeyType": "RANGE"}],
            AttributeDefinitions=[
                {"AttributeName": "endpoint_id", "AttributeType": "S"},
                {"AttributeName": "job_id", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        jobs_table = RequestedJobsTable(table_name)

        yield sqs, dynamodb, queue_url, dlq_url, jobs_table, table_name

        # Cleanup
        dynamodb.Table(table_name).delete()
        sqs.delete_queue(QueueUrl=queue_url)
        sqs.delete_queue(QueueUrl=dlq_url)


def test_get_outstanding_requests_empty_queue(buffered_queue_setup):
    """Test getting outstanding requests when queue is empty"""
    queue, sqs, dynamodb, queue_url, dlq_url, jobs_table, table_name = buffered_queue_setup
    requests = queue.get_outstanding_requests()
    assert len(requests) == 0


def test_get_outstanding_requests_with_valid_messages(buffered_queue_setup):
    """Test getting outstanding requests with valid messages in queue"""
    queue, sqs, dynamodb, queue_url, dlq_url, jobs_table, table_name = buffered_queue_setup

    # Add messages to queue
    for i in range(3):
        message = create_sample_image_request_message_body(f"job-{i}")
        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))

    # Get outstanding requests
    requests = queue.get_outstanding_requests()
    assert len(requests) == 3
    assert requests[0].job_id == "job-0-id"


def test_handle_invalid_message(buffered_queue_setup):
    """Test handling of invalid messages"""
    queue, sqs, dynamodb, queue_url, dlq_url, jobs_table, table_name = buffered_queue_setup

    # Send invalid message to queue
    sqs.send_message(QueueUrl=queue_url, MessageBody="invalid-json")

    # Process messages
    requests = queue.get_outstanding_requests()

    # Verify invalid message was moved to DLQ
    dlq_messages = sqs.receive_message(QueueUrl=dlq_url, MaxNumberOfMessages=10).get("Messages", [])

    assert len(requests) == 0
    assert len(dlq_messages) == 1
    assert dlq_messages[0]["Body"] == "invalid-json"


def test_retry_failed_requests(buffered_queue_setup):
    """Test retry mechanism for failed requests"""
    queue, sqs, dynamodb, queue_url, dlq_url, jobs_table, table_name = buffered_queue_setup

    # Create a request and add it to the table
    request_data = create_sample_image_request_message_body()
    image_request = ImageRequest.from_external_message(request_data)
    status_record = jobs_table.add_new_request(image_request)

    # Force update of the item to look like it has already been run at sometime
    # in the past (longer than the retry timeout)
    jobs_table.table.update_item(
        Key={"endpoint_id": status_record.endpoint_id, "job_id": status_record.job_id},
        UpdateExpression="SET last_attempt = :time, num_attempts = num_attempts + :inc",
        ExpressionAttributeValues={":time": int(time.time()) - (queue.retry_time + 5), ":inc": 1},
        ReturnValues="UPDATED_NEW",
    )

    # Get outstanding requests, the request should be returned
    requests = queue.get_outstanding_requests()
    assert len(requests) == 1
    assert requests[0].num_attempts == 1


def test_purge_completed_requests(buffered_queue_setup):
    """Test purging of completed requests"""
    queue, sqs, dynamodb, queue_url, dlq_url, jobs_table, table_name = buffered_queue_setup

    # Create a completed request
    request_data = create_sample_image_request_message_body()
    image_request = ImageRequest.from_external_message(request_data)

    jobs_table.add_new_request(image_request)
    jobs_table.update_request_details(image_request, region_count=1)
    jobs_table.complete_region(image_request, "region1")

    # Get outstanding requests (should purge completed)
    requests = queue.get_outstanding_requests()
    assert len(requests) == 0


def test_max_retry_attempts_exceeded(buffered_queue_setup):
    """Test handling of requests that exceed max retry attempts"""
    queue, sqs, dynamodb, queue_url, dlq_url, jobs_table, table_name = buffered_queue_setup

    # Create a request and add it to the table
    request_data = create_sample_image_request_message_body()
    image_request = ImageRequest.from_external_message(request_data)
    status_record = jobs_table.add_new_request(image_request)

    # Force update of the item to look like it has already used up its retries
    # in the past (longer than the retry timeout)
    jobs_table.table.update_item(
        Key={"endpoint_id": status_record.endpoint_id, "job_id": status_record.job_id},
        UpdateExpression="SET last_attempt = :time, num_attempts = num_attempts + :inc",
        ExpressionAttributeValues={
            ":time": int(time.time()) - (queue.retry_time + 5),
            ":inc": queue.max_retry_attempts,
        },
        ReturnValues="UPDATED_NEW",
    )

    # Get outstanding requests and make sure the attempt is not among them
    requests = queue.get_outstanding_requests()
    assert len(requests) == 0

    # Verify message was moved to DLQ
    dlq_messages = sqs.receive_message(QueueUrl=dlq_url, MaxNumberOfMessages=10).get("Messages", [])

    assert len(dlq_messages) == 1


def test_respect_max_jobs_lookahead(buffered_queue_setup):
    """Test that the queue respects the max_jobs_lookahead limit"""
    queue, sqs, dynamodb, queue_url, dlq_url, jobs_table, table_name = buffered_queue_setup

    # Add more messages than max_jobs_lookahead
    for i in range(queue.max_jobs_lookahead + 5):
        message = create_sample_image_request_message_body(f"job-{i}")
        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))

    # Get outstanding requests
    requests = queue.get_outstanding_requests()
    assert len(requests) == queue.max_jobs_lookahead


def test_fetch_new_requests_with_region_calculator(buffered_queue_setup, mocker):
    """Test _fetch_new_requests() with region_calculator calculates and stores region_count"""
    queue, sqs, dynamodb, queue_url, dlq_url, jobs_table, table_name = buffered_queue_setup

    # Create mock region calculator
    mock_region_calculator = mocker.Mock(spec=RegionCalculator)
    mock_regions = [
        ((0, 0), (10240, 10240)),
        ((0, 10240), (10240, 10240)),
        ((10240, 0), (10240, 10240)),
        ((10240, 10240), (10240, 10240)),
    ]
    mock_region_calculator.calculate_regions.return_value = mock_regions

    # Create queue with region calculator
    queue_with_calculator = BufferedImageRequestQueue(
        image_queue_url=queue_url,
        image_dlq_url=dlq_url,
        requested_jobs_table=jobs_table,
        region_calculator=mock_region_calculator,
    )

    # Add message to queue
    message = create_sample_image_request_message_body()
    sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))

    # Get outstanding requests
    requests = queue_with_calculator.get_outstanding_requests()

    # Verify region_count was calculated and stored
    assert len(requests) == 1
    assert requests[0].region_count == 4
    mock_region_calculator.calculate_regions.assert_called_once()


def test_fetch_new_requests_moves_inaccessible_images_to_dlq(buffered_queue_setup, mocker):
    """Test _fetch_new_requests() moves inaccessible images to DLQ (fail-fast)"""
    queue, sqs, dynamodb, queue_url, dlq_url, jobs_table, table_name = buffered_queue_setup

    # Create mock region calculator that raises LoadImageException
    mock_region_calculator = mocker.Mock(spec=RegionCalculator)
    mock_region_calculator.calculate_regions.side_effect = LoadImageException("Image not accessible")

    # Create queue with region calculator
    queue_with_calculator = BufferedImageRequestQueue(
        image_queue_url=queue_url,
        image_dlq_url=dlq_url,
        requested_jobs_table=jobs_table,
        region_calculator=mock_region_calculator,
    )

    # Add message to queue
    message = create_sample_image_request_message_body()
    sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))

    # Get outstanding requests
    requests = queue_with_calculator.get_outstanding_requests()

    # Verify request was not added to outstanding requests
    assert len(requests) == 0

    # Verify message was moved to DLQ
    dlq_messages = sqs.receive_message(QueueUrl=dlq_url, MaxNumberOfMessages=10).get("Messages", [])
    assert len(dlq_messages) == 1


def test_fetch_new_requests_without_region_calculator(buffered_queue_setup):
    """Test _fetch_new_requests() without region_calculator stores region_count=None"""
    queue, sqs, dynamodb, queue_url, dlq_url, jobs_table, table_name = buffered_queue_setup

    # Add message to queue
    message = create_sample_image_request_message_body()
    sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))

    # Get outstanding requests (queue created without region_calculator in setUp)
    requests = queue.get_outstanding_requests()

    # Verify region_count is None
    assert len(requests) == 1
    assert requests[0].region_count is None


def test_fetch_new_requests_logs_warning_without_region_calculator(buffered_queue_setup, mocker):
    """Test _fetch_new_requests() logs warning when region_calculator not provided"""
    queue, sqs, dynamodb, queue_url, dlq_url, jobs_table, table_name = buffered_queue_setup

    mock_logger = mocker.patch("aws.osml.model_runner.scheduler.buffered_image_request_queue.logger")

    # Add message to queue
    message = create_sample_image_request_message_body()
    sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))

    # Get outstanding requests
    queue.get_outstanding_requests()

    # Verify warning was logged
    mock_logger.warning.assert_called()
    warning_call_args = str(mock_logger.warning.call_args)
    assert "Region calculator not provided" in warning_call_args


def test_load_image_exception_handling(buffered_queue_setup, mocker):
    """Test LoadImageException handling moves message to DLQ"""
    queue, sqs, dynamodb, queue_url, dlq_url, jobs_table, table_name = buffered_queue_setup

    mock_logger = mocker.patch("aws.osml.model_runner.scheduler.buffered_image_request_queue.logger")

    # Create mock region calculator that raises LoadImageException
    mock_region_calculator = mocker.Mock(spec=RegionCalculator)
    mock_region_calculator.calculate_regions.side_effect = LoadImageException("Cannot read image header")

    # Create queue with region calculator
    queue_with_calculator = BufferedImageRequestQueue(
        image_queue_url=queue_url,
        image_dlq_url=dlq_url,
        requested_jobs_table=jobs_table,
        region_calculator=mock_region_calculator,
    )

    # Add message to queue
    message = create_sample_image_request_message_body()
    sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))

    # Get outstanding requests
    requests = queue_with_calculator.get_outstanding_requests()

    # Verify error was logged
    mock_logger.error.assert_called()
    error_call_args = str(mock_logger.error.call_args)
    assert "inaccessible" in error_call_args

    # Verify request was not added
    assert len(requests) == 0

    # Verify message was moved to DLQ
    dlq_messages = sqs.receive_message(QueueUrl=dlq_url, MaxNumberOfMessages=10).get("Messages", [])
    assert len(dlq_messages) == 1


def test_fetch_new_requests_with_variant_selector(buffered_queue_setup, mocker):
    """Test _fetch_new_requests() with variant_selector selects variant early"""
    queue, sqs, dynamodb, queue_url, dlq_url, jobs_table, table_name = buffered_queue_setup

    # Create mock variant selector
    mock_variant_selector = mocker.Mock(spec=EndpointVariantSelector)

    def select_variant_side_effect(image_request):
        # Simulate variant selection by modifying the request
        if image_request.model_endpoint_parameters is None:
            image_request.model_endpoint_parameters = {}
        image_request.model_endpoint_parameters["TargetVariant"] = "variant-1"
        return image_request

    mock_variant_selector.select_variant.side_effect = select_variant_side_effect

    # Create queue with variant selector
    queue_with_selector = BufferedImageRequestQueue(
        image_queue_url=queue_url,
        image_dlq_url=dlq_url,
        requested_jobs_table=jobs_table,
        variant_selector=mock_variant_selector,
    )

    # Add message to queue
    message = create_sample_image_request_message_body()
    sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))

    # Get outstanding requests
    requests = queue_with_selector.get_outstanding_requests()

    # Verify variant was selected
    assert len(requests) == 1
    mock_variant_selector.select_variant.assert_called_once()
    assert requests[0].request_payload.model_endpoint_parameters.get("TargetVariant") == "variant-1"


def test_fetch_new_requests_without_variant_selector(buffered_queue_setup):
    """Test _fetch_new_requests() without variant_selector leaves TargetVariant unchanged"""
    queue, sqs, dynamodb, queue_url, dlq_url, jobs_table, table_name = buffered_queue_setup

    # Add message to queue with explicit TargetVariant
    message = create_sample_image_request_message_body()
    message["imageProcessorParameters"] = {"TargetVariant": "original-variant"}
    sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))

    # Get outstanding requests (queue created without variant_selector in setUp)
    requests = queue.get_outstanding_requests()

    # Verify TargetVariant is unchanged
    assert len(requests) == 1
    assert requests[0].request_payload.model_endpoint_parameters.get("TargetVariant") == "original-variant"


def test_fetch_new_requests_honors_explicit_target_variant(buffered_queue_setup, mocker):
    """Test _fetch_new_requests() honors explicit TargetVariant (never overrides)"""
    queue, sqs, dynamodb, queue_url, dlq_url, jobs_table, table_name = buffered_queue_setup

    # Create mock variant selector
    mock_variant_selector = mocker.Mock(spec=EndpointVariantSelector)

    def select_variant_side_effect(image_request):
        # Variant selector should honor existing TargetVariant
        if (
            image_request.model_endpoint_parameters
            and image_request.model_endpoint_parameters.get("TargetVariant") == "explicit-variant"
        ):
            # Don't override - return as-is
            return image_request
        # Otherwise select a variant
        if image_request.model_endpoint_parameters is None:
            image_request.model_endpoint_parameters = {}
        image_request.model_endpoint_parameters["TargetVariant"] = "selected-variant"
        return image_request

    mock_variant_selector.select_variant.side_effect = select_variant_side_effect

    # Create queue with variant selector
    queue_with_selector = BufferedImageRequestQueue(
        image_queue_url=queue_url,
        image_dlq_url=dlq_url,
        requested_jobs_table=jobs_table,
        variant_selector=mock_variant_selector,
    )

    # Add message with explicit TargetVariant
    message = create_sample_image_request_message_body()
    message["imageProcessorParameters"] = {"TargetVariant": "explicit-variant"}
    sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))

    # Get outstanding requests
    requests = queue_with_selector.get_outstanding_requests()

    # Verify explicit TargetVariant was honored
    assert len(requests) == 1
    assert requests[0].request_payload.model_endpoint_parameters.get("TargetVariant") == "explicit-variant"


def test_fetch_new_requests_works_for_http_endpoints(buffered_queue_setup, mocker):
    """Test _fetch_new_requests() works for HTTP endpoints (no variant selection)"""
    queue, sqs, dynamodb, queue_url, dlq_url, jobs_table, table_name = buffered_queue_setup

    # Create mock variant selector
    mock_variant_selector = mocker.Mock(spec=EndpointVariantSelector)
    mock_variant_selector.select_variant.side_effect = lambda req: req  # Return unchanged for HTTP

    # Create queue with variant selector
    queue_with_selector = BufferedImageRequestQueue(
        image_queue_url=queue_url,
        image_dlq_url=dlq_url,
        requested_jobs_table=jobs_table,
        variant_selector=mock_variant_selector,
    )

    # Add message with HTTP endpoint
    message = create_sample_image_request_message_body()
    message["imageProcessor"]["name"] = "http://example.com/model"
    sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))

    # Get outstanding requests
    requests = queue_with_selector.get_outstanding_requests()

    # Verify request was processed successfully
    assert len(requests) == 1
    mock_variant_selector.select_variant.assert_called_once()


def test_get_outstanding_requests_handles_exception_returns_empty(buffered_queue_setup, mocker):
    """Test get_outstanding_requests handles exception and returns empty list"""
    queue, sqs, dynamodb, queue_url, dlq_url, jobs_table, table_name = buffered_queue_setup

    mock_logger = mocker.patch("aws.osml.model_runner.scheduler.buffered_image_request_queue.logger")

    # Arrange - Mock get_outstanding_requests to raise Exception
    queue.requested_jobs_table.get_outstanding_requests = mocker.Mock(side_effect=Exception("DDB error"))

    # Act
    requests = queue.get_outstanding_requests()

    # Assert - returns empty list
    assert len(requests) == 0
    # Verify error logged
    mock_logger.error.assert_called_once()
    error_args = str(mock_logger.error.call_args)
    assert "Error getting outstanding requests" in error_args


def test_fetch_new_requests_invalid_request_skips_to_next(buffered_queue_setup, mocker):
    """Test fetch_new_requests skips invalid request and moves to DLQ"""
    queue, sqs, dynamodb, queue_url, dlq_url, jobs_table, table_name = buffered_queue_setup

    # Arrange - Add two valid-looking messages
    for i in range(2):
        message = create_sample_image_request_message_body(f"job-{i}")
        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))

    # Mock is_valid to return False for first request only
    mock_is_valid = mocker.patch.object(ImageRequest, "is_valid", return_value=False)
    mock_is_valid.side_effect = [False, True]

    # Act
    requests = queue.get_outstanding_requests()

    # Assert - only second request processed (first invalid)
    assert len(requests) == 1
    assert requests[0].job_id == "job-1-id"

    # Verify invalid message moved to DLQ
    dlq_messages = sqs.receive_message(QueueUrl=dlq_url, MaxNumberOfMessages=10).get("Messages", [])
    assert len(dlq_messages) == 1


def test_fetch_new_requests_client_error_on_ddb_add_logs_and_continues(buffered_queue_setup, mocker):
    """Test fetch_new_requests handles ClientError on DDB add, logs, and continues"""
    queue, sqs, dynamodb, queue_url, dlq_url, jobs_table, table_name = buffered_queue_setup

    mock_logger = mocker.patch("aws.osml.model_runner.scheduler.buffered_image_request_queue.logger")

    # Arrange - Add three valid messages
    for i in range(3):
        message = create_sample_image_request_message_body(f"job-{i}")
        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))

    # Mock add_new_request to fail on second call only
    original_add = queue.requested_jobs_table.add_new_request
    call_count = [0]

    def mock_add_with_failure(image_request, region_count=None):
        call_count[0] += 1
        if call_count[0] == 2:
            raise ClientError({"Error": {"Code": "ServiceUnavailable", "Message": "DDB error"}}, "PutItem")
        return original_add(image_request, region_count)

    queue.requested_jobs_table.add_new_request = mock_add_with_failure

    # Act
    requests = queue.get_outstanding_requests()

    # Assert - first and third requests processed successfully
    assert len(requests) == 2
    assert requests[0].job_id == "job-0-id"
    assert requests[1].job_id == "job-2-id"

    # Verify error logged for second request
    mock_logger.error.assert_called()
    error_args = str(mock_logger.error.call_args)
    assert "Unable to move valid image request" in error_args


def test_fetch_new_requests_client_error_on_sqs_receive_breaks_loop(buffered_queue_setup, mocker):
    """Test fetch_new_requests handles ClientError on SQS receive and breaks loop"""
    queue, sqs, dynamodb, queue_url, dlq_url, jobs_table, table_name = buffered_queue_setup

    mock_logger = mocker.patch("aws.osml.model_runner.scheduler.buffered_image_request_queue.logger")

    # Mock sqs_client.receive_message to raise ClientError
    queue.sqs_client.receive_message = mocker.Mock(
        side_effect=ClientError({"Error": {"Code": "ServiceUnavailable"}}, "ReceiveMessage")
    )

    # Act
    requests = queue.get_outstanding_requests()

    # Assert - returns empty (loop breaks on error)
    assert len(requests) == 0

    # Verify error logged
    mock_logger.error.assert_called()
    error_args = str(mock_logger.error.call_args)
    assert "Error receiving messages from SQS" in error_args


def test_handle_invalid_message_client_error_logs_exception(buffered_queue_setup, mocker):
    """Test _handle_invalid_message handles ClientError and logs exception"""
    queue, sqs, dynamodb, queue_url, dlq_url, jobs_table, table_name = buffered_queue_setup

    mock_logger = mocker.patch("aws.osml.model_runner.scheduler.buffered_image_request_queue.logger")

    # Arrange - Create invalid message
    sqs.send_message(QueueUrl=queue_url, MessageBody="invalid-json")

    # Mock send_message to DLQ to raise ClientError
    original_send = queue.sqs_client.send_message

    def mock_send_with_failure(*args, **kwargs):
        if kwargs.get("QueueUrl") == dlq_url:
            raise ClientError({"Error": {"Code": "ServiceUnavailable"}}, "SendMessage")
        return original_send(*args, **kwargs)

    queue.sqs_client.send_message = mock_send_with_failure

    # Act
    requests = queue.get_outstanding_requests()

    # Assert
    assert len(requests) == 0

    # Verify error logged
    mock_logger.error.assert_called()
    error_args = str(mock_logger.error.call_args)
    assert "Unable to move invalid image request" in error_args

    # Verify exception logged
    mock_logger.exception.assert_called()


def test_purge_finished_requests_client_error_logs_and_skips(buffered_queue_setup, mocker):
    """Test _purge_finished_requests handles ClientError and logs with job_id"""
    queue, sqs, dynamodb, queue_url, dlq_url, jobs_table, table_name = buffered_queue_setup

    mock_logger = mocker.patch("aws.osml.model_runner.scheduler.buffered_image_request_queue.logger")

    # Arrange - Create two requests: one normal, one that should be purged
    for i in range(2):
        request_data = create_sample_image_request_message_body(f"job-{i}")
        image_request = ImageRequest.from_external_message(request_data)
        status_record = jobs_table.add_new_request(image_request)

        if i == 0:
            # First request: exceed max retries (should be purged)
            jobs_table.table.update_item(
                Key={"endpoint_id": status_record.endpoint_id, "job_id": status_record.job_id},
                UpdateExpression="SET last_attempt = :time, num_attempts = num_attempts + :inc, region_count = :count",
                ExpressionAttributeValues={
                    ":time": int(time.time()) - (queue.retry_time + 5),
                    ":inc": queue.max_retry_attempts,
                    ":count": 1,
                },
                ReturnValues="UPDATED_NEW",
            )

    # Mock send_message to raise ClientError when sending to DLQ
    queue.sqs_client.send_message = mocker.Mock(
        side_effect=ClientError({"Error": {"Code": "ServiceUnavailable", "Message": "DLQ error"}}, "SendMessage")
    )

    # Act
    requests = queue.get_outstanding_requests()

    # Assert - only second request remains (first request purge failed but was removed)
    assert len(requests) == 1
    assert requests[0].job_id == "job-1-id"

    # Verify error logged with job_id
    mock_logger.error.assert_called()
    error_args = str(mock_logger.error.call_args)
    assert "Unable to cleanup outstanding request" in error_args
    assert "job-0-id" in error_args


def test_emit_buffered_queue_metrics_skips_if_interval_not_reached(buffered_queue_setup, mocker):
    """Test _emit_buffered_queue_metrics skips emission if interval not reached"""
    queue, sqs, dynamodb, queue_url, dlq_url, jobs_table, table_name = buffered_queue_setup

    # Arrange - Set last emission time to recent
    queue._last_metric_emission_time = time.time()

    # Mock _do_emit_buffered_queue_metrics to track calls
    queue._do_emit_buffered_queue_metrics = mocker.Mock()

    # Act - call _emit_buffered_queue_metrics
    queue._emit_buffered_queue_metrics(num_buffered_requests=10, num_visible_requests=5)

    # Assert - _do_emit_buffered_queue_metrics NOT called
    queue._do_emit_buffered_queue_metrics.assert_not_called()


def test_emit_image_access_error_metric_handles_exception(buffered_queue_setup, mocker):
    """Test _emit_image_access_error_metric handles exception gracefully"""
    queue, sqs, dynamodb, queue_url, dlq_url, jobs_table, table_name = buffered_queue_setup

    mock_logger = mocker.patch("aws.osml.model_runner.scheduler.buffered_image_request_queue.logger")

    # Arrange - Create mock metrics that raises exception
    mock_metrics = create_mock_metrics_logger()
    mock_metrics.put_metric.side_effect = Exception("Metrics emission failed")

    # Act - should not propagate exception
    queue._emit_image_access_error_metric.__wrapped__(queue, "test-endpoint", metrics=mock_metrics)

    # Assert - error logged, exception doesn't propagate
    mock_logger.error.assert_called()
    error_args = str(mock_logger.error.call_args)
    assert "Error emitting image access error metric" in error_args
    assert "test-endpoint" in error_args


def test_errors_metric_increments_on_load_image_exception(buffered_queue_metrics_setup, mocker):
    """Test Errors metric (Operation=Scheduling, ModelName=<endpoint>) increments on LoadImageException"""
    from aws_embedded_metrics.unit import Unit

    from aws.osml.model_runner.app_config import MetricLabels

    sqs, dynamodb, queue_url, dlq_url, jobs_table, table_name = buffered_queue_metrics_setup

    mock_region_calculator = mocker.Mock(spec=RegionCalculator)
    mock_region_calculator.calculate_regions.side_effect = LoadImageException("Image not accessible")

    queue = BufferedImageRequestQueue(
        image_queue_url=queue_url,
        image_dlq_url=dlq_url,
        requested_jobs_table=jobs_table,
        region_calculator=mock_region_calculator,
    )

    mock_metrics = create_mock_metrics_logger()

    queue._emit_image_access_error_metric.__wrapped__(queue, "test-model", metrics=mock_metrics)

    mock_metrics.put_dimensions.assert_called_once_with(
        {
            MetricLabels.OPERATION_DIMENSION: MetricLabels.SCHEDULING_OPERATION,
            MetricLabels.MODEL_NAME_DIMENSION: "test-model",
        }
    )
    mock_metrics.put_metric.assert_called_once_with(MetricLabels.ERRORS, 1, str(Unit.COUNT.value))


def test_errors_metric_follows_standard_pattern(buffered_queue_metrics_setup):
    """Test Errors metric follows standard ModelRunner pattern"""
    from aws_embedded_metrics.unit import Unit

    from aws.osml.model_runner.app_config import MetricLabels

    sqs, dynamodb, queue_url, dlq_url, jobs_table, table_name = buffered_queue_metrics_setup

    queue = BufferedImageRequestQueue(
        image_queue_url=queue_url,
        image_dlq_url=dlq_url,
        requested_jobs_table=jobs_table,
    )

    mock_metrics = create_mock_metrics_logger()

    queue._emit_image_access_error_metric.__wrapped__(queue, "test-endpoint", metrics=mock_metrics)

    mock_metrics.put_dimensions.assert_called_with(
        {
            MetricLabels.OPERATION_DIMENSION: MetricLabels.SCHEDULING_OPERATION,
            MetricLabels.MODEL_NAME_DIMENSION: "test-endpoint",
        }
    )
    mock_metrics.put_metric.assert_called_with(MetricLabels.ERRORS, 1, str(Unit.COUNT.value))


def test_errors_metric_not_emitted_when_metrics_logger_is_none(buffered_queue_metrics_setup):
    """Test Errors metric is not emitted when metrics logger is None"""
    sqs, dynamodb, queue_url, dlq_url, jobs_table, table_name = buffered_queue_metrics_setup

    queue = BufferedImageRequestQueue(
        image_queue_url=queue_url,
        image_dlq_url=dlq_url,
        requested_jobs_table=jobs_table,
    )

    # Should not raise exception when metrics is None
    queue._emit_image_access_error_metric.__wrapped__(queue, "test-endpoint", metrics=None)
