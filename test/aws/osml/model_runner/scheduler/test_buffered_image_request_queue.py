#  Copyright 2025-2026 Amazon.com, Inc. or its affiliates.

import json
import time
import unittest
from unittest.mock import Mock, patch

import boto3
from moto import mock_aws

from aws.osml.model_runner.api import ImageRequest
from aws.osml.model_runner.database.requested_jobs_table import RequestedJobsTable
from aws.osml.model_runner.exceptions import LoadImageException
from aws.osml.model_runner.scheduler import BufferedImageRequestQueue, EndpointVariantSelector
from aws.osml.model_runner.tile_worker import RegionCalculator


@mock_aws
class TestBufferedImageRequestQueue(unittest.TestCase):
    """Test cases for BufferedImageRequestQueue"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Set up mock AWS resources
        self.sqs = boto3.client("sqs")
        self.dynamodb = boto3.resource("dynamodb")
        self.cloudwatch = boto3.client("cloudwatch")

        # Create SQS queues
        self.queue_url = self.sqs.create_queue(QueueName="test-image-queue")["QueueUrl"]
        self.dlq_url = self.sqs.create_queue(QueueName="test-image-dlq")["QueueUrl"]

        # Create DynamoDB table
        self.table_name = "test-requested-jobs"
        self.dynamodb.create_table(
            TableName=self.table_name,
            KeySchema=[{"AttributeName": "endpoint_id", "KeyType": "HASH"}, {"AttributeName": "job_id", "KeyType": "RANGE"}],
            AttributeDefinitions=[
                {"AttributeName": "endpoint_id", "AttributeType": "S"},
                {"AttributeName": "job_id", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        # Create RequestedJobsTable instance
        self.jobs_table = RequestedJobsTable(self.table_name)

        # Create BufferedImageRequestQueue instance
        self.queue = BufferedImageRequestQueue(
            image_queue_url=self.queue_url,
            image_dlq_url=self.dlq_url,
            requested_jobs_table=self.jobs_table,
            max_jobs_lookahead=10,
            retry_time=60,
            max_retry_attempts=2,
        )

    def create_sample_image_request_message_body(self, job_name: str = "test-job") -> dict:
        """Helper method to create a sample image request message"""
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

    def test_get_outstanding_requests_empty_queue(self):
        """Test getting outstanding requests when queue is empty"""
        requests = self.queue.get_outstanding_requests()
        self.assertEqual(len(requests), 0)

    def test_get_outstanding_requests_with_valid_messages(self):
        """Test getting outstanding requests with valid messages in queue"""
        # Add messages to queue
        for i in range(3):
            message = self.create_sample_image_request_message_body(f"job-{i}")
            self.sqs.send_message(QueueUrl=self.queue_url, MessageBody=json.dumps(message))

        # Get outstanding requests
        requests = self.queue.get_outstanding_requests()
        self.assertEqual(len(requests), 3)
        self.assertEqual(requests[0].job_id, "job-0-id")

    def test_handle_invalid_message(self):
        """Test handling of invalid messages"""
        # Send invalid message to queue
        self.sqs.send_message(QueueUrl=self.queue_url, MessageBody="invalid-json")

        # Process messages
        requests = self.queue.get_outstanding_requests()

        # Verify invalid message was moved to DLQ
        dlq_messages = self.sqs.receive_message(QueueUrl=self.dlq_url, MaxNumberOfMessages=10).get("Messages", [])

        self.assertEqual(len(requests), 0)
        self.assertEqual(len(dlq_messages), 1)
        self.assertEqual(dlq_messages[0]["Body"], "invalid-json")

    def test_retry_failed_requests(self):
        """Test retry mechanism for failed requests"""
        # Create a request and add it to the table
        request_data = self.create_sample_image_request_message_body()
        image_request = ImageRequest.from_external_message(request_data)
        status_record = self.jobs_table.add_new_request(image_request)

        # Force update of the item to look like it has already been run at sometime
        # in the past (longer than the retry timeout)
        self.jobs_table.table.update_item(
            Key={"endpoint_id": status_record.endpoint_id, "job_id": status_record.job_id},
            UpdateExpression="SET last_attempt = :time, num_attempts = num_attempts + :inc",
            ExpressionAttributeValues={":time": int(time.time()) - (self.queue.retry_time + 5), ":inc": 1},
            ReturnValues="UPDATED_NEW",
        )

        # Get outstanding requests, the request should be returned
        requests = self.queue.get_outstanding_requests()
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0].num_attempts, 1)

    def test_purge_completed_requests(self):
        """Test purging of completed requests"""
        # Create a completed request
        request_data = self.create_sample_image_request_message_body()
        image_request = ImageRequest.from_external_message(request_data)

        self.jobs_table.add_new_request(image_request)
        self.jobs_table.update_request_details(image_request, region_count=1)
        self.jobs_table.complete_region(image_request, "region1")

        # Get outstanding requests (should purge completed)
        requests = self.queue.get_outstanding_requests()
        self.assertEqual(len(requests), 0)

    def test_max_retry_attempts_exceeded(self):
        """Test handling of requests that exceed max retry attempts"""
        # Create a request and add it to the table
        request_data = self.create_sample_image_request_message_body()
        image_request = ImageRequest.from_external_message(request_data)
        status_record = self.jobs_table.add_new_request(image_request)

        # Force update of the item to look like it has already used up its retries
        # in the past (longer than the retry timeout)
        self.jobs_table.table.update_item(
            Key={"endpoint_id": status_record.endpoint_id, "job_id": status_record.job_id},
            UpdateExpression="SET last_attempt = :time, num_attempts = num_attempts + :inc",
            ExpressionAttributeValues={
                ":time": int(time.time()) - (self.queue.retry_time + 5),
                ":inc": self.queue.max_retry_attempts,
            },
            ReturnValues="UPDATED_NEW",
        )

        # Get outstanding requests and make sure the attempt is not among them
        requests = self.queue.get_outstanding_requests()
        self.assertEqual(len(requests), 0)

        # Verify message was moved to DLQ
        dlq_messages = self.sqs.receive_message(QueueUrl=self.dlq_url, MaxNumberOfMessages=10).get("Messages", [])

        self.assertEqual(len(dlq_messages), 1)

    def test_respect_max_jobs_lookahead(self):
        """Test that the queue respects the max_jobs_lookahead limit"""
        # Add more messages than max_jobs_lookahead
        for i in range(self.queue.max_jobs_lookahead + 5):
            message = self.create_sample_image_request_message_body(f"job-{i}")
            self.sqs.send_message(QueueUrl=self.queue_url, MessageBody=json.dumps(message))

        # Get outstanding requests
        requests = self.queue.get_outstanding_requests()
        self.assertEqual(len(requests), self.queue.max_jobs_lookahead)

    def test_fetch_new_requests_with_region_calculator(self):
        """Test _fetch_new_requests() with region_calculator calculates and stores region_count"""
        # Create mock region calculator
        mock_region_calculator = Mock(spec=RegionCalculator)
        mock_regions = [
            ((0, 0), (10240, 10240)),
            ((0, 10240), (10240, 10240)),
            ((10240, 0), (10240, 10240)),
            ((10240, 10240), (10240, 10240)),
        ]
        mock_region_calculator.calculate_regions.return_value = mock_regions

        # Create queue with region calculator
        queue_with_calculator = BufferedImageRequestQueue(
            image_queue_url=self.queue_url,
            image_dlq_url=self.dlq_url,
            requested_jobs_table=self.jobs_table,
            region_calculator=mock_region_calculator,
        )

        # Add message to queue
        message = self.create_sample_image_request_message_body()
        self.sqs.send_message(QueueUrl=self.queue_url, MessageBody=json.dumps(message))

        # Get outstanding requests
        requests = queue_with_calculator.get_outstanding_requests()

        # Verify region_count was calculated and stored
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0].region_count, 4)
        mock_region_calculator.calculate_regions.assert_called_once()

    def test_fetch_new_requests_moves_inaccessible_images_to_dlq(self):
        """Test _fetch_new_requests() moves inaccessible images to DLQ (fail-fast)"""
        # Create mock region calculator that raises LoadImageException
        mock_region_calculator = Mock(spec=RegionCalculator)
        mock_region_calculator.calculate_regions.side_effect = LoadImageException("Image not accessible")

        # Create queue with region calculator
        queue_with_calculator = BufferedImageRequestQueue(
            image_queue_url=self.queue_url,
            image_dlq_url=self.dlq_url,
            requested_jobs_table=self.jobs_table,
            region_calculator=mock_region_calculator,
        )

        # Add message to queue
        message = self.create_sample_image_request_message_body()
        self.sqs.send_message(QueueUrl=self.queue_url, MessageBody=json.dumps(message))

        # Get outstanding requests
        requests = queue_with_calculator.get_outstanding_requests()

        # Verify request was not added to outstanding requests
        self.assertEqual(len(requests), 0)

        # Verify message was moved to DLQ
        dlq_messages = self.sqs.receive_message(QueueUrl=self.dlq_url, MaxNumberOfMessages=10).get("Messages", [])
        self.assertEqual(len(dlq_messages), 1)

    def test_fetch_new_requests_without_region_calculator(self):
        """Test _fetch_new_requests() without region_calculator stores region_count=None"""
        # Add message to queue
        message = self.create_sample_image_request_message_body()
        self.sqs.send_message(QueueUrl=self.queue_url, MessageBody=json.dumps(message))

        # Get outstanding requests (queue created without region_calculator in setUp)
        requests = self.queue.get_outstanding_requests()

        # Verify region_count is None
        self.assertEqual(len(requests), 1)
        self.assertIsNone(requests[0].region_count)

    @patch("aws.osml.model_runner.scheduler.buffered_image_request_queue.logger")
    def test_fetch_new_requests_logs_warning_without_region_calculator(self, mock_logger):
        """Test _fetch_new_requests() logs warning when region_calculator not provided"""
        # Add message to queue
        message = self.create_sample_image_request_message_body()
        self.sqs.send_message(QueueUrl=self.queue_url, MessageBody=json.dumps(message))

        # Get outstanding requests
        self.queue.get_outstanding_requests()

        # Verify warning was logged
        mock_logger.warning.assert_called()
        warning_call_args = str(mock_logger.warning.call_args)
        self.assertIn("Region calculator not provided", warning_call_args)

    @patch("aws.osml.model_runner.scheduler.buffered_image_request_queue.logger")
    def test_load_image_exception_handling(self, mock_logger):
        """Test LoadImageException handling moves message to DLQ"""
        # Create mock region calculator that raises LoadImageException
        mock_region_calculator = Mock(spec=RegionCalculator)
        mock_region_calculator.calculate_regions.side_effect = LoadImageException("Cannot read image header")

        # Create queue with region calculator
        queue_with_calculator = BufferedImageRequestQueue(
            image_queue_url=self.queue_url,
            image_dlq_url=self.dlq_url,
            requested_jobs_table=self.jobs_table,
            region_calculator=mock_region_calculator,
        )

        # Add message to queue
        message = self.create_sample_image_request_message_body()
        self.sqs.send_message(QueueUrl=self.queue_url, MessageBody=json.dumps(message))

        # Get outstanding requests
        requests = queue_with_calculator.get_outstanding_requests()

        # Verify error was logged
        mock_logger.error.assert_called()
        error_call_args = str(mock_logger.error.call_args)
        self.assertIn("inaccessible", error_call_args)

        # Verify request was not added
        self.assertEqual(len(requests), 0)

        # Verify message was moved to DLQ
        dlq_messages = self.sqs.receive_message(QueueUrl=self.dlq_url, MaxNumberOfMessages=10).get("Messages", [])
        self.assertEqual(len(dlq_messages), 1)

    def test_fetch_new_requests_with_variant_selector(self):
        """Test _fetch_new_requests() with variant_selector selects variant early"""
        # Create mock variant selector
        mock_variant_selector = Mock(spec=EndpointVariantSelector)

        def select_variant_side_effect(image_request):
            # Simulate variant selection by modifying the request
            if image_request.model_endpoint_parameters is None:
                image_request.model_endpoint_parameters = {}
            image_request.model_endpoint_parameters["TargetVariant"] = "variant-1"
            return image_request

        mock_variant_selector.select_variant.side_effect = select_variant_side_effect

        # Create queue with variant selector
        queue_with_selector = BufferedImageRequestQueue(
            image_queue_url=self.queue_url,
            image_dlq_url=self.dlq_url,
            requested_jobs_table=self.jobs_table,
            variant_selector=mock_variant_selector,
        )

        # Add message to queue
        message = self.create_sample_image_request_message_body()
        self.sqs.send_message(QueueUrl=self.queue_url, MessageBody=json.dumps(message))

        # Get outstanding requests
        requests = queue_with_selector.get_outstanding_requests()

        # Verify variant was selected
        self.assertEqual(len(requests), 1)
        mock_variant_selector.select_variant.assert_called_once()
        self.assertEqual(requests[0].request_payload.model_endpoint_parameters.get("TargetVariant"), "variant-1")

    def test_fetch_new_requests_without_variant_selector(self):
        """Test _fetch_new_requests() without variant_selector leaves TargetVariant unchanged"""
        # Add message to queue with explicit TargetVariant
        message = self.create_sample_image_request_message_body()
        message["imageProcessorParameters"] = {"TargetVariant": "original-variant"}
        self.sqs.send_message(QueueUrl=self.queue_url, MessageBody=json.dumps(message))

        # Get outstanding requests (queue created without variant_selector in setUp)
        requests = self.queue.get_outstanding_requests()

        # Verify TargetVariant is unchanged
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0].request_payload.model_endpoint_parameters.get("TargetVariant"), "original-variant")

    def test_fetch_new_requests_honors_explicit_target_variant(self):
        """Test _fetch_new_requests() honors explicit TargetVariant (never overrides)"""
        # Create mock variant selector
        mock_variant_selector = Mock(spec=EndpointVariantSelector)

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
            image_queue_url=self.queue_url,
            image_dlq_url=self.dlq_url,
            requested_jobs_table=self.jobs_table,
            variant_selector=mock_variant_selector,
        )

        # Add message with explicit TargetVariant
        message = self.create_sample_image_request_message_body()
        message["imageProcessorParameters"] = {"TargetVariant": "explicit-variant"}
        self.sqs.send_message(QueueUrl=self.queue_url, MessageBody=json.dumps(message))

        # Get outstanding requests
        requests = queue_with_selector.get_outstanding_requests()

        # Verify explicit TargetVariant was honored
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0].request_payload.model_endpoint_parameters.get("TargetVariant"), "explicit-variant")

    def test_fetch_new_requests_works_for_http_endpoints(self):
        """Test _fetch_new_requests() works for HTTP endpoints (no variant selection)"""
        # Create mock variant selector
        mock_variant_selector = Mock(spec=EndpointVariantSelector)
        mock_variant_selector.select_variant.side_effect = lambda req: req  # Return unchanged for HTTP

        # Create queue with variant selector
        queue_with_selector = BufferedImageRequestQueue(
            image_queue_url=self.queue_url,
            image_dlq_url=self.dlq_url,
            requested_jobs_table=self.jobs_table,
            variant_selector=mock_variant_selector,
        )

        # Add message with HTTP endpoint
        message = self.create_sample_image_request_message_body()
        message["imageProcessor"]["name"] = "http://example.com/model"
        self.sqs.send_message(QueueUrl=self.queue_url, MessageBody=json.dumps(message))

        # Get outstanding requests
        requests = queue_with_selector.get_outstanding_requests()

        # Verify request was processed successfully
        self.assertEqual(len(requests), 1)
        mock_variant_selector.select_variant.assert_called_once()

    @patch("aws.osml.model_runner.scheduler.buffered_image_request_queue.logger")
    def test_get_outstanding_requests_handles_exception_returns_empty(self, mock_logger):
        """Test get_outstanding_requests handles exception and returns empty list"""
        # Arrange - Mock get_outstanding_requests to raise Exception
        self.queue.requested_jobs_table.get_outstanding_requests = Mock(side_effect=Exception("DDB error"))

        # Act
        requests = self.queue.get_outstanding_requests()

        # Assert - returns empty list
        self.assertEqual(len(requests), 0)
        # Verify error logged
        mock_logger.error.assert_called_once()
        error_args = str(mock_logger.error.call_args)
        self.assertIn("Error getting outstanding requests", error_args)

    @patch.object(ImageRequest, "is_valid", return_value=False)
    def test_fetch_new_requests_invalid_request_skips_to_next(self, mock_is_valid):
        """Test fetch_new_requests skips invalid request and moves to DLQ"""
        # Arrange - Add two valid-looking messages
        for i in range(2):
            message = self.create_sample_image_request_message_body(f"job-{i}")
            self.sqs.send_message(QueueUrl=self.queue_url, MessageBody=json.dumps(message))

        # Mock is_valid to return False for first request only
        mock_is_valid.side_effect = [False, True]

        # Act
        requests = self.queue.get_outstanding_requests()

        # Assert - only second request processed (first invalid)
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0].job_id, "job-1-id")

        # Verify invalid message moved to DLQ
        dlq_messages = self.sqs.receive_message(QueueUrl=self.dlq_url, MaxNumberOfMessages=10).get("Messages", [])
        self.assertEqual(len(dlq_messages), 1)

    @patch("aws.osml.model_runner.scheduler.buffered_image_request_queue.logger")
    def test_fetch_new_requests_client_error_on_ddb_add_logs_and_continues(self, mock_logger):
        """Test fetch_new_requests handles ClientError on DDB add, logs, and continues"""
        from botocore.exceptions import ClientError

        # Arrange - Add three valid messages
        for i in range(3):
            message = self.create_sample_image_request_message_body(f"job-{i}")
            self.sqs.send_message(QueueUrl=self.queue_url, MessageBody=json.dumps(message))

        # Mock add_new_request to fail on second call only
        original_add = self.queue.requested_jobs_table.add_new_request
        call_count = [0]

        def mock_add_with_failure(image_request, region_count=None):
            call_count[0] += 1
            if call_count[0] == 2:
                raise ClientError({"Error": {"Code": "ServiceUnavailable", "Message": "DDB error"}}, "PutItem")
            return original_add(image_request, region_count)

        self.queue.requested_jobs_table.add_new_request = mock_add_with_failure

        # Act
        requests = self.queue.get_outstanding_requests()

        # Assert - first and third requests processed successfully
        self.assertEqual(len(requests), 2)
        self.assertEqual(requests[0].job_id, "job-0-id")
        self.assertEqual(requests[1].job_id, "job-2-id")

        # Verify error logged for second request
        mock_logger.error.assert_called()
        error_args = str(mock_logger.error.call_args)
        self.assertIn("Unable to move valid image request", error_args)

    @patch("aws.osml.model_runner.scheduler.buffered_image_request_queue.logger")
    def test_fetch_new_requests_client_error_on_sqs_receive_breaks_loop(self, mock_logger):
        """Test fetch_new_requests handles ClientError on SQS receive and breaks loop"""
        from botocore.exceptions import ClientError

        # Mock sqs_client.receive_message to raise ClientError
        self.queue.sqs_client.receive_message = Mock(
            side_effect=ClientError({"Error": {"Code": "ServiceUnavailable"}}, "ReceiveMessage")
        )

        # Act
        requests = self.queue.get_outstanding_requests()

        # Assert - returns empty (loop breaks on error)
        self.assertEqual(len(requests), 0)

        # Verify error logged
        mock_logger.error.assert_called()
        error_args = str(mock_logger.error.call_args)
        self.assertIn("Error receiving messages from SQS", error_args)

    @patch("aws.osml.model_runner.scheduler.buffered_image_request_queue.logger")
    def test_handle_invalid_message_client_error_logs_exception(self, mock_logger):
        """Test _handle_invalid_message handles ClientError and logs exception"""
        from botocore.exceptions import ClientError

        # Arrange - Create invalid message
        self.sqs.send_message(QueueUrl=self.queue_url, MessageBody="invalid-json")

        # Mock send_message to DLQ to raise ClientError
        original_send = self.queue.sqs_client.send_message

        def mock_send_with_failure(*args, **kwargs):
            if kwargs.get("QueueUrl") == self.dlq_url:
                raise ClientError({"Error": {"Code": "ServiceUnavailable"}}, "SendMessage")
            return original_send(*args, **kwargs)

        self.queue.sqs_client.send_message = mock_send_with_failure

        # Act
        requests = self.queue.get_outstanding_requests()

        # Assert
        self.assertEqual(len(requests), 0)

        # Verify error logged
        mock_logger.error.assert_called()
        error_args = str(mock_logger.error.call_args)
        self.assertIn("Unable to move invalid image request", error_args)

        # Verify exception logged
        mock_logger.exception.assert_called()

    @patch("aws.osml.model_runner.scheduler.buffered_image_request_queue.logger")
    def test_purge_finished_requests_client_error_logs_and_skips(self, mock_logger):
        """Test _purge_finished_requests handles ClientError and logs with job_id"""
        from botocore.exceptions import ClientError

        # Arrange - Create two requests: one normal, one that should be purged
        for i in range(2):
            request_data = self.create_sample_image_request_message_body(f"job-{i}")
            image_request = ImageRequest.from_external_message(request_data)
            status_record = self.jobs_table.add_new_request(image_request)

            if i == 0:
                # First request: exceed max retries (should be purged)
                self.jobs_table.table.update_item(
                    Key={"endpoint_id": status_record.endpoint_id, "job_id": status_record.job_id},
                    UpdateExpression="SET last_attempt = :time, num_attempts = num_attempts + :inc, region_count = :count",
                    ExpressionAttributeValues={
                        ":time": int(time.time()) - (self.queue.retry_time + 5),
                        ":inc": self.queue.max_retry_attempts,
                        ":count": 1,
                    },
                    ReturnValues="UPDATED_NEW",
                )

        # Mock send_message to raise ClientError when sending to DLQ
        self.queue.sqs_client.send_message = Mock(
            side_effect=ClientError({"Error": {"Code": "ServiceUnavailable", "Message": "DLQ error"}}, "SendMessage")
        )

        # Act
        requests = self.queue.get_outstanding_requests()

        # Assert - only second request remains (first request purge failed but was removed)
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0].job_id, "job-1-id")

        # Verify error logged with job_id
        mock_logger.error.assert_called()
        error_args = str(mock_logger.error.call_args)
        self.assertIn("Unable to cleanup outstanding request", error_args)
        self.assertIn("job-0-id", error_args)

    def test_emit_buffered_queue_metrics_skips_if_interval_not_reached(self):
        """Test _emit_buffered_queue_metrics skips emission if interval not reached"""
        # Arrange - Set last emission time to recent
        self.queue._last_metric_emission_time = time.time()

        # Mock _do_emit_buffered_queue_metrics to track calls
        self.queue._do_emit_buffered_queue_metrics = Mock()

        # Act - call _emit_buffered_queue_metrics
        self.queue._emit_buffered_queue_metrics(num_buffered_requests=10, num_visible_requests=5)

        # Assert - _do_emit_buffered_queue_metrics NOT called
        self.queue._do_emit_buffered_queue_metrics.assert_not_called()

    @patch("aws.osml.model_runner.scheduler.buffered_image_request_queue.logger")
    def test_emit_image_access_error_metric_handles_exception(self, mock_logger):
        """Test _emit_image_access_error_metric handles exception gracefully"""
        # Arrange - Create mock metrics that raises exception
        mock_metrics = self.create_mock_metrics_logger()
        mock_metrics.put_metric.side_effect = Exception("Metrics emission failed")

        # Act - should not propagate exception
        self.queue._emit_image_access_error_metric.__wrapped__(self.queue, "test-endpoint", metrics=mock_metrics)

        # Assert - error logged, exception doesn't propagate
        mock_logger.error.assert_called()
        error_args = str(mock_logger.error.call_args)
        self.assertIn("Error emitting image access error metric", error_args)
        self.assertIn("test-endpoint", error_args)

    def create_mock_metrics_logger(self):
        """Create a mock MetricsLogger that passes isinstance checks"""
        from unittest.mock import MagicMock, Mock

        from aws_embedded_metrics.logger.metrics_logger import MetricsLogger

        mock_metrics = MagicMock(spec=MetricsLogger)
        mock_metrics.put_dimensions = Mock()
        mock_metrics.put_metric = Mock()
        mock_metrics.reset_dimensions = Mock()
        return mock_metrics

    def tearDown(self):
        """Clean up test fixtures after each test method."""
        # Clean up DynamoDB table
        self.dynamodb.Table(self.table_name).delete()

        # Clean up SQS queues
        self.sqs.delete_queue(QueueUrl=self.queue_url)
        self.sqs.delete_queue(QueueUrl=self.dlq_url)


if __name__ == "__main__":
    unittest.main()


@mock_aws
class TestBufferedImageRequestQueueMetricsEmission(unittest.TestCase):
    """Test cases for metrics emission in BufferedImageRequestQueue"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.sqs = boto3.client("sqs", region_name="us-west-2")
        self.dynamodb = boto3.resource("dynamodb", region_name="us-west-2")

        # Create SQS queues
        self.queue_url = self.sqs.create_queue(QueueName="test-image-queue-metrics")["QueueUrl"]
        self.dlq_url = self.sqs.create_queue(QueueName="test-image-dlq-metrics")["QueueUrl"]

        # Create DynamoDB table
        self.table_name = "test-requested-jobs-metrics"
        self.dynamodb.create_table(
            TableName=self.table_name,
            KeySchema=[{"AttributeName": "endpoint_id", "KeyType": "HASH"}, {"AttributeName": "job_id", "KeyType": "RANGE"}],
            AttributeDefinitions=[
                {"AttributeName": "endpoint_id", "AttributeType": "S"},
                {"AttributeName": "job_id", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        self.jobs_table = RequestedJobsTable(self.table_name)

    def create_mock_metrics_logger(self):
        """Create a mock MetricsLogger that passes isinstance checks"""
        from unittest.mock import MagicMock, Mock

        from aws_embedded_metrics.logger.metrics_logger import MetricsLogger

        mock_metrics = MagicMock(spec=MetricsLogger)
        mock_metrics.put_dimensions = Mock()
        mock_metrics.put_metric = Mock()
        return mock_metrics

    def test_errors_metric_increments_on_load_image_exception(self):
        """Test Errors metric (Operation=Scheduling, ModelName=<endpoint>) increments on LoadImageException"""
        from aws_embedded_metrics.unit import Unit

        from aws.osml.model_runner.app_config import MetricLabels

        mock_region_calculator = Mock(spec=RegionCalculator)
        mock_region_calculator.calculate_regions.side_effect = LoadImageException("Image not accessible")

        queue = BufferedImageRequestQueue(
            image_queue_url=self.queue_url,
            image_dlq_url=self.dlq_url,
            requested_jobs_table=self.jobs_table,
            region_calculator=mock_region_calculator,
        )

        mock_metrics = self.create_mock_metrics_logger()

        queue._emit_image_access_error_metric.__wrapped__(queue, "test-model", metrics=mock_metrics)

        mock_metrics.put_dimensions.assert_called_once_with(
            {
                MetricLabels.OPERATION_DIMENSION: MetricLabels.SCHEDULING_OPERATION,
                MetricLabels.MODEL_NAME_DIMENSION: "test-model",
            }
        )
        mock_metrics.put_metric.assert_called_once_with(MetricLabels.ERRORS, 1, str(Unit.COUNT.value))

    def test_errors_metric_follows_standard_pattern(self):
        """Test Errors metric follows standard ModelRunner pattern"""
        from aws_embedded_metrics.unit import Unit

        from aws.osml.model_runner.app_config import MetricLabels

        queue = BufferedImageRequestQueue(
            image_queue_url=self.queue_url,
            image_dlq_url=self.dlq_url,
            requested_jobs_table=self.jobs_table,
        )

        mock_metrics = self.create_mock_metrics_logger()

        queue._emit_image_access_error_metric.__wrapped__(queue, "test-endpoint", metrics=mock_metrics)

        mock_metrics.put_dimensions.assert_called_with(
            {
                MetricLabels.OPERATION_DIMENSION: MetricLabels.SCHEDULING_OPERATION,
                MetricLabels.MODEL_NAME_DIMENSION: "test-endpoint",
            }
        )
        mock_metrics.put_metric.assert_called_with(MetricLabels.ERRORS, 1, str(Unit.COUNT.value))

    def test_errors_metric_not_emitted_when_metrics_logger_is_none(self):
        """Test Errors metric is not emitted when metrics logger is None"""
        queue = BufferedImageRequestQueue(
            image_queue_url=self.queue_url,
            image_dlq_url=self.dlq_url,
            requested_jobs_table=self.jobs_table,
        )

        # Should not raise exception when metrics is None
        queue._emit_image_access_error_metric.__wrapped__(queue, "test-endpoint", metrics=None)

    def tearDown(self):
        """Clean up test fixtures after each test method."""
        self.dynamodb.Table(self.table_name).delete()
        self.sqs.delete_queue(QueueUrl=self.queue_url)
        self.sqs.delete_queue(QueueUrl=self.dlq_url)
