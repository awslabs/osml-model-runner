#  Copyright 2025-2026 Amazon.com, Inc. or its affiliates.

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from aws.osml.model_runner.api import ImageRequest
from aws.osml.model_runner.database.requested_jobs_table import ImageRequestStatusRecord, RequestedJobsTable


@pytest.fixture
def requested_jobs_table_setup():
    """Set up test fixtures before each test method."""
    table_name = "test-requested-jobs"

    with mock_aws():
        # Create the mock DynamoDB table
        ddb = boto3.resource("dynamodb")
        ddb.create_table(
            TableName=table_name,
            KeySchema=[{"AttributeName": "endpoint_id", "KeyType": "HASH"}, {"AttributeName": "job_id", "KeyType": "RANGE"}],
            AttributeDefinitions=[
                {"AttributeName": "endpoint_id", "AttributeType": "S"},
                {"AttributeName": "job_id", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        table = RequestedJobsTable(table_name)
        yield table, table_name, ddb


def create_sample_image_request(job_name: str = "test-job") -> ImageRequest:
    """Helper method to create a sample ImageRequest"""
    return ImageRequest.from_external_message(
        {
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
    )


def test_add_new_request(requested_jobs_table_setup):
    """Test adding a new request to the table"""
    table, table_name, ddb = requested_jobs_table_setup
    image_request = create_sample_image_request()
    table.add_new_request(image_request)

    # Verify the item was added correctly
    response = ddb.Table(table_name).get_item(Key={"endpoint_id": image_request.model_name, "job_id": image_request.job_id})
    item = response["Item"]

    assert item["endpoint_id"] == image_request.model_name
    assert item["job_id"] == image_request.job_id
    assert item["num_attempts"] == 0
    assert item["regions_complete"] == []


def test_get_outstanding_requests(requested_jobs_table_setup):
    """Test retrieving outstanding requests from the table"""
    table, _, _ = requested_jobs_table_setup
    # Add multiple requests
    requests = [
        create_sample_image_request(job_name="test-job-1"),
        create_sample_image_request(job_name="test-job-2"),
    ]

    for request in requests:
        table.add_new_request(request)

    # Retrieve outstanding requests
    outstanding = table.get_outstanding_requests()

    assert len(outstanding) == 2
    assert isinstance(outstanding[0], ImageRequestStatusRecord)


def test_start_next_attempt(requested_jobs_table_setup):
    """Test starting the next attempt for a request"""
    table, table_name, ddb = requested_jobs_table_setup
    image_request = create_sample_image_request()
    table.add_new_request(image_request)

    # Get the record and try to start next attempt
    records = table.get_outstanding_requests()
    assert len(records) == 1

    # First attempt should succeed
    success = table.start_next_attempt(records[0])
    assert success

    # Verify the attempt was recorded
    response = ddb.Table(table_name).get_item(Key={"endpoint_id": image_request.model_name, "job_id": image_request.job_id})
    item = response["Item"]
    assert item["num_attempts"] == 1

    # Trying to start next attempt with old record should fail
    success = table.start_next_attempt(records[0])
    assert not success


def test_complete_request(requested_jobs_table_setup):
    """Test completing and removing a request"""
    table, table_name, ddb = requested_jobs_table_setup
    image_request = create_sample_image_request()
    table.add_new_request(image_request)

    # Verify the item exists
    response = ddb.Table(table_name).get_item(Key={"endpoint_id": image_request.model_name, "job_id": image_request.job_id})
    assert "Item" in response

    # Complete the request
    table.complete_request(image_request)

    # Verify the item was deleted
    response = ddb.Table(table_name).get_item(Key={"endpoint_id": image_request.model_name, "job_id": image_request.job_id})
    assert "Item" not in response


def test_get_outstanding_requests_pagination(requested_jobs_table_setup):
    """Test that get_outstanding_requests handles pagination correctly"""
    table, _, _ = requested_jobs_table_setup
    # Add enough items to trigger pagination (DynamoDB default limit is 1MB)
    for i in range(10):
        request = create_sample_image_request(job_name=f"test-job-{i}")
        table.add_new_request(request)

    # Retrieve all items
    outstanding = table.get_outstanding_requests()

    assert len(outstanding) == 10
    assert isinstance(outstanding[0], ImageRequestStatusRecord)
    assert len({r.job_id for r in outstanding}) == 10


def test_complete_region_multiple(requested_jobs_table_setup):
    """Test completing multiple different regions."""
    table, table_name, ddb = requested_jobs_table_setup
    # Arrange
    image_request = create_sample_image_request()
    table.add_new_request(image_request)

    # Act
    result1 = table.complete_region(image_request, "region1")
    result2 = table.complete_region(image_request, "region2")
    result3 = table.complete_region(image_request, "region1")  # duplicate

    # Assert
    assert result1
    assert result2
    assert not result3

    # Verify final state
    response = ddb.Table(table_name).get_item(Key={"endpoint_id": image_request.model_name, "job_id": image_request.job_id})
    completed_regions = response["Item"]["regions_complete"]
    assert len(completed_regions) == 2
    assert "region1" in completed_regions
    assert "region2" in completed_regions


def test_complete_region_nonexistent_record(requested_jobs_table_setup):
    """Test completing a region for a non-existent record."""
    table, _, _ = requested_jobs_table_setup
    # Arrange
    image_request = create_sample_image_request()

    # Act
    result = table.complete_region(image_request, "region1")

    # Assert - missing record is treated as a conditional failure and returns False
    assert not result


def test_update_request_details_success(requested_jobs_table_setup):
    """Test successfully updating region count for a request."""
    table, table_name, ddb = requested_jobs_table_setup
    # Arrange
    image_request = create_sample_image_request()
    table.add_new_request(image_request)
    region_count = 5

    # Act
    table.update_request_details(image_request, region_count)

    # Verify the region count was updated
    response = ddb.Table(table_name).get_item(Key={"endpoint_id": image_request.model_name, "job_id": image_request.job_id})
    assert response["Item"]["region_count"] == region_count


def test_update_request_details_nonexistent_record(requested_jobs_table_setup):
    """Test updating region count for a non-existent record."""
    table, _, _ = requested_jobs_table_setup
    # Arrange
    image_request = create_sample_image_request("missing-request-details-job")
    region_count = 5

    # Act/Assert
    with pytest.raises(ClientError):
        table.update_request_details(image_request, region_count)


def test_add_new_request_with_region_count(requested_jobs_table_setup):
    """Test adding a new request with region_count stores value correctly in DDB."""
    table, table_name, ddb = requested_jobs_table_setup
    # Arrange
    image_request = create_sample_image_request()
    region_count = 10

    # Act
    table.add_new_request(image_request, region_count=region_count)

    # Assert - Verify the item was added with region_count
    response = ddb.Table(table_name).get_item(Key={"endpoint_id": image_request.model_name, "job_id": image_request.job_id})
    item = response["Item"]

    assert item["endpoint_id"] == image_request.model_name
    assert item["job_id"] == image_request.job_id
    assert item["num_attempts"] == 0
    assert item["regions_complete"] == []
    assert item["region_count"] == region_count


def test_add_new_request_without_region_count(requested_jobs_table_setup):
    """Test adding a new request without region_count stores None in DDB."""
    table, table_name, ddb = requested_jobs_table_setup
    # Arrange
    image_request = create_sample_image_request()

    # Act
    table.add_new_request(image_request)

    # Assert - Verify the item was added without region_count (None)
    response = ddb.Table(table_name).get_item(Key={"endpoint_id": image_request.model_name, "job_id": image_request.job_id})
    item = response["Item"]

    assert item["endpoint_id"] == image_request.model_name
    assert item["job_id"] == image_request.job_id
    assert item["num_attempts"] == 0
    assert item["regions_complete"] == []
    # In DynamoDB, None values are not stored, so the key won't exist
    assert "region_count" not in item


def test_get_outstanding_requests_returns_region_count(requested_jobs_table_setup):
    """Test get_outstanding_requests() returns records with region_count field."""
    table, _, _ = requested_jobs_table_setup
    # Arrange - Add requests with and without region_count
    request_with_count = create_sample_image_request(job_name="test-job-with-count")
    request_without_count = create_sample_image_request(job_name="test-job-without-count")

    table.add_new_request(request_with_count, region_count=15)
    table.add_new_request(request_without_count)

    # Act
    outstanding = table.get_outstanding_requests()

    # Assert
    assert len(outstanding) == 2

    # Find the records by job_id
    record_with_count = next(r for r in outstanding if r.job_id == request_with_count.job_id)
    record_without_count = next(r for r in outstanding if r.job_id == request_without_count.job_id)

    assert record_with_count.region_count == 15
    assert record_without_count.region_count is None


def test_region_count_persists_across_start_next_attempt(requested_jobs_table_setup):
    """Test region_count persists across start_next_attempt() calls."""
    table, _, _ = requested_jobs_table_setup
    # Arrange
    image_request = create_sample_image_request()
    region_count = 20
    table.add_new_request(image_request, region_count=region_count)

    # Act - Get the record and start next attempt
    records = table.get_outstanding_requests()
    assert len(records) == 1
    initial_record = records[0]
    assert initial_record.region_count == region_count

    # Start next attempt
    success = table.start_next_attempt(initial_record)
    assert success

    # Assert - Verify region_count persists after attempt update
    updated_records = table.get_outstanding_requests()
    assert len(updated_records) == 1
    updated_record = updated_records[0]

    assert updated_record.region_count == region_count
    assert updated_record.num_attempts == 1


def test_add_new_request_client_error_logs_and_raises(requested_jobs_table_setup):
    """Test add_new_request handles ClientError by logging and raising exception."""
    table, _, _ = requested_jobs_table_setup
    # Arrange
    image_request = create_sample_image_request()

    # Mock put_item to raise ClientError
    def mock_put_item(*args, **kwargs):
        raise ClientError({"Error": {"Code": "ServiceUnavailable", "Message": "Service unavailable"}}, "PutItem")

    original_put_item = table.table.put_item
    table.table.put_item = mock_put_item

    # Act / Assert
    with pytest.raises(ClientError) as context:
        table.add_new_request(image_request)

    assert context.value.response["Error"]["Code"] == "ServiceUnavailable"

    # Restore original method
    table.table.put_item = original_put_item


def test_update_request_details_client_error_logs_and_raises(requested_jobs_table_setup):
    """Test update_request_details handles non-conditional ClientError by logging and raising."""
    table, _, _ = requested_jobs_table_setup
    # Arrange
    image_request = create_sample_image_request()
    table.add_new_request(image_request)
    region_count = 10

    # Mock update_item to raise non-conditional ClientError
    def mock_update_item(*args, **kwargs):
        raise ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "Throughput exceeded"}}, "UpdateItem"
        )

    original_update_item = table.table.update_item
    table.table.update_item = mock_update_item

    # Act / Assert
    with pytest.raises(ClientError) as context:
        table.update_request_details(image_request, region_count)

    assert context.value.response["Error"]["Code"] == "ProvisionedThroughputExceededException"

    # Restore original method
    table.table.update_item = original_update_item


def test_complete_region_handles_missing_regions_complete_attribute(requested_jobs_table_setup):
    """Test complete_region initializes regions_complete when missing on an existing item."""
    table, table_name, ddb = requested_jobs_table_setup
    image_request = create_sample_image_request()
    table.add_new_request(image_request)

    # Simulate an older record shape that did not include regions_complete.
    ddb.Table(table_name).update_item(
        Key={"endpoint_id": image_request.model_name, "job_id": image_request.job_id},
        UpdateExpression="REMOVE regions_complete",
    )

    result = table.complete_region(image_request, "region1")
    assert result

    response = ddb.Table(table_name).get_item(Key={"endpoint_id": image_request.model_name, "job_id": image_request.job_id})
    assert response["Item"]["regions_complete"] == ["region1"]


def test_get_outstanding_requests_client_error_logs_and_raises(requested_jobs_table_setup):
    """Test get_outstanding_requests handles ClientError by logging and raising exception."""
    table, _, _ = requested_jobs_table_setup

    # Arrange - Mock scan to raise ClientError
    def mock_scan(*args, **kwargs):
        raise ClientError({"Error": {"Code": "ResourceNotFoundException", "Message": "Table not found"}}, "Scan")

    original_scan = table.table.scan
    table.table.scan = mock_scan

    # Act / Assert
    with pytest.raises(ClientError) as context:
        table.get_outstanding_requests()

    assert context.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # Restore original method
    table.table.scan = original_scan


def test_complete_request_client_error_logs_and_raises(requested_jobs_table_setup):
    """Test complete_request handles ClientError by logging and raising exception."""
    table, _, _ = requested_jobs_table_setup
    # Arrange
    image_request = create_sample_image_request()
    table.add_new_request(image_request)

    # Mock delete_item to raise ClientError
    def mock_delete_item(*args, **kwargs):
        raise ClientError({"Error": {"Code": "InternalServerError", "Message": "Internal error"}}, "DeleteItem")

    original_delete_item = table.table.delete_item
    table.table.delete_item = mock_delete_item

    # Act / Assert
    with pytest.raises(ClientError) as context:
        table.complete_request(image_request)

    assert context.value.response["Error"]["Code"] == "InternalServerError"

    # Restore original method
    table.table.delete_item = original_delete_item


def test_get_outstanding_requests_pagination_with_many_pages(requested_jobs_table_setup):
    """Test get_outstanding_requests handles multi-page pagination correctly."""
    table, _, _ = requested_jobs_table_setup
    # Arrange - Create 35+ requests to force pagination
    for i in range(35):
        request = create_sample_image_request(job_name=f"test-pagination-job-{i}")
        table.add_new_request(request)

    # Act
    outstanding = table.get_outstanding_requests()

    # Assert - Verify all records returned across multiple pages
    assert len(outstanding) == 35
    assert isinstance(outstanding[0], ImageRequestStatusRecord)

    # Verify all unique job IDs are present
    job_ids = {r.job_id for r in outstanding}
    assert len(job_ids) == 35
