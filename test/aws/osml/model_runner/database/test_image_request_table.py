#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

import os
import time
from decimal import Decimal

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from aws.osml.model_runner.app_config import BotoConfig
from aws.osml.model_runner.database.image_request_table import ImageRequestItem, ImageRequestTable

TEST_IMAGE_ID = "test-image-id"


@pytest.fixture
def image_request_table_setup():
    """Create DynamoDB table and ImageRequestTable instance"""
    with mock_aws():
        # Create virtual DDB table for testing
        ddb = boto3.resource("dynamodb", config=BotoConfig.default)
        table = ddb.create_table(
            TableName=os.environ["IMAGE_REQUEST_TABLE"],
            KeySchema=[{"AttributeName": "image_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "image_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        image_request_table = ImageRequestTable(os.environ["IMAGE_REQUEST_TABLE"])
        image_request_item = ImageRequestItem(image_id=TEST_IMAGE_ID)

        yield image_request_table, image_request_item

        table.delete()


def test_image_started_success(image_request_table_setup):
    """
    Validate we can start an image, and it gets created in the table.
    """
    image_request_table, image_request_item = image_request_table_setup
    image_request_table.start_image_request(image_request_item)
    resulting_image_request_item = image_request_table.get_image_request(TEST_IMAGE_ID)
    assert resulting_image_request_item.image_id == TEST_IMAGE_ID


def test_region_complete_success_count(image_request_table_setup):
    """
    Validate that when we complete a region successfully, it updates the DDB item.
    """
    image_request_table, image_request_item = image_request_table_setup
    image_request_table.start_image_request(image_request_item)
    image_request_table.complete_region_request(TEST_IMAGE_ID, False)
    resulting_image_request_item = image_request_table.get_image_request(TEST_IMAGE_ID)
    assert resulting_image_request_item.region_success == Decimal(1)
    assert resulting_image_request_item.region_error == Decimal(0)


def test_region_complete_error_count(image_request_table_setup):
    """
    Validate that when we fail to complete a region, it updates the DDB item.
    """
    image_request_table, image_request_item = image_request_table_setup
    image_request_table.start_image_request(image_request_item)
    image_request_item.region_count = Decimal(0)
    image_request_item.region_success = Decimal(0)
    image_request_item.region_error = Decimal(0)
    image_request_table.update_ddb_item(image_request_item)
    image_request_table.complete_region_request(TEST_IMAGE_ID, True)
    resulting_image_request_item = image_request_table.get_image_request(TEST_IMAGE_ID)
    assert resulting_image_request_item.region_error == Decimal(1)
    assert resulting_image_request_item.region_success == Decimal(0)


def test_is_image_complete_success(image_request_table_setup):
    """
    Validate that we can successfully determine when an image has been completed.
    """
    image_request_table, image_request_item = image_request_table_setup
    image_request_table.start_image_request(image_request_item)
    image_request_item.region_count = Decimal(1)
    image_request_item.region_success = Decimal(1)
    image_request_item.region_error = Decimal(0)
    image_request_table.update_ddb_item(image_request_item)
    assert image_request_table.is_image_request_complete(image_request_item)


def test_region_ended_success(image_request_table_setup):
    """
    Validate that we can successfully end an image's processing by setting its end time.
    """
    image_request_table, image_request_item = image_request_table_setup
    image_request_table.start_image_request(image_request_item)
    image_request_table.end_image_request(TEST_IMAGE_ID)
    resulting_image_request_item = image_request_table.get_image_request(TEST_IMAGE_ID)
    assert resulting_image_request_item.end_time is not None


def test_start_image_failure(image_request_table_setup, mocker):
    """
    Validate that we throw the correct StartImageFailed exception.
    """
    from aws.osml.model_runner.database.exceptions import StartImageException

    image_request_table, image_request_item = image_request_table_setup
    mock_put_exception = mocker.Mock(side_effect=ClientError({"Error": {"Code": 500, "Message": "ClientError"}}, "put_item"))
    mocker.patch.object(image_request_table, "put_ddb_item", mock_put_exception)

    with pytest.raises(StartImageException):
        image_request_table.start_image_request(image_request_item)


def test_complete_region_failure(image_request_table_setup, mocker):
    """
    Validate that we throw the correct CompleteRegionException when region completion fails.
    """
    from aws.osml.model_runner.database.exceptions import CompleteRegionException

    image_request_table, image_request_item = image_request_table_setup
    image_request_table.start_image_request(image_request_item)
    mock_update_exception = mocker.Mock(
        side_effect=ClientError({"Error": {"Code": 500, "Message": "ClientError"}}, "update_item")
    )
    mocker.patch.object(image_request_table, "update_ddb_item", mock_update_exception)

    with pytest.raises(CompleteRegionException):
        image_request_table.complete_region_request(TEST_IMAGE_ID, False)


def test_end_image_failure(image_request_table_setup, mocker):
    """
    Validate that we throw the correct EndImageException when ending image processing fails.
    """
    from aws.osml.model_runner.database.exceptions import EndImageException

    image_request_table, image_request_item = image_request_table_setup
    image_request_table.start_image_request(image_request_item)
    mock_update_exception = mocker.Mock(
        side_effect=ClientError({"Error": {"Code": 500, "Message": "ClientError"}}, "update_item")
    )
    mocker.patch.object(image_request_table, "update_ddb_item", mock_update_exception)

    with pytest.raises(EndImageException):
        image_request_table.end_image_request(TEST_IMAGE_ID)


def test_get_image_request_failure(image_request_table_setup, mocker):
    """
    Validate that we throw the correct GetImageRequestItemException when an image request can't be found.
    """
    from aws.osml.model_runner.database.exceptions import GetImageRequestItemException

    image_request_table, image_request_item = image_request_table_setup
    mock_get_exception = mocker.Mock(side_effect=ClientError({"Error": {"Code": "ResourceNotFoundException"}}, "get_item"))
    mocker.patch.object(image_request_table, "get_ddb_item", mock_get_exception)

    with pytest.raises(GetImageRequestItemException):
        image_request_table.get_image_request("DOES-NOT-EXIST-IMAGE-ID")


def test_is_image_request_complete_failure(image_request_table_setup):
    """
    Validate that we throw the correct IsImageCompleteException when checking if an image is complete fails.
    """
    from aws.osml.model_runner.database.exceptions import IsImageCompleteException

    image_request_table, image_request_item = image_request_table_setup
    image_request_table.start_image_request(image_request_item)
    image_request_item.region_count = None
    image_request_item.region_success = None
    image_request_item.region_error = None

    with pytest.raises(IsImageCompleteException):
        image_request_table.is_image_request_complete(image_request_item)


def test_get_processing_duration():
    """
    Validate that `get_processing_duration` correctly calculates the processing time in seconds.
    """
    start_time = int(time.time() * 1000) - 5000
    duration = ImageRequestTable.get_processing_duration(start_time)
    # Processing duration should be at least 5 seconds
    assert duration >= 5
