#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import unittest
from unittest.mock import Mock, patch

import boto3
from botocore.exceptions import ClientError
from decimal import Decimal
from moto import mock_aws

TEST_TABLE_NAME = "test-region-request-table"
TEST_IMAGE_ID = "test-image-id"
TEST_REGION_ID = "test-region-id"
TEST_JOB_ID = "test-job-id"
MOCK_PUT_EXCEPTION = Mock(side_effect=ClientError({"Error": {"Code": 500, "Message": "ClientError"}}, "put_item"))
MOCK_UPDATE_EXCEPTION = Mock(side_effect=ClientError({"Error": {"Code": 500, "Message": "ClientError"}}, "update_item"))


@mock_aws
class TestRegionRequestTable(unittest.TestCase):
    def setUp(self):
        """
        Set up virtual DDB resources/tables for each test to use.
        """
        from aws.osml.model_runner.app_config import BotoConfig
        from aws.osml.model_runner.database.region_request_table import RegionRequestItem, RegionRequestTable

        self.ddb = boto3.resource("dynamodb", config=BotoConfig.default)
        self.table = self.ddb.create_table(
            TableName=TEST_TABLE_NAME,
            KeySchema=[
                {"AttributeName": "region_id", "KeyType": "HASH"},
                {"AttributeName": "image_id", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "region_id", "AttributeType": "S"},
                {"AttributeName": "image_id", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        self.region_request_table = RegionRequestTable(TEST_TABLE_NAME)
        self.region_request_item = RegionRequestItem(TEST_REGION_ID, TEST_IMAGE_ID, TEST_JOB_ID)

    def tearDown(self):
        """
        Delete virtual DDB resources/tables after each test.
        """
        self.table.delete()
        self.ddb = None
        self.region_request_table = None
        self.region_request_item = None

    def test_region_started_success(self):
        """
        Validate that starting a region request successfully stores it in the table.
        """
        from aws.osml.model_runner.common import RequestStatus

        self.region_request_table.start_region_request(self.region_request_item)
        resulting_region_request_item = self.region_request_table.get_region_request(TEST_REGION_ID, TEST_IMAGE_ID)
        assert resulting_region_request_item.image_id == TEST_IMAGE_ID
        assert resulting_region_request_item.region_id == TEST_REGION_ID
        assert resulting_region_request_item.job_id == TEST_JOB_ID
        assert resulting_region_request_item.region_status == RequestStatus.STARTED

    def test_region_complete_success(self):
        """
        Validate that completing a region request updates the DDB item successfully.
        """
        from aws.osml.model_runner.common import RequestStatus

        self.region_request_table.start_region_request(self.region_request_item)
        self.region_request_table.complete_region_request(self.region_request_item, RequestStatus.SUCCESS)
        resulting_region_request_item = self.region_request_table.get_region_request(TEST_REGION_ID, TEST_IMAGE_ID)

        assert resulting_region_request_item.region_status == RequestStatus.SUCCESS
        assert resulting_region_request_item.last_updated_time is not None
        assert resulting_region_request_item.end_time is not None

    def test_region_updated_success(self):
        """
        Validate that updating an item in the region request table works as expected.
        """
        self.region_request_table.start_region_request(self.region_request_item)
        self.region_request_item.total_tiles = 1
        self.region_request_table.update_region_request(self.region_request_item)
        resulting_region_request_item = self.region_request_table.get_region_request(TEST_REGION_ID, TEST_IMAGE_ID)

        assert resulting_region_request_item.total_tiles == 1
        assert resulting_region_request_item.last_updated_time is not None

    def test_region_complete_failed(self):
        """
        Validate that marking a region request as failed updates the DDB item.
        """
        from aws.osml.model_runner.common import RequestStatus

        self.region_request_table.start_region_request(self.region_request_item)
        self.region_request_table.complete_region_request(self.region_request_item, RequestStatus.FAILED)
        resulting_region_request_item = self.region_request_table.get_region_request(TEST_REGION_ID, TEST_IMAGE_ID)

        assert resulting_region_request_item.region_status == RequestStatus.FAILED
        assert resulting_region_request_item.last_updated_time is not None
        assert resulting_region_request_item.end_time is not None

    def test_start_region_failure_exception(self):
        """
        Validate that a StartRegionException is raised when starting a region fails.
        """
        from aws.osml.model_runner.database.exceptions import StartRegionException

        self.region_request_table.table.put_item = MOCK_PUT_EXCEPTION
        with self.assertRaises(StartRegionException):
            self.region_request_table.start_region_request(self.region_request_item)

    def test_complete_region_failure_exception(self):
        """
        Validate that a CompleteRegionException is raised when completing a region fails.
        """
        from aws.osml.model_runner.database.exceptions import CompleteRegionException

        self.region_request_table.table.update_item = MOCK_UPDATE_EXCEPTION
        self.region_request_table.start_region_request(self.region_request_item)
        with self.assertRaises(CompleteRegionException):
            self.region_request_table.complete_region_request(self.region_request_item, "FAILED")

    def test_region_updated_failure_exception(self):
        """
        Validate that an UpdateRegionException is raised when updating a region request fails.
        """
        from aws.osml.model_runner.database.exceptions import UpdateRegionException

        self.region_request_table.table.update_item = MOCK_UPDATE_EXCEPTION
        self.region_request_table.start_region_request(self.region_request_item)
        with self.assertRaises(UpdateRegionException):
            self.region_request_table.update_region_request(self.region_request_item)

    def test_get_region_request_none(self):
        """
        Validate that getting a non-existent region request returns None.
        """
        self.region_request_table.table.update_item = MOCK_UPDATE_EXCEPTION
        self.region_request_table.start_region_request(self.region_request_item)

        resulting_region_request = self.region_request_table.get_region_request(
            "DOES-NOT-EXIST-REGION-ID", "DOES-NOT-EXIST-IMAGE-ID"
        )
        assert resulting_region_request is None

    def test_add_tile_success(self):
        """
        Validate that tiles can be added as succeeded to the region request item.
        """
        from aws.osml.model_runner.common import TileState

        self.region_request_table.start_region_request(self.region_request_item)
        tile = ((0, 0), (256, 256))
        self.region_request_table.add_tile(TEST_IMAGE_ID, TEST_REGION_ID, tile, TileState.SUCCEEDED)

        success_tile_item = self.region_request_table.get_region_request(TEST_REGION_ID, TEST_IMAGE_ID)
        assert len(success_tile_item.succeeded_tiles) == 1

    def test_add_tile_failed(self):
        """
        Validate that tiles can be added as failed to the region request item.
        """
        from aws.osml.model_runner.common import TileState

        self.region_request_table.start_region_request(self.region_request_item)
        tile = ((0, 0), (256, 256))
        self.region_request_table.add_tile(TEST_IMAGE_ID, TEST_REGION_ID, tile, TileState.FAILED)
        failed_tile_item = self.region_request_table.get_region_request(TEST_REGION_ID, TEST_IMAGE_ID)

        assert len(failed_tile_item.failed_tiles) == 1

    def test_add_tile_invalid_format(self):
        """
        Validate that adding a tile with an invalid format raises UpdateRegionException.
        """
        from aws.osml.model_runner.common import TileState
        from aws.osml.model_runner.database.exceptions import UpdateRegionException

        self.region_request_table.start_region_request(self.region_request_item)

        with self.assertRaises(UpdateRegionException):
            self.region_request_table.add_tile(TEST_IMAGE_ID, TEST_REGION_ID, "bad_format", TileState.SUCCEEDED)

    def test_from_region_request_with_partial_data(self):
        """
        Validate that from_region_request handles partial data in the RegionRequest.
        """
        from aws.osml.model_runner.api import RegionRequest
        from aws.osml.model_runner.database import RegionRequestItem

        region_request = RegionRequest(
            region_id=TEST_REGION_ID,
            image_id=TEST_IMAGE_ID,
            job_id=None,
            region_bounds=[[0, 0], [256, 256]],
            tile_size=[256, 256],
            tile_overlap=[0, 0],
            tile_format="tif",
            tile_compression="LZW",
        )

        region_request_item = RegionRequestItem.from_region_request(region_request)
        assert region_request_item.region_id == TEST_REGION_ID
        assert region_request_item.image_id == TEST_IMAGE_ID
        assert region_request_item.job_id is None
        assert region_request_item.tile_format == "tif"

    def test_is_image_complete_success(self):
        """
        Validate that we can successfully determine when an image has been completed.
        """
        from aws.osml.model_runner.database.image_request_table import ImageRequestItem

        image_request_item = ImageRequestItem(image_id=TEST_IMAGE_ID)
        image_request_item.region_count = Decimal(1)
        image_request_item.region_success = Decimal(1)
        image_request_item.region_error = Decimal(0)

        with patch("aws.osml.model_runner.database.region_request_table.RegionRequestTable.get_image_request_complete_counts") as mock_counts:
            mock_counts.return_value = (0, 1)  # (failed_count, completed_count)
            done, completed, failed = self.region_request_table.is_image_request_complete(image_request_item)
            assert done is True
            assert completed == 1
            assert failed == 0

    def test_is_image_request_complete_with_failures(self):
        """
        Validate that is_image_request_complete correctly handles failed regions.
        """
        from aws.osml.model_runner.database.image_request_table import ImageRequestItem

        image_request_item = ImageRequestItem(image_id=TEST_IMAGE_ID)

        image_request_item.region_count = Decimal(3)
        image_request_item.region_success = Decimal(2)
        image_request_item.region_error = Decimal(1)

        # Mock the get_image_request_complete_counts function to return mixed status
        with patch("aws.osml.model_runner.database.region_request_table.RegionRequestTable.get_image_request_complete_counts") as mock_counts:
            mock_counts.return_value = (1, 2)  # (failed_count, completed_count)
            done, completed, failed = self.region_request_table.is_image_request_complete(image_request_item)
            assert done is True
            assert completed == 2
            assert failed == 1

    def test_get_or_create_region_request_item_existing(self):
        """Test get_or_create_region_request_item with existing item"""
        from aws.osml.model_runner.api import RegionRequest

        # Start a region first
        self.region_request_table.start_region_request(self.region_request_item)

        # Create RegionRequest
        region_request = RegionRequest(
            region_id=TEST_REGION_ID,
            image_id=TEST_IMAGE_ID,
            job_id=TEST_JOB_ID,
            region_bounds=[[0, 0], [256, 256]],
            tile_size=[256, 256],
            tile_overlap=[0, 0],
        )

        # Should return existing item
        result = self.region_request_table.get_or_create_region_request_item(region_request)
        assert result.region_id == TEST_REGION_ID
        assert result.image_id == TEST_IMAGE_ID

    def test_get_or_create_region_request_item_new(self):
        """Test get_or_create_region_request_item creating new item"""
        from aws.osml.model_runner.api import RegionRequest

        region_request = RegionRequest(
            region_id="new-region-id",
            image_id="new-image-id",
            job_id="new-job-id",
            region_bounds=[[0, 0], [256, 256]],
            tile_size=[256, 256],
            tile_overlap=[0, 0],
        )

        # Should create new item
        result = self.region_request_table.get_or_create_region_request_item(region_request)
        assert result.region_id == "new-region-id"
        assert result.image_id == "new-image-id"

        # Verify it was actually created in the table
        retrieved = self.region_request_table.get_region_request("new-region-id", "new-image-id")
        assert retrieved is not None

    def test_add_tile_exception_handling(self):
        """Test add_tile with exception during update"""
        from aws.osml.model_runner.common import TileState
        from aws.osml.model_runner.database.exceptions import UpdateRegionException

        self.region_request_table.start_region_request(self.region_request_item)
        
        # Mock update to raise exception
        self.region_request_table.table.update_item = MOCK_UPDATE_EXCEPTION
        
        tile = ((0, 0), (256, 256))
        with self.assertRaises(UpdateRegionException):
            self.region_request_table.add_tile(TEST_IMAGE_ID, TEST_REGION_ID, tile, TileState.SUCCEEDED)

    def test_is_image_request_complete_not_done(self):
        """Test is_image_request_complete when image is not complete"""
        from aws.osml.model_runner.database.image_request_table import ImageRequestItem

        image_request_item = ImageRequestItem(image_id=TEST_IMAGE_ID)
        image_request_item.region_count = Decimal(3)
        image_request_item.region_success = Decimal(1)
        image_request_item.region_error = Decimal(0)

        with patch("aws.osml.model_runner.database.region_request_table.RegionRequestTable.get_image_request_complete_counts") as mock_counts:
            mock_counts.return_value = (0, 1)  # Only 1 of 3 complete
            done, completed, failed = self.region_request_table.is_image_request_complete(image_request_item)
            assert done is False
            assert completed == 1
            assert failed == 0

if __name__ == "__main__":
    unittest.main()
