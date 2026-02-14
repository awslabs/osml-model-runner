#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

TEST_TABLE_NAME = "test-region-request-table"
TEST_IMAGE_ID = "test-image-id"
TEST_REGION_ID = "test-region-id"
TEST_JOB_ID = "test-job-id"


@pytest.fixture
def region_request_table_setup():
    """
    Set up virtual DDB resources/tables for each test to use.
    """
    from aws.osml.model_runner.app_config import BotoConfig
    from aws.osml.model_runner.database.region_request_table import RegionRequestItem, RegionRequestTable

    with mock_aws():
        ddb = boto3.resource("dynamodb", config=BotoConfig.default)
        table = ddb.create_table(
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
        region_request_table = RegionRequestTable(TEST_TABLE_NAME)
        region_request_item = RegionRequestItem(TEST_REGION_ID, TEST_IMAGE_ID, TEST_JOB_ID)

        yield region_request_table, region_request_item

        table.delete()


def test_region_started_success(region_request_table_setup):
    """
    Validate that starting a region request successfully stores it in the table.
    """
    from aws.osml.model_runner.common import RequestStatus

    region_request_table, region_request_item = region_request_table_setup
    region_request_table.start_region_request(region_request_item)
    resulting_region_request_item = region_request_table.get_region_request(TEST_REGION_ID, TEST_IMAGE_ID)
    assert resulting_region_request_item.image_id == TEST_IMAGE_ID
    assert resulting_region_request_item.region_id == TEST_REGION_ID
    assert resulting_region_request_item.job_id == TEST_JOB_ID
    assert resulting_region_request_item.region_status == RequestStatus.STARTED


def test_region_complete_success(region_request_table_setup):
    """
    Validate that completing a region request updates the DDB item successfully.
    """
    from aws.osml.model_runner.common import RequestStatus

    region_request_table, region_request_item = region_request_table_setup
    region_request_table.start_region_request(region_request_item)
    region_request_table.complete_region_request(region_request_item, RequestStatus.SUCCESS)
    resulting_region_request_item = region_request_table.get_region_request(TEST_REGION_ID, TEST_IMAGE_ID)

    assert resulting_region_request_item.region_status == RequestStatus.SUCCESS
    assert resulting_region_request_item.last_updated_time is not None
    assert resulting_region_request_item.end_time is not None


def test_region_updated_success(region_request_table_setup):
    """
    Validate that updating an item in the region request table works as expected.
    """
    region_request_table, region_request_item = region_request_table_setup
    region_request_table.start_region_request(region_request_item)
    region_request_item.total_tiles = 1
    region_request_table.update_region_request(region_request_item)
    resulting_region_request_item = region_request_table.get_region_request(TEST_REGION_ID, TEST_IMAGE_ID)

    assert resulting_region_request_item.total_tiles == 1
    assert resulting_region_request_item.last_updated_time is not None


def test_region_complete_failed(region_request_table_setup):
    """
    Validate that marking a region request as failed updates the DDB item.
    """
    from aws.osml.model_runner.common import RequestStatus

    region_request_table, region_request_item = region_request_table_setup
    region_request_table.start_region_request(region_request_item)
    region_request_table.complete_region_request(region_request_item, RequestStatus.FAILED)
    resulting_region_request_item = region_request_table.get_region_request(TEST_REGION_ID, TEST_IMAGE_ID)

    assert resulting_region_request_item.region_status == RequestStatus.FAILED
    assert resulting_region_request_item.last_updated_time is not None
    assert resulting_region_request_item.end_time is not None


def test_start_region_failure_exception(mocker, region_request_table_setup):
    """
    Validate that a StartRegionException is raised when starting a region fails.
    """
    from aws.osml.model_runner.database.exceptions import StartRegionException

    region_request_table, region_request_item = region_request_table_setup
    mock_put_exception = mocker.Mock(side_effect=ClientError({"Error": {"Code": 500, "Message": "ClientError"}}, "put_item"))
    region_request_table.table.put_item = mock_put_exception
    with pytest.raises(StartRegionException):
        region_request_table.start_region_request(region_request_item)


def test_complete_region_failure_exception(mocker, region_request_table_setup):
    """
    Validate that a CompleteRegionException is raised when completing a region fails.
    """
    from aws.osml.model_runner.database.exceptions import CompleteRegionException

    region_request_table, region_request_item = region_request_table_setup
    mock_update_exception = mocker.Mock(
        side_effect=ClientError({"Error": {"Code": 500, "Message": "ClientError"}}, "update_item")
    )
    region_request_table.table.update_item = mock_update_exception
    region_request_table.start_region_request(region_request_item)
    with pytest.raises(CompleteRegionException):
        region_request_table.complete_region_request(region_request_item, "FAILED")


def test_region_updated_failure_exception(mocker, region_request_table_setup):
    """
    Validate that an UpdateRegionException is raised when updating a region request fails.
    """
    from aws.osml.model_runner.database.exceptions import UpdateRegionException

    region_request_table, region_request_item = region_request_table_setup
    mock_update_exception = mocker.Mock(
        side_effect=ClientError({"Error": {"Code": 500, "Message": "ClientError"}}, "update_item")
    )
    region_request_table.table.update_item = mock_update_exception
    region_request_table.start_region_request(region_request_item)
    with pytest.raises(UpdateRegionException):
        region_request_table.update_region_request(region_request_item)


def test_get_region_request_none(mocker, region_request_table_setup):
    """
    Validate that getting a non-existent region request returns None.
    """
    region_request_table, region_request_item = region_request_table_setup
    mock_update_exception = mocker.Mock(
        side_effect=ClientError({"Error": {"Code": 500, "Message": "ClientError"}}, "update_item")
    )
    region_request_table.table.update_item = mock_update_exception
    region_request_table.start_region_request(region_request_item)

    resulting_region_request = region_request_table.get_region_request("DOES-NOT-EXIST-REGION-ID", "DOES-NOT-EXIST-IMAGE-ID")
    assert resulting_region_request is None


def test_add_tile_success(region_request_table_setup):
    """
    Validate that tiles can be added as succeeded to the region request item.
    """
    from aws.osml.model_runner.common import TileState

    region_request_table, region_request_item = region_request_table_setup
    region_request_table.start_region_request(region_request_item)
    tile = ((0, 0), (256, 256))
    region_request_table.add_tiles(TEST_IMAGE_ID, TEST_REGION_ID, [tile], TileState.SUCCEEDED)

    success_tile_item = region_request_table.get_region_request(TEST_REGION_ID, TEST_IMAGE_ID)
    assert len(success_tile_item.succeeded_tiles) == 1


def test_add_tile_failed(region_request_table_setup):
    """
    Validate that tiles can be added as failed to the region request item.
    """
    from aws.osml.model_runner.common import TileState

    region_request_table, region_request_item = region_request_table_setup
    region_request_table.start_region_request(region_request_item)
    tile = ((0, 0), (256, 256))
    region_request_table.add_tiles(TEST_IMAGE_ID, TEST_REGION_ID, [tile], TileState.FAILED)
    failed_tile_item = region_request_table.get_region_request(TEST_REGION_ID, TEST_IMAGE_ID)

    assert len(failed_tile_item.failed_tiles) == 1


def test_add_tile_invalid_format(region_request_table_setup):
    """
    Validate that adding a tile with an invalid format raises UpdateRegionException.
    """
    from aws.osml.model_runner.common import TileState
    from aws.osml.model_runner.database.exceptions import UpdateRegionException

    region_request_table, region_request_item = region_request_table_setup
    region_request_table.start_region_request(region_request_item)

    with pytest.raises(UpdateRegionException):
        region_request_table.add_tiles(TEST_IMAGE_ID, TEST_REGION_ID, ["bad_format"], TileState.SUCCEEDED)


def test_add_tiles_batch_success(region_request_table_setup):
    """
    Validate that multiple tiles can be added in one call.
    """
    from aws.osml.model_runner.common import TileState

    region_request_table, region_request_item = region_request_table_setup
    region_request_table.start_region_request(region_request_item)
    tiles = [((0, 0), (256, 256)), ((256, 0), (512, 256))]
    region_request_table.add_tiles(TEST_IMAGE_ID, TEST_REGION_ID, tiles, TileState.SUCCEEDED)

    success_tile_item = region_request_table.get_region_request(TEST_REGION_ID, TEST_IMAGE_ID)
    assert len(success_tile_item.succeeded_tiles) == 2


def test_add_tiles_empty_list_raises(region_request_table_setup):
    """
    Validate that add_tiles rejects empty input.
    """
    from aws.osml.model_runner.common import TileState
    from aws.osml.model_runner.database.exceptions import UpdateRegionException

    region_request_table, region_request_item = region_request_table_setup
    region_request_table.start_region_request(region_request_item)

    with pytest.raises(UpdateRegionException):
        region_request_table.add_tiles(TEST_IMAGE_ID, TEST_REGION_ID, [], TileState.SUCCEEDED)


def test_from_region_request_with_partial_data():
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
