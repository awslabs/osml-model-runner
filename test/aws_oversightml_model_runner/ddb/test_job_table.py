import unittest
from decimal import Decimal
from unittest import mock
from unittest.mock import Mock

import boto3
import botocore
import moto 
from moto.dynamodb2 import mock_dynamodb2


from aws_oversightml_model_runner.exceptions.exceptions import (
    CompleteRegionFailed,
    EndImageFailed,
    ImageStatsFailed,
    StartImageFailed,
)
from configuration import TEST_ENV_CONFIG

TEST_IMAGE_ID = "test-image-id"
TEST_JOB_TABLE_NAME = "ImageProcessingJobs"
TEST_JOB_TABLE_KEY_SCHEMA = [{"AttributeName": "image_id", "KeyType": "HASH"}]
TEST_JOB_TABLE_ATTRIBUTE_DEFINITIONS = [{"AttributeName": "image_id", "AttributeType": "S"}]
TEST_MOCK_PUT_EXCEPTION = Mock(
    side_effect=botocore.exceptions.ClientError(
        {"Error": {"Code": 500, "Message": "ClientError"}}, "put_item"
    )
)
TEST_MOCK_UPDATE_EXCEPTION = Mock(
    side_effect=botocore.exceptions.ClientError(
        {"Error": {"Code": 500, "Message": "ClientError"}}, "update_item"
    )
)


@mock.patch.dict("os.environ", TEST_ENV_CONFIG, clear=True)
@mock_dynamodb2
class TestJobTable(unittest.TestCase):
    def setUp(self):
        """
        Set up virtual DDB resources/tables for each test to use
        """
        from aws_oversightml_model_runner.app_config import BotoConfig
        from aws_oversightml_model_runner.ddb.job_table import JobTable

        # Prepare something ahead of all tests
        # Create virtual DDB table to write test data into
        self.ddb = boto3.resource("dynamodb", config=BotoConfig.default)
        self.table = self.ddb.create_table(
            TableName=TEST_JOB_TABLE_NAME,
            KeySchema=TEST_JOB_TABLE_KEY_SCHEMA,
            AttributeDefinitions=TEST_JOB_TABLE_ATTRIBUTE_DEFINITIONS,
        )
        self.job_table = JobTable(TEST_JOB_TABLE_NAME)

    def tearDown(self):
        """
        Delete virtual DDB resources/tables after each test
        """
        self.table.delete()
        self.dynamodb = None
        self.job_table = None

    def test_image_started_success(self):
        """
        Validate we can start an image, and it gets created in the table
        """
        self.job_table.start_image(TEST_IMAGE_ID)
        resulting_job_item = self.job_table.get_job_item(TEST_IMAGE_ID)
        assert resulting_job_item.image_id == TEST_IMAGE_ID

    def test_region_complete_success_count(self):
        """
        Validate that when we complete a region successfully it updates the ddb item
        """
        self.job_table.start_image(TEST_IMAGE_ID)
        self.job_table.complete_region(TEST_IMAGE_ID)
        resulting_job_item = self.job_table.get_job_item(TEST_IMAGE_ID)
        assert resulting_job_item.region_success == Decimal(1)
        assert resulting_job_item.region_error == Decimal(0)

    def test_region_complete_error_count(self):
        """
        Validate that when we fail to complete a region it updates the ddb item
        """
        self.job_table.start_image(TEST_IMAGE_ID)
        self.job_table.complete_region(TEST_IMAGE_ID, True)
        resulting_job_item = self.job_table.get_job_item(TEST_IMAGE_ID)
        assert resulting_job_item.region_error == Decimal(1)
        assert resulting_job_item.region_success == Decimal(0)

    def test_is_image_complete_success(self):
        """
        Validate that we can successfully determine when an image has been completed
        """
        self.job_table.start_image(TEST_IMAGE_ID)
        self.job_table.image_stats(TEST_IMAGE_ID, Decimal(0), Decimal(0), Decimal(0))
        assert self.job_table.is_image_complete(TEST_IMAGE_ID)

    def test_image_stats_success(self):
        """
        Validate that we can successfully set the image statistics for the ddb item
        """
        self.job_table.start_image(TEST_IMAGE_ID)
        self.job_table.image_stats(TEST_IMAGE_ID, Decimal(0), Decimal(0), Decimal(0))
        resulting_job_item = self.job_table.get_job_item(TEST_IMAGE_ID)
        assert resulting_job_item.region_count == Decimal(0)
        assert int(resulting_job_item.width) == 0
        assert int(resulting_job_item.height) == 0

    def test_region_embedded_success(self):
        """
        Validate that we can successfully end an images processing by setting its end time
        """
        self.job_table.start_image(TEST_IMAGE_ID)
        self.job_table.end_image(TEST_IMAGE_ID)
        resulting_job_item = self.job_table.get_job_item(TEST_IMAGE_ID)
        assert resulting_job_item.end_time is not None

    def test_start_image_failure(self):
        """
        Validate that throw the correct StartImageFailed exception
        """
        self.job_table.table.put_item = TEST_MOCK_PUT_EXCEPTION
        with self.assertRaises(StartImageFailed):
            self.job_table.start_image(TEST_IMAGE_ID)

    def test_complete_region_failure(self):
        self.job_table.table.update_item = TEST_MOCK_PUT_EXCEPTION
        self.job_table.start_image(TEST_IMAGE_ID)
        with self.assertRaises(CompleteRegionFailed):
            self.job_table.complete_region(TEST_IMAGE_ID)

    def test_end_image_failure(self):
        self.job_table.table.update_item = TEST_MOCK_PUT_EXCEPTION
        self.job_table.start_image(TEST_IMAGE_ID)
        with self.assertRaises(EndImageFailed):
            self.job_table.end_image(TEST_IMAGE_ID)

    def test_image_stats_failure(self):
        self.job_table.table.update_item = TEST_MOCK_PUT_EXCEPTION
        self.job_table.start_image(TEST_IMAGE_ID)
        with self.assertRaises(ImageStatsFailed):
            self.job_table.image_stats(TEST_IMAGE_ID, Decimal(0), Decimal(0), Decimal(0))


if __name__ == "__main__":
    unittest.main()
