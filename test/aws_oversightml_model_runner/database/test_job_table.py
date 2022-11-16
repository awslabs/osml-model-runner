import unittest
from decimal import Decimal
from unittest import mock
from unittest.mock import Mock

import boto3
from botocore.exceptions import ClientError
from moto import mock_dynamodb

from configuration import (
    TEST_ENV_CONFIG,
    TEST_IMAGE_ID,
    TEST_JOB_TABLE_ATTRIBUTE_DEFINITIONS,
    TEST_JOB_TABLE_KEY_SCHEMA,
)

TEST_MOCK_PUT_EXCEPTION = Mock(
    side_effect=ClientError({"Error": {"Code": 500, "Message": "ClientError"}}, "put_item")
)
TEST_MOCK_UPDATE_EXCEPTION = Mock(
    side_effect=ClientError({"Error": {"Code": 500, "Message": "ClientError"}}, "update_item")
)


@mock_dynamodb
@mock.patch.dict("os.environ", TEST_ENV_CONFIG, clear=True)
class TestJobTable(unittest.TestCase):
    def setUp(self):
        """
        Set up virtual DDB resources/tables for each test to use
        """
        from aws_oversightml_model_runner.app_config import BotoConfig
        from aws_oversightml_model_runner.database.job_table import JobItem, JobTable

        # Prepare something ahead of all tests
        # Create virtual DDB table to write test data into
        self.ddb = boto3.resource("dynamodb", config=BotoConfig.default)
        self.table = self.ddb.create_table(
            TableName=TEST_ENV_CONFIG["JOB_TABLE"],
            KeySchema=TEST_JOB_TABLE_KEY_SCHEMA,
            AttributeDefinitions=TEST_JOB_TABLE_ATTRIBUTE_DEFINITIONS,
            BillingMode="PAY_PER_REQUEST",
        )
        self.job_table = JobTable(TEST_ENV_CONFIG["JOB_TABLE"])
        self.job_item = JobItem(image_id=TEST_IMAGE_ID)

    def tearDown(self):
        """
        Delete virtual DDB resources/tables after each test
        """

        self.table.delete()
        self.ddb = None
        self.job_table = None
        self.job_item = None

    def test_image_started_success(self):
        """
        Validate we can start an image, and it gets created in the table
        """
        self.job_table.start_image_request(self.job_item)
        resulting_job_item = self.job_table.get_image_request(TEST_IMAGE_ID)
        assert resulting_job_item.image_id == TEST_IMAGE_ID

    def test_region_complete_success_count(self):
        """
        Validate that when we complete a region successfully it updates the ddb item
        """
        self.job_table.start_image_request(self.job_item)
        self.job_table.complete_region_request(TEST_IMAGE_ID)
        resulting_job_item = self.job_table.get_image_request(TEST_IMAGE_ID)
        assert resulting_job_item.region_success == Decimal(1)
        assert resulting_job_item.region_error == Decimal(0)

    def test_region_complete_error_count(self):
        """
        Validate that when we fail to complete a region it updates the ddb item
        """
        self.job_table.start_image_request(self.job_item)
        self.job_item.region_count = Decimal(0)
        self.job_item.region_success = Decimal(0)
        self.job_item.region_error = Decimal(0)
        self.job_table.update_ddb_item(self.job_item)
        self.job_table.complete_region_request(TEST_IMAGE_ID, True)
        resulting_job_item = self.job_table.get_image_request(TEST_IMAGE_ID)
        assert resulting_job_item.region_error == Decimal(1)
        assert resulting_job_item.region_success == Decimal(0)

    def test_is_image_complete_success(self):
        """
        Validate that we can successfully determine when an image has been completed
        """
        # Build a job item with all our relevant image metadata
        self.job_table.start_image_request(self.job_item)
        self.job_item.region_count = Decimal(1)
        self.job_item.region_success = Decimal(1)
        self.job_item.region_error = Decimal(0)
        self.job_table.update_ddb_item(self.job_item)

        assert self.job_table.is_image_request_complete(self.job_item)

    def test_region_ended_success(self):
        """
        Validate that we can successfully end an images processing by setting its end time
        """
        self.job_table.start_image_request(self.job_item)
        self.job_table.end_image_request(TEST_IMAGE_ID)
        resulting_job_item = self.job_table.get_image_request(TEST_IMAGE_ID)
        assert resulting_job_item.end_time is not None

    def test_start_image_failure(self):
        """
        Validate that throw the correct StartImageFailed exception
        """
        from aws_oversightml_model_runner.database.exceptions import StartImageException

        self.job_table.table.put_item = TEST_MOCK_PUT_EXCEPTION
        with self.assertRaises(StartImageException):
            self.job_table.start_image_request(self.job_item)

    def test_complete_region_failure(self):
        from aws_oversightml_model_runner.database.exceptions import CompleteRegionException

        self.job_table.table.update_item = TEST_MOCK_PUT_EXCEPTION
        self.job_table.start_image_request(self.job_item)
        with self.assertRaises(CompleteRegionException):
            self.job_table.complete_region_request(TEST_IMAGE_ID)

    def test_end_image_failure(self):
        from aws_oversightml_model_runner.database.exceptions import EndImageException

        self.job_table.table.update_item = TEST_MOCK_PUT_EXCEPTION
        self.job_table.start_image_request(self.job_item)
        with self.assertRaises(EndImageException):
            self.job_table.end_image_request(TEST_IMAGE_ID)


if __name__ == "__main__":
    unittest.main()
