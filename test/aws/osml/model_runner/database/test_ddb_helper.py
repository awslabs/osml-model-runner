#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

import os
from dataclasses import dataclass
from decimal import Decimal
from unittest import TestCase
from unittest.mock import Mock

import boto3
from botocore.exceptions import ClientError
from moto import mock_aws

TEST_IMAGE_ID = "test-image-id"


@mock_aws
class TestDDBHelper(TestCase):
    def setUp(self):
        """
        Set up the DynamoDB mock environment, including creating a test table and initializing DDB items.
        """
        from aws.osml.model_runner.app_config import BotoConfig
        from aws.osml.model_runner.database.ddb_helper import DDBItem, DDBKey
        from aws.osml.model_runner.database.image_request_table import ImageRequestItem

        self.ddb = boto3.resource("dynamodb", config=BotoConfig.default)
        self.table_name = os.environ["IMAGE_REQUEST_TABLE"]
        self.table = self.ddb.create_table(
            TableName=self.table_name,
            KeySchema=[{"AttributeName": "image_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "image_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )

        # Initialize DDB items for testing
        self.image_request_item = ImageRequestItem(image_id=TEST_IMAGE_ID)
        ddb_key = DDBKey(hash_key="image_id", hash_value="12345")
        self.ddb_item = DDBItem()
        self.ddb_item.ddb_key = ddb_key
        ddb_range_key = DDBKey(hash_key="image_id", hash_value="12345", range_key="other_field", range_value="range")
        self.range_image_request_item = ImageRequestItem(image_id="test-image-id")
        self.range_image_request_item.ddb_key = ddb_range_key

    def tearDown(self):
        """
        Clean up the mock environment by deleting the test table and resetting instance variables.
        """
        self.table.delete()
        self.table = None
        self.ddb = None
        self.table_name = None
        self.ddb_item = None
        self.image_request_item = None

    def test_ddb_item_to_put(self):
        """
        Test that the `to_put` method correctly prepares an item for a DynamoDB put operation.
        """
        data_to_put = self.ddb_item.to_put()
        assert data_to_put == {}, "Expected empty dictionary for default DDBItem"

    def test_ddb_item_to_put_and_update_with_fields(self):
        """
        Test that DDBItem helpers include only valid fields.
        """
        from aws.osml.model_runner.database.ddb_helper import DDBItem, DDBKey

        @dataclass
        class ExampleItem(DDBItem):
            pk: str = None
            name: str = None
            count: int = None

        item = ExampleItem(pk="123", name="example", count=None)
        item.ddb_key = DDBKey(hash_key="pk", hash_value="123")
        assert item.to_put() == {"pk": "123", "name": "example"}
        assert item.to_update() == {"name": "example"}

    def test_ddb_item_to_update(self):
        """
        Test that the `to_update` method correctly prepares an item for a DynamoDB update operation.
        """
        data_to_update = self.ddb_item.to_update()
        assert data_to_update == {}, "Expected empty dictionary for default DDBItem"

    def test_ddb_helper_put_ddb_item(self):
        """
        Test that the `put_ddb_item` method correctly puts an item into DynamoDB and handles conditions.
        """
        from aws.osml.model_runner.database.ddb_helper import DDBHelper

        helper = DDBHelper(self.table_name)
        res = helper.put_ddb_item(self.image_request_item)
        assert res["ResponseMetadata"]["HTTPStatusCode"] == 200, "Expected successful HTTP status code"

        with self.assertRaises(ClientError):
            # Test conditional put that should fail due to existing item
            helper.put_ddb_item(self.image_request_item, condition_expression="attribute_not_exists(image_id)")

    def test_ddb_helper_get_ddb_item(self):
        """
        Test that the `get_ddb_item` method correctly retrieves an item from DynamoDB.
        """
        from aws.osml.model_runner.database.ddb_helper import DDBHelper

        helper = DDBHelper(self.table_name)
        helper.put_ddb_item(self.image_request_item)
        returned_item_dict = helper.get_ddb_item(self.image_request_item)
        assert returned_item_dict == {"image_id": TEST_IMAGE_ID}, "Expected item to match inserted job item"

    def test_ddb_helper_delete_ddb_item(self):
        """
        Test that the `delete_ddb_item` method correctly deletes an item from DynamoDB.
        """
        from aws.osml.model_runner.database.ddb_helper import DDBHelper

        helper = DDBHelper(self.table_name)
        helper.put_ddb_item(self.image_request_item)
        res = helper.delete_ddb_item(self.image_request_item)
        assert res["ResponseMetadata"]["HTTPStatusCode"] == 200, "Expected successful deletion response"

    def test_ddb_helper_update_ddb_item(self):
        """
        Test that the `update_ddb_item` method correctly updates an item in DynamoDB.
        """
        from aws.osml.model_runner.database.ddb_helper import DDBHelper

        helper = DDBHelper(self.table_name)
        helper.put_ddb_item(self.image_request_item)
        self.image_request_item.model_name = "noop"
        results = helper.update_ddb_item(self.image_request_item)
        assert results == {"image_id": "test-image-id", "model_name": "noop"}, "Expected updated item attributes"

    def test_ddb_helper_update_ddb_item_invalid_params(self):
        """
        Test that the `update_ddb_item` method raises an exception when invalid parameters are provided.
        """
        from aws.osml.model_runner.database.ddb_helper import DDBHelper
        from aws.osml.model_runner.database.exceptions import DDBUpdateException

        helper = DDBHelper(self.table_name)
        helper.put_ddb_item(self.image_request_item)
        self.image_request_item.model_name = "noop"
        with self.assertRaises(DDBUpdateException):
            # Expect failure when only update attributes are provided without an expression
            helper.update_ddb_item(self.image_request_item, update_attr={":model_name": "noop"})

    def test_ddb_helper_update_ddb_item_with_explicit_params(self):
        """
        Test update with explicit expression and attributes.
        """
        from aws.osml.model_runner.database.ddb_helper import DDBHelper

        helper = DDBHelper(self.table_name)
        helper.put_ddb_item(self.image_request_item)
        update_exp = "SET model_name = :model_name"
        update_attr = {":model_name": "noop"}
        results = helper.update_ddb_item(self.image_request_item, update_exp=update_exp, update_attr=update_attr)
        assert results["model_name"] == "noop"

    def test_ddb_helper_query_items(self):
        """
        Test that the `query_items` method correctly queries and retrieves items based on a hash key.
        """
        from aws.osml.model_runner.database.ddb_helper import DDBHelper

        helper = DDBHelper(self.table_name)
        helper.put_ddb_item(self.image_request_item)

        retrieved_items = helper.query_items(self.image_request_item)
        assert len(retrieved_items) == 1, "Expected one item to be retrieved from query"

    def test_ddb_helper_query_items_pagination(self):
        """
        Test that query_items handles pagination and converts decimals.
        """
        from aws.osml.model_runner.database.ddb_helper import DDBHelper

        helper = DDBHelper(self.table_name)
        helper.table.query = Mock(
            side_effect=[
                {"Items": [{"value": Decimal("1.0")}], "LastEvaluatedKey": {"image_id": "next"}},
                {"Items": [{"value": Decimal("1.5")}]},
            ]
        )
        items = helper.query_items(self.image_request_item)
        assert items == [{"value": 1}, {"value": 1.5}]
        assert helper.table.query.call_count == 2

    def test_ddb_helper_get_update_params(self):
        """
        Test the utility method `get_update_params` to ensure it generates correct update expressions and attributes.
        """
        from aws.osml.model_runner.database.ddb_helper import DDBHelper

        helper = DDBHelper(self.table_name)
        self.image_request_item.model_name = "noop"
        update_expression, update_attributes = helper.get_update_params(
            self.image_request_item.to_update(), self.image_request_item
        )
        assert update_expression == "SET  model_name = :model_name", "Expected correct update expression"
        assert update_attributes == {":model_name": "noop"}, "Expected correct update attributes"

        self.range_image_request_item.model_name = "noop_2"
        update_dict = self.range_image_request_item.to_update()
        update_dict["other_field"] = "value"
        range_update_expression, range_update_attributes = helper.get_update_params(
            update_dict, self.range_image_request_item
        )
        assert range_update_expression == "SET  model_name = :model_name", "Expected range-based update expression"
        assert range_update_attributes == {":model_name": "noop_2"}, "Expected range-based update attributes"

    def test_ddb_helper_get_keys(self):
        """
        Test the utility method `get_keys` to ensure it retrieves keys correctly from DDBItem instances.
        """
        from aws.osml.model_runner.database.ddb_helper import DDBHelper

        helper = DDBHelper(self.table_name)
        keys = helper.get_keys(self.range_image_request_item)
        assert keys == {"image_id": "12345", "other_field": "range"}, "Expected hash and range keys to be correct"

        hash_only_keys = helper.get_keys(self.image_request_item)
        assert hash_only_keys == {"image_id": TEST_IMAGE_ID}

    def test_convert_decimal(self):
        """
        Test decimal conversion for lists and dicts.
        """
        from aws.osml.model_runner.database.ddb_helper import DDBHelper

        data = {"int": Decimal("2.0"), "float": Decimal("2.5"), "list": [Decimal("3.0"), {"v": Decimal("4.5")}]}
        converted = DDBHelper.convert_decimal(data)
        assert converted == {"int": 2, "float": 2.5, "list": [3, {"v": 4.5}]}

    def test_batch_write_items_with_retries(self):
        """
        Test batch_write_items retries unprocessed items.
        """
        from aws.osml.model_runner.database.ddb_helper import DDBHelper

        helper = DDBHelper(self.table_name)
        helper.client.batch_write_item = Mock(
            side_effect=[
                {"UnprocessedItems": {self.table_name: [{"PutRequest": {"Item": self.image_request_item.to_put()}}]}},
                {"UnprocessedItems": {}},
            ]
        )
        helper.batch_write_items([self.image_request_item], max_retries=2, max_delay=0)
        assert helper.client.batch_write_item.call_count == 2

    def test_batch_write_items_exceeds_retries(self):
        """
        Test batch_write_items raises when unprocessed items remain.
        """
        from aws.osml.model_runner.database.ddb_helper import DDBBatchWriteException, DDBHelper

        helper = DDBHelper(self.table_name)
        helper.client.batch_write_item = Mock(
            return_value={
                "UnprocessedItems": {self.table_name: [{"PutRequest": {"Item": self.image_request_item.to_put()}}]}
            }
        )
        with self.assertRaises(DDBBatchWriteException):
            helper.batch_write_items([self.image_request_item], max_retries=1, max_delay=0)

    def test_batch_write_items_retries_on_exception(self):
        """
        Test batch_write_items retries on exceptions and succeeds.
        """
        from aws.osml.model_runner.database.ddb_helper import DDBHelper

        helper = DDBHelper(self.table_name)
        helper.client.batch_write_item = Mock(side_effect=[Exception("boom"), {"UnprocessedItems": {}}])
        helper.batch_write_items([self.image_request_item], max_retries=1, max_delay=0)
        assert helper.client.batch_write_item.call_count == 2
