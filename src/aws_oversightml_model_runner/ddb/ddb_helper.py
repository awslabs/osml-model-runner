from dataclasses import asdict, dataclass, field
from typing import Tuple

import boto3

from aws_oversightml_model_runner.app_config import BotoConfig
from aws_oversightml_model_runner.exceptions.exceptions import DDBUpdateFailed


@dataclass
class DDBKey:
    """
    DDBItem is a dataclass meant to represent a single item in a DynamoDB table via a key-value pair

    The data schema is defined as follows:
    key:  (str) the table key to index on
    value: (str) the value of the item (given a key) to use
    """

    hash_key: str = field(init=False)
    hash_value: str = field(init=False)

    def to_put(self) -> dict:
        return {
            k: v
            for k, v in asdict(self).items()
            if v is not None and v is not self.hash_key and k != "value"
        }

    def to_update(self) -> dict:
        return {
            k: v
            for k, v in asdict(self).items()
            if v is not None and v is not self.hash_value and v is not self.hash_key
        }


class DDBHelper:
    """
    DDBHelper is a class meant to help OSML with accessing and interacting with DynamoDB tables. Generally this class
    should be inherited by downstream specific table classes to build on top of such as the FeatureTable and JobTable
    classes.

    :param table_name: (str) the name of the table to interact with
    """

    def __init__(self, table_name: str) -> None:
        # build a table resource to use for accessing data
        self.table = boto3.resource("dynamodb", config=BotoConfig.default).Table(table_name)

    def get_ddb_item(self, ddb_item: DDBKey) -> dict:
        """
        Get a DynamoDB item from table

        :param ddb_item: DDBItem = item that we want to get (required)

        :return: dict: response from the get_item request
        """
        response = self.table.get_item(
            Key={
                ddb_item.hash_key: ddb_item.hash_value,
            }
        )
        return response["Item"]

    def put_ddb_item(self, ddb_item: DDBKey) -> dict:
        """
        Put a DynamoDB item into the table

        :param ddb_item: dict = item that we want to put (required)

        :return: dict: response from the put_item request
        """
        put_item = ddb_item.to_put()
        response = self.table.put_item(Item=put_item)
        return response

    def delete_ddb_item(self, ddb_item: DDBKey) -> dict:
        """
        Delete a DynamoDB item from the table

        :param ddb_item: DDBItem = item that we want to delete (required)

        :return: dict: response from the delete_item request
        """
        return self.table.delete_item(
            Key={
                ddb_item.hash_key: ddb_item.hash_value,
            }
        )

    def update_ddb_item(
        self, ddb_item: DDBKey, update_exp: str = None, update_attr: dict = None
    ) -> dict:
        """
        Update the DynamoDB item based on the contents of an input dictionary. If the user doesn't
        provide an update expression and attributes one will be generated from the body.

        :param ddb_item: DDBItem = item that we want to update (required)
        :param update_exp: Optional[str] = the update expression to use for the update
        :param update_attr: Optional[list] = attribute string to use when updating DDB item

        :return: dict = the new ddb item as a dict
        """
        # if we weren't provided an explicit update expression/attributes
        # then we'll build them from the body
        if not update_exp and not update_attr:
            update_item = ddb_item.to_update()
            update_exp, update_attr = self.get_update_params(update_item)

        # if we still don't have an update expression then we'll just
        if update_exp and update_attr:
            response = self.table.update_item(
                Key={
                    ddb_item.hash_key: ddb_item.hash_value,
                },
                UpdateExpression=update_exp,
                ExpressionAttributeValues=update_attr,
            )
            return response
        else:
            raise DDBUpdateFailed(
                "Failed to produce update expression or attributes for DDB update!"
            )

    @staticmethod
    def get_update_params(body: dict) -> Tuple[str, dict]:
        """
        Generate an update expression and a dict of values to update a dynamodb table.

        :param body: dict = the body of the request that contains the updated data

        :return: Tuple[str, dict] = the generated update expression and attributes
        """
        update_expr = ["SET "]
        update_attr = dict()

        for key, val in body.items():
            update_expr.append(f" {key} = :{key},")
            update_attr[f":{key}"] = val

        return "".join(update_expr)[:-1], update_attr
