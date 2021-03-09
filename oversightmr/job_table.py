import time

import boto3
import botocore


class JobTable:

    def __init__(self, table_name: str):
        self.ddb_job_table = boto3.resource('dynamodb').Table(table_name)

    def image_started(self, image_id: str):
        start_time_millisec = int(time.time() * 1000)

        try:
            result = self.ddb_job_table.update_item(
                Key={
                    'image_id': image_id,
                },
                UpdateExpression="SET start_time = :start_time",
                ConditionExpression='attribute_not_exists(start_time) OR start_time < :start_time',
                ExpressionAttributeValues={
                    ':start_time': start_time_millisec,
                }
            )
        except botocore.exceptions.ClientError as e:
            # Ignore the ConditionalCheckFailedException, bubble up
            # other exceptions.
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                raise

    def image_stats(self, image_id: str, total_tile_count: int, width: int, height: int):
        try:
            result = self.ddb_job_table.update_item(
                Key={
                    'image_id': image_id,
                },
                UpdateExpression="SET tile_count = :tile_count, width = :width, height = :height",
                ExpressionAttributeValues={
                    ':tile_count': total_tile_count,
                    ':width': width,
                    ':height': height
                }
            )
        except botocore.exceptions.ClientError as e:
                raise

    def image_ended(self, image_id: str):
        end_time_millisec = int(time.time() * 1000)

        try:
            result = self.ddb_job_table.update_item(
                Key={
                    'image_id': image_id,
                },
                UpdateExpression="SET end_time = :end_time",
                ConditionExpression='attribute_not_exists(end_time) OR emd_time > :end_time',
                ExpressionAttributeValues={
                    ':end_time': end_time_millisec,
                }
            )
        except botocore.exceptions.ClientError as e:
            # Ignore the ConditionalCheckFailedException, bubble up
            # other exceptions.
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                raise

