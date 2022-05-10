import time

import boto3
import botocore


class JobTable:

    def __init__(self, table_name: str):
        self.ddb_job_table = boto3.resource('dynamodb').Table(table_name)

    def image_started(self, image_id: str):
        start_time_millisec = int(time.time() * 1000)
        # These records are temporary and will expire 24 hours after creation. Jobs should take minutes to run
        # so this time should be conservative enough to let a team debug an urgent issue without leaving a
        # ton of state leftover in the system.
        expire_time_millisec = start_time_millisec + (24*60*60*1000)

        try:
            result = self.ddb_job_table.update_item(
                Key={
                    'image_id': image_id,
                },
                UpdateExpression="SET start_time = :start_time, expire_time = :expire_time",
                ConditionExpression='attribute_not_exists(start_time) OR start_time < :start_time',
                ExpressionAttributeValues={
                    ':start_time': start_time_millisec,
                    ':expire_time': expire_time_millisec
                }
            )
        except botocore.exceptions.ClientError as e:
            # Ignore the ConditionalCheckFailedException, bubble up
            # other exceptions.
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                raise

    def region_complete(self, image_id: str, error: bool = False):
        try:
            if error:
                error_count = 1
                success_count = 0
            else:
                error_count = 0
                success_count = 1

            self.ddb_job_table.update_item(
                Key={
                    'image_id': image_id,
                },
                UpdateExpression="SET region_success = region_success + :success_count, region_error = region_error + :error_count",
                ExpressionAttributeValues={
                    ':success_count': success_count,
                    ':error_count': error_count
                }
            )

        except botocore.exceptions.ClientError as e:
            raise

    def is_image_complete(self, image_id: str) -> bool:
        try:
            get_response = self.ddb_job_table.get_item(Key={'image_id': image_id})
            if 'Item' in get_response:
                job_status = get_response['Item']
                return int(job_status['region_count']) <= (
                        int(job_status['region_success']) + int(job_status['region_error']))

        except botocore.exceptions.ClientError as e:
            raise

    def image_stats(self, image_id: str, region_count: int, width: int, height: int):
        try:
            result = self.ddb_job_table.update_item(
                Key={
                    'image_id': image_id,
                },
                UpdateExpression="SET region_count = :region_count, width = :width, height = :height, region_success = :region_success, region_error = :region_error",
                ExpressionAttributeValues={
                    ':region_count': region_count,
                    ':width': width,
                    ':height': height,
                    ':region_success': 0,
                    ':region_error': 0
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
