# This is a small utility script that assumes any objects that match a S3 bucket + prefix are images. These images
# are all sent to a SQS queue for processing.
# TODO: Parameterize this script and make it a more robust dev test tool, everything is hard coded
import json
import os
from secrets import token_hex

import boto3

BUCKET_NAME = os.environ["BUCKET_NAME"]
OBJECT_PREFIX = os.environ["OBJECT_PREFIX"]
ACCOUNT_USER = os.environ["USER"]
ACCOUNT_NUMBER = os.environ["ACCOUNT_NUMBER"]
REGION = os.environ["REGION"]

s3_client = boto3.client("s3")
sqs_client = boto3.client("sqs")


def ListObjects(bucket_name, prefix):
    response = s3_client.list_objects(Bucket=bucket_name, Prefix=prefix)
    for content in response.get("Contents", []):
        yield content.get("Key")


if __name__ == "__main__":

    for key in ListObjects(BUCKET_NAME, OBJECT_PREFIX):
        if not key.endswith(".tif"):
            continue
        message_body = {
            "jobArn": "arn:aws:oversightml:" + REGION + ":" + ACCOUNT_NUMBER + ":ipj/test-job",
            "jobName": "test-job",
            "jobId": token_hex(16),
            "jobStatus": "SUBMITTED",
            "imageUrls": ["s3://" + BUCKET_NAME + "/" + key],
            "outputBucket": "spacenet-" + ACCOUNT_USER + "-devaccount",
            "outputPrefix": "oversight",
            "imageProcessor": {"name": "charon-xview-endpoint", "type": "SM_ENDPOINT"},
            "imageProcessorTileSize": 1024,
            "imageProcessorTileOverlap": 50,
            "imageProcessorTileFormat": "NITF",
        }

        sqs_client.send_message(
            QueueUrl="https://sqs.{0}.amazonaws.com/{1}/ImageRequestQueue".format(
                REGION, ACCOUNT_NUMBER
            ),
            MessageBody=json.dumps(message_body),
        )
