# This is a small utility script that assumes any objects that match a S3 bucket + prefix are images. These images
# are all sent to a SQS queue for processing.
# TODO: Parameterize this script and make it a more robust dev test tool, everything is hard coded
import json
import uuid

import boto3

# bucket_name = "spacenet-dataset"
# object_prefix = "AOIs/AOI_1_Rio/srcData/mosaic_3band/"
bucket_name = "spacenet-parrised-devaccount"
object_prefix = "AOI_1_Rio/srcData/rasterData/3-Band/013022223103.tif"
# bucket_name = "spacenet-dataset"
# object_prefix = "AOIs/AOI_7_Moscow/PS-RGB/"


s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')


def ListObjects(bucket_name, prefix):
    response = s3_client.list_objects(Bucket=bucket_name, Prefix=prefix)
    for content in response.get('Contents', []):
        yield content.get('Key')


if __name__ == "__main__":

    for key in ListObjects(bucket_name, object_prefix):
        if not key.endswith(".tif"):
            continue
        message_body = {
            'jobArn': "arn:aws:oversightml:us-east-1:010321660603:ipj/test-job",
            'jobName': "test-job",
            'jobId': str(uuid.uuid4()),
            'jobStatus': "SUBMITTED",
            'imageUrls': ["s3://" + bucket_name + "/" + key],
            'outputBucket': "spacenet-parrised-devaccount",
            'outputPrefix': "oversight",
            'imageProcessor': {
                'name': "charon-xview-endpoint",
                'type': "SM_ENDPOINT"
            },
            'imageProcessorTileSize': 1024,
            'imageProcessorTileOverlap': 50,
            'imageProcessorTileFormat': "NITF"
        }

        sqs_client.send_message(
            QueueUrl='https://sqs.us-east-1.amazonaws.com/010321660603/Oversight-ImageQueue',
            MessageBody=json.dumps(message_body)
        )
