import json

import boto3

#bucket_name = "spacenet-dataset"
#object_prefix = "AOIs/AOI_1_Rio/srcData/mosaic_3band/"
#bucket_name = "spacenet-parrised-devaccount"
#object_prefix = "AOI_1_Rio/srcData/rasterData/3-Band/"
bucket_name = "spacenet-dataset"
object_prefix = "AOIs/AOI_7_Moscow/PS-RGB/"


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
            'imageURL': "s3://" + bucket_name + "/" + key,
            'outputBucket': "spacenet-parrised-devaccount",
            'outputPrefix': "oversight",
            'modelName': "charon-xview-endpoint"
        }

        sqs_client.send_message(
            QueueUrl='https://sqs.us-east-1.amazonaws.com/010321660603/Oversight-ImageQueue',
            MessageBody=json.dumps(message_body)
        )

