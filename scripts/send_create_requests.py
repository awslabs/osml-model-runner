import argparse
import datetime
import time

import boto3


def ListObjects(bucket_name, prefix):
    response = s3_client.list_objects(Bucket=bucket_name, Prefix=prefix)
    for content in response.get('Contents', []):
        yield content.get('Key')


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-ib', '--input-bucket', default="spacenet-dataset")
    parser.add_argument('-ip', '--input-prefix', default='AOIs/AOI_1_Rio/srcData/mosaic_3band/013022223131.tif')
    parser.add_argument('-ob', '--output-bucket', default='oversightml-iad-beta-hydratest')
    parser.add_argument('-op', '--output-prefix', default='results')
    parser.add_argument('-ts', '--tile-size', default=1024)
    parser.add_argument('-to', '--tile-overlap', default=100)
    parser.add_argument('-tf', '--tile-format', default="NITF")
    parser.add_argument('-m', '--model', default="charon-xview-endpoint")
    parser.add_argument('-ni', '--num-images', default=100)
    parser.add_argument('-e', '--endpoint', default='https://kojefgt238.execute-api.us-east-1.amazonaws.com/Beta')
    args = parser.parse_args()

    s3_client = boto3.client('s3')
    oversightml_client = boto3.client('oversightml', endpoint_url=args.endpoint)

    delay_seconds = 60 * 60 / int(args.num_images)

    for image_number in range(0, int(args.num_images)):

        if image_number > 0:
            time.sleep(delay_seconds)

        image_urls = []
        for key in ListObjects(args.input_bucket, args.input_prefix):
            if key.endswith(".tif") or key.endswith(".nitf"):
                image_urls.append("s3://" + args.input_bucket + "/" + key)

        job_name = "Job-" + datetime.datetime.utcnow().isoformat()

        image_processor = dict(name=args.model, type="SM_ENDPOINT")

        print("Sending request for: " + job_name)
        response = oversightml_client.create_image_processing_job(
            jobName=job_name,
            imageUrls=image_urls,
            outputBucket=args.output_bucket,
            outputPrefix=args.output_prefix + "/" + job_name,
            imageProcessor=image_processor,
            imageProcessorTileSize=args.tile_size,
            imageProcessorTileOverlap=args.tile_overlap,
            imageProcessorTileFormat=args.tile_format
        )
        if 'ResponseMetadata' not in response or response['ResponseMetadata']['HTTPStatusCode'] != 201:
            print("Response Not Accepted!")
            print(response)
            break
        else:
            print(f"Request Accepted: jobId={response['jobId']} jobArn={response['jobArn']}")


