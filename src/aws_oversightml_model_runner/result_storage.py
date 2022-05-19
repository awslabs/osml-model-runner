import logging
import os
from typing import List, Dict

import boto3
import geojson
from geojson import Feature, FeatureCollection


class ResultStorage:

    def __init__(self, s3_output_bucket: str, s3_output_key_prefix: str, assumed_credentials: Dict[str, str] = None):
        self.s3_output_bucket = s3_output_bucket
        self.s3_output_key_prefix = s3_output_key_prefix
        if assumed_credentials is not None:
            # Here we will be writing to S3 using an IAM role other than the one for this process.
            self.s3Client = boto3.client('s3',
                                         aws_access_key_id=assumed_credentials['AccessKeyId'],
                                         aws_secret_access_key=assumed_credentials['SecretAccessKey'],
                                         aws_session_token=assumed_credentials['SessionToken'])
        else:
            # If no invocation role is provided the assumption is that the default role for this container will be
            # sufficient to write to the S3 bucket.
            self.s3Client = boto3.client('s3')

    def write_to_s3(self, image_id: str, features: List[Feature]) -> None:
        logging.info("Writing Image Results to S3")
        # Add the aggregated features to a feature collection and encode the full set of features as a GeoJSON output.
        output_feature_collection = FeatureCollection(features)
        self.s3Client.put_object(
            Body=str(geojson.dumps(output_feature_collection)),
            Bucket=self.s3_output_bucket,
            Key=os.path.join(self.s3_output_key_prefix, image_id.split('/')[-1] + '.geojson'),
            ACL="bucket-owner-full-control"
        )
        logging.info("Done writing Image Results to S3")
