import logging
import os
from typing import List

import boto3
import geojson
from geojson import Feature, FeatureCollection


class ResultStorage:

    def __init__(self, s3_output_bucket: str, s3_output_key_prefix: str):
        self.s3_output_bucket = s3_output_bucket
        self.s3_output_key_prefix = s3_output_key_prefix
        self.s3Client = boto3.client('s3')

    def write_to_s3(self, image_id: str, features: List[Feature]) -> None:
        logging.info("Writing Image Results to S3")
        # Add the aggregated features to a feature collection and encode the full set of features as a GeoJSON output.
        output_feature_collection = FeatureCollection(features)
        self.s3Client.put_object(
            Body=str(geojson.dumps(output_feature_collection)),
            Bucket=self.s3_output_bucket,
            Key=os.path.join(self.s3_output_key_prefix, image_id.split('/')[-1] + '.geojson')
        )
        logging.info("Done writing Image Results to S3")
