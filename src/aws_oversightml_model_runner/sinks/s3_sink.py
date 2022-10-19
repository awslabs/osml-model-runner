import logging
import os
from typing import List, Optional

import boto3
import geojson
from geojson import Feature, FeatureCollection

from aws_oversightml_model_runner.app_config import BotoConfig
from aws_oversightml_model_runner.sinks import Sink, SinkMode
from aws_oversightml_model_runner.worker.credentials_utils import get_credentials_for_assumed_role

logger = logging.getLogger(__name__)


class S3Sink(Sink):
    def __init__(
        self,
        bucket: str,
        prefix: str,
        assumed_role: Optional[str] = None,
    ):
        self.bucket = bucket
        self.prefix = prefix
        if assumed_role:
            assumed_credentials = get_credentials_for_assumed_role(assumed_role)
            # Here we will be writing to S3 using an IAM role other than the one for this process.
            self.s3Client = boto3.client(
                "s3",
                aws_access_key_id=assumed_credentials["AccessKeyId"],
                aws_secret_access_key=assumed_credentials["SecretAccessKey"],
                aws_session_token=assumed_credentials["SessionToken"],
                config=BotoConfig.default,
            )
        else:
            # If no invocation role is provided the assumption is that the default role for this
            # container will be sufficient to write to the S3 bucket.
            self.s3Client = boto3.client("s3", config=BotoConfig.default)

    @property
    def mode(self) -> SinkMode:
        return SinkMode.AGGREGATE

    def write(self, image_id: str, features: List[Feature]) -> None:
        features_collection = FeatureCollection(features)
        # image_id is the concatenation of the job id and source image url in s3. We just
        # want to base our key off of the original image file name so split by '/' and use
        # the last element
        object_key = os.path.join(self.prefix, image_id.split("/")[-1] + ".geojson")
        # Add the aggregated features to a feature collection and encode the full set of features
        # as a GeoJSON output.
        self.s3Client.put_object(
            Body=str(geojson.dumps(features_collection)),
            Bucket=self.bucket,
            Key=object_key,
            ACL="bucket-owner-full-control",
        )
        logger.info(
            "Wrote aggregate feature collection for Image '{}' to s3://{}/{}".format(
                image_id, self.bucket, object_key
            )
        )

    @staticmethod
    def name() -> str:
        return "S3"
