import logging
import sys
from typing import List, Optional

import boto3
import geojson
from geojson import Feature, FeatureCollection

from aws_oversightml_model_runner.app_config import BotoConfig, ServiceConfig
from aws_oversightml_model_runner.common import get_credentials_for_assumed_role

from .sink import Sink, SinkMode

logger = logging.getLogger(__name__)


class KinesisSink(Sink):
    def __init__(
        self,
        stream: str,
        batch_size: int = None,
        assumed_role: Optional[str] = None,
    ) -> None:
        self.stream = stream
        self.batch_size = batch_size
        if assumed_role:
            assumed_credentials = get_credentials_for_assumed_role(assumed_role)
            # Here we will be writing to Kinesis using an IAM role other than the one for this process.
            self.kinesisClient = boto3.client(
                "kinesis",
                aws_access_key_id=assumed_credentials["AccessKeyId"],
                aws_secret_access_key=assumed_credentials["SecretAccessKey"],
                aws_session_token=assumed_credentials["SessionToken"],
                config=BotoConfig.default,
            )
        else:
            # If no invocation role is provided the assumption is that the default role for this
            # container will be sufficient to write to the Kinesis stream.
            self.kinesisClient = boto3.client("kinesis", config=BotoConfig.default)

    def _flush_stream(self, partition_key: str, features: List[Feature]) -> None:
        record = geojson.dumps(FeatureCollection(features))
        self.kinesisClient.put_record(
            StreamName=self.stream,
            PartitionKey=partition_key,
            Data=record,
        )

    @property
    def mode(self) -> SinkMode:
        # Only aggregate mode is supported at the moment
        return SinkMode.AGGREGATE

    def write(self, image_id: str, features: List[Feature]) -> None:
        # image_id is the concatenation of the job id and source image url in s3. We just
        # want to base our key off of the original image file name so split by '/' and use
        # the last element
        partition_key = image_id.split("/")[-1]
        pending_features: List[Feature] = []
        pending_features_size: int = 0

        for feature in features:
            if self.batch_size == 1:
                self._flush_stream(partition_key, [feature])
            else:
                feature_size = sys.getsizeof(geojson.dumps(feature))
                if (
                    self.batch_size
                    and pending_features
                    and len(pending_features) % self.batch_size == 0
                ) or pending_features_size + feature_size > (
                    int(ServiceConfig.kinesis_max_record_size)
                ):
                    self._flush_stream(partition_key, pending_features)
                    pending_features = []
                    pending_features_size = 0

                pending_features.append(feature)
                pending_features_size += feature_size

        # Flush any remaining features
        if pending_features:
            self._flush_stream(partition_key, pending_features)
        logger.info(
            f"Wrote {len(features)} features for Image '{image_id}' to Kinesis Stream '{self.stream}'"
        )

    @staticmethod
    def name() -> str:
        return "Kinesis"
