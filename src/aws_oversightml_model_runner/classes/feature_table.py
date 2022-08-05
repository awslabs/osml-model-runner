import logging
import time
from typing import Dict, List

import boto3
import geojson
from aws_embedded_metrics.logger.metrics_logger import MetricsLogger
from aws_embedded_metrics.metric_scope import metric_scope
from aws_embedded_metrics.unit import Unit
from boto3 import dynamodb
from geojson import Feature

from aws_oversightml_model_runner.classes.timer import Timer
from aws_oversightml_model_runner.utils.constants import (
    BOTO_CONFIG,
    FEATURE_AGG_LATENCY_METRIC,
    FEATURE_ERROR_METRIC,
    FEATURE_STORE_LATENCY_METRIC,
    FEATURE_UPDATE_ERROR_CODE,
    FEATURE_UPDATE_EXCEPTION_ERROR_CODE,
)
from aws_oversightml_model_runner.utils.feature_helper import feature_nms
from aws_oversightml_model_runner.utils.image_helper import ImageDimensions

logger = logging.getLogger(__name__)


class FeatureTable:
    def __init__(self, table_name: str, tile_size: ImageDimensions, overlap: ImageDimensions):
        self.ddb_feature_table = self.get_dynamodb_resource().Table(table_name)
        self.tile_size = tile_size
        self.overlap = overlap

    @metric_scope
    def add_features(self, features, model_name: str, metrics: MetricsLogger):
        metrics.set_dimensions()
        start_time_millisec = int(time.time() * 1000)
        # These records are temporary and will expire 24 hours after creation. Jobs should take
        # minutes to run so this time should be conservative enough to let a team debug an urgent
        # issue without leaving a ton of state leftover in the system.
        expire_time_millisec = start_time_millisec + (24 * 60 * 60 * 1000)
        with Timer(
            task_str="Add image features",
            metric_name=FEATURE_STORE_LATENCY_METRIC,
            logger=logger,
            metrics_logger=metrics,
        ):
            for key, value in self.group_features_by_key(features).items():
                try:
                    hash_key, range_key = key.split("-region-", 1)
                    encoded_features = []
                    for feature in value:
                        encoded_features.append(geojson.dumps(feature))

                    # TODO: Can do a simple put if range key is not overlap region
                    result = self.ddb_feature_table.update_item(
                        Key={"hash_key": hash_key, "range_key": range_key},
                        UpdateExpression="""
                            SET
                                features = list_append(if_not_exists(features, :empty_list), :i),
                                expire_time = :expire_time
                        """,
                        ExpressionAttributeValues={
                            ":i": encoded_features,
                            ":empty_list": [],
                            ":expire_time": expire_time_millisec,
                        },
                        ReturnValues="UPDATED_NEW",
                    )
                    if result["ResponseMetadata"]["HTTPStatusCode"] != 200:
                        metrics.put_metric(FEATURE_UPDATE_ERROR_CODE, 1, Unit.COUNT.value)
                        metrics.put_metric(FEATURE_ERROR_METRIC, 1, Unit.COUNT.value)
                        logger.error(
                            "Unable to update feature table - HTTP Status Code: {}".format(
                                result["ResponseMetadata"]["HTTPStatusCode"]
                            )
                        )
                except Exception as e:
                    metrics.put_metric(FEATURE_UPDATE_EXCEPTION_ERROR_CODE, 1, Unit.COUNT.value)
                    metrics.put_metric(FEATURE_ERROR_METRIC, 1, Unit.COUNT.value)
                    logger.error("Unable to update feature table")
                    logger.exception(e)

    @metric_scope
    def get_all_features(self, image_id: str, metrics: MetricsLogger) -> List[Feature]:
        metrics.set_dimensions()
        all_features_retrieved = False
        deduped_features: List[Feature] = []

        with Timer(
            task_str="Aggregate and dedeuplicate image features",
            metric_name=FEATURE_AGG_LATENCY_METRIC,
            logger=logger,
            metrics_logger=metrics,
        ):
            response = self.ddb_feature_table.query(
                KeyConditionExpression=dynamodb.conditions.Key("hash_key").eq(image_id)
            )

            while not all_features_retrieved:
                for item in response["Items"]:
                    features = []
                    for encoded_feature in item["features"]:
                        features.append(geojson.loads(encoded_feature))

                    deduped_features.extend(feature_nms(features))

                if "LastEvaluatedKey" in response:
                    response = self.ddb_feature_table.query(
                        KeyConditionExpression=dynamodb.conditions.Key("hash_key").eq(image_id),
                        ExclusiveStartKey=response["LastEvaluatedKey"],
                    )
                else:
                    all_features_retrieved = True

        return deduped_features

    def group_features_by_key(self, features):
        result: Dict[str, List[Feature]] = {}
        for feature in features:
            key = self.generate_tile_key(feature)
            result.setdefault(key, []).append(feature)
        return result

    def generate_tile_key(self, feature: Feature) -> str:
        bbox = feature["properties"]["bounds_imcoords"]

        # TODO: Check tile size to see if it is w,h or row/col
        # This is the size of the unique pixels in each tile
        stride_x = self.tile_size[0] - self.overlap[0]
        stride_y = self.tile_size[1] - self.overlap[1]

        max_x_index = int(bbox[2] / stride_x)
        max_y_index = int(bbox[3] / stride_y)

        min_x_index = int(bbox[0] / stride_x)
        min_y_index = int(bbox[1] / stride_y)
        min_x_offset = int(bbox[0]) % stride_x
        min_y_offset = int(bbox[1]) % stride_y

        if min_x_offset < self.overlap[0] and min_x_index > 0:
            min_x_index -= 1
        if min_y_offset < self.overlap[1] and min_y_index > 0:
            min_y_index -= 1

        return "{}-region-{}:{}:{}:{}".format(
            feature["properties"]["image_id"], min_x_index, max_x_index, min_y_index, max_y_index
        )

    @staticmethod
    def get_dynamodb_resource():
        return boto3.resource("dynamodb", config=BOTO_CONFIG)
