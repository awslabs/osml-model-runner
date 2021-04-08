import logging
import os
import numpy as np
from typing import List, Dict, Union, Any

import boto3
from boto3 import dynamodb

import geojson
from geojson import Feature
from .metrics import metric_scope, now

class FeatureTable:

    def __init__(self, table_name: str, tile_size: [int], overlap: [int]):
        self.ddb_feature_table = self.get_dynamodb_resource().Table(table_name)
        self.tile_size = tile_size
        self.overlap = overlap

    @metric_scope
    def add_features(self, features, metrics):

        feature_store_start = now()
        for key, value in self.group_features_by_key(features).items():
            try:

                hash_key, range_key = key.split("-region-", 1)
                encoded_features = []
                for feature in value:
                    encoded_features.append(geojson.dumps(feature))

                # TODO: Can do a simple put if range key is not overlap region
                result = self.ddb_feature_table.update_item(
                    Key={
                        'hash_key': hash_key,
                        'range_key': range_key
                    },
                    UpdateExpression="SET features = list_append(if_not_exists(features, :empty_list), :i)",
                    ExpressionAttributeValues={
                        ':i': encoded_features,
                        ':empty_list': []
                    },
                    ReturnValues="UPDATED_NEW"
                )
                if result['ResponseMetadata']['HTTPStatusCode'] != 200:
                    logging.error("Unable to update feature table")
            except Exception as e:
                logging.exception(e)

        feature_store_end = now()
        metrics.put_metric("FeatureStoreLatency", (feature_store_end - feature_store_start), "Microseconds")

    @metric_scope
    def get_all_features(self, image_id:str, metrics) -> List[Feature]:
        feature_agg_start = now()
        response = self.ddb_feature_table.query(
            KeyConditionExpression=dynamodb.conditions.Key('hash_key').eq(image_id)
        )

        deduped_features = []
        for item in response['Items']:
            features = []
            for encoded_feature in item['features']:
                features.append(geojson.loads(encoded_feature))

            deduped_features.extend(feature_nms(features))

        feature_agg_end = now()
        metrics.put_metric("FeatureAggLatency", (feature_agg_end - feature_agg_start), "Microseconds")

        return deduped_features

    def group_features_by_key(self, features):
        result = {}
        for feature in features:
            key = self.generate_tile_key(feature)
            result.setdefault(key, []).append(feature)
        return result

    def generate_tile_key(self, feature: Feature) -> str:
        bbox = feature['properties']['bounds_imcoords']

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

        return "{}-region-{}:{}:{}:{}".format(feature['properties']['image_id'],
                                       min_x_index,
                                       max_x_index,
                                       min_y_index,
                                       max_y_index)


    @staticmethod
    def get_dynamodb_resource():
        return boto3.resource('dynamodb')


def feature_nms(feature_list: List[Feature]) -> List[Feature]:
    """
    NMS implementation adapted from https://gist.github.com/quantombone/1144423

    :param feature_list: a list of geojson features with a property of bounds_imcoords
    :return: the filtered list of features
    """

    if not feature_list or len(feature_list) == 0:
        return []

    overlap = 0.75

    pick = []

    # a numpy array of objects with columns [detect_id, x1, y1, x2, y2]
    selected_feature_properties = []
    for feature in feature_list:
        id_bbox_row = [feature['id']]
        id_bbox_row.extend(feature['properties']['bounds_imcoords'])
        selected_feature_properties.append(id_bbox_row)
    feature_bbox_array = np.array(selected_feature_properties)

    x1 = feature_bbox_array[:, 1].astype("float")
    y1 = feature_bbox_array[:, 2].astype("float")
    x2 = feature_bbox_array[:, 3].astype("float")
    y2 = feature_bbox_array[:, 4].astype("float")

    area = (x2 - x1 + 1) * (y2 - y1 + 1)
    I = np.argsort(y2)

    while len(I) > 0:
        last = len(I) - 1
        i = I[last]
        pick.append(i)

        xx1 = np.maximum(x1[i], x1[I[:last]])
        yy1 = np.maximum(y1[i], y1[I[:last]])
        xx2 = np.minimum(x2[i], x2[I[:last]])
        yy2 = np.minimum(y2[i], y2[I[:last]])

        w = np.maximum(0, xx2 - xx1 + 1)
        h = np.maximum(0, yy2 - yy1 + 1)

        o = (w * h) / area[I[:last]]

        I = np.delete(I, np.concatenate(([last], np.where(o > overlap)[0])))

    selected_feature_ids = feature_bbox_array[pick][:, 0]
    feature_list[:] = [feature for feature in feature_list if feature['id'] in selected_feature_ids]

    # Turning off this metric. When measured this NMS was well in the weeds of the overall processing time
    # (~250 microseconds each) and the sum of the invocations is captured in the total feature aggregation
    # time.

    return feature_list
