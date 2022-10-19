from typing import List

import geojson
import mock
import pytest
from botocore.stub import ANY, Stubber

from configuration import TEST_ENV_CONFIG


@mock.patch.dict("os.environ", TEST_ENV_CONFIG, clear=True)
def test_get_all_features_paginated():
    from aws_oversightml_model_runner.ddb.feature_table import FeatureTable

    feature_table = FeatureTable("TEST-TABLE-NAME", (1024, 1024), (100, 100))

    with Stubber(feature_table.ddb_feature_table.meta.client) as ddb_stubber:
        page_1_params = {
            "TableName": "TEST-TABLE-NAME",
            "KeyConditionExpression": ANY,
        }
        page_1_response = {
            "Items": [test_feature_1],
            "LastEvaluatedKey": {
                "hash_key": test_feature_1["hash_key"],
                "range_key": test_feature_1["range_key"],
            },
        }

        page_2_params = {
            "TableName": "TEST-TABLE-NAME",
            "KeyConditionExpression": ANY,
            "ExclusiveStartKey": ANY,
        }
        page_2_response = {"Items": [test_feature_2]}

        ddb_stubber.add_response("query", page_1_response, page_1_params)
        ddb_stubber.add_response("query", page_2_response, page_2_params)

        result = feature_table.get_all_features(
            "e7a8b923-c0e6-4f6b-9198-24382b5df226:s3://spacenet-dataset/AOIs/AOI_1_Rio/srcData/mosaic_3band/013022223131.tif"  # noqa: E501
        )
        ddb_stubber.assert_no_pending_responses()
        assert len(result) == 4


@pytest.fixture
def test_feature_list() -> List[geojson.Feature]:
    with open("./test/data/detections.geojson", "r") as geojson_file:
        sample_features = geojson.load(geojson_file)["features"]
    return sample_features


def test_group_features_by_key(test_feature_list):
    from aws_oversightml_model_runner.ddb.feature_table import FeatureTable

    feature_table = FeatureTable("TEST-TABLE-NAME", (1024, 1024), (100, 100))
    features_dict = feature_table.group_features_by_key(test_feature_list)
    assert len(features_dict) == 2  # there are 2 keys


def test_generate_tile_key(test_feature_list):
    from aws_oversightml_model_runner.ddb.feature_table import FeatureTable

    feature_table = FeatureTable("TEST-TABLE-NAME", (1024, 1024), (100, 100))
    result = feature_table.generate_tile_key(test_feature_list[0])
    image_id = "7db12549-3bcb-49c8-acba-25d46ef5cbf3:s3://spacenet-dataset/AOIs/AOI_1_Rio/srcData/mosaic_3band/013022223131.tif-region-0:0:0:0"  # noqa: E501
    assert result == image_id


def test_add_features(test_feature_list):
    from aws_oversightml_model_runner.ddb.feature_table import FeatureTable

    feature_table = FeatureTable("TEST-TABLE-NAME", (1024, 1024), (100, 100))
    feature_table.add_features(test_feature_list)


test_feature_1 = {
    "hash_key": {
        "S": "e7a8b923-c0e6-4f6b-9198-24382b5df226:s3://spacenet-dataset/AOIs/AOI_1_Rio/srcData/mosaic_3band/013022223131.tif"  # noqa: E501
    },
    "range_key": {"S": "0:0:0:0"},
    "features": {
        "L": [
            {
                "S": '{"type": "Feature", "id": "96128a11-2e46-47b8-a33b-55ce8150a455", "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "properties": {"bounds_imcoords": [429, 553, 440, 561], "feature_types": {"ground_motor_passenger_vehicle": 0.2961518466472626}, "detection_score": 0.2961518466472626, "image_id": "e7a8b923-c0e6-4f6b-9198-24382b5df226:s3://spacenet-dataset/AOIs/AOI_1_Rio/srcData/mosaic_3band/013022223131.tif"}}'  # noqa: E501
            },
            {
                "S": '{"type": "Feature", "id": "fb033f8b-aefe-40a1-a56c-dc42e494477b", "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "properties": {"bounds_imcoords": [414, 505, 423, 515], "feature_types": {"ground_motor_passenger_vehicle": 0.2887503504753113}, "detection_score": 0.2887503504753113, "image_id": "e7a8b923-c0e6-4f6b-9198-24382b5df226:s3://spacenet-dataset/AOIs/AOI_1_Rio/srcData/mosaic_3band/013022223131.tif"}}'  # noqa: E501
            },
            {
                "S": '{"type": "Feature", "id": "0c4970ba-228d-487b-a29a-71ed97adbd89", "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "properties": {"bounds_imcoords": [664, 597, 674, 607], "feature_types": {"ground_motor_passenger_vehicle": 0.27162906527519226}, "detection_score": 0.27162906527519226, "image_id": "e7a8b923-c0e6-4f6b-9198-24382b5df226:s3://spacenet-dataset/AOIs/AOI_1_Rio/srcData/mosaic_3band/013022223131.tif"}}'  # noqa: E501
            },
        ]
    },
}

test_feature_2 = {
    "hash_key": {
        "S": "e7a8b923-c0e6-4f6b-9198-24382b5df226:s3://spacenet-dataset/AOIs/AOI_1_Rio/srcData/mosaic_3band/013022223131.tif"  # noqa: E501
    },
    "range_key": {"S": "0:0:1:1"},
    "features": {
        "L": [
            {
                "S": '{"type": "Feature", "id": "26c28104-4d3f-4595-b252-cb2af1dfff4b", "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "properties": {"bounds_imcoords": [646, 1649, 654, 1658], "feature_types": {"ground_motor_passenger_vehicle": 0.25180014967918396}, "detection_score": 0.25180014967918396, "image_id": "e7a8b923-c0e6-4f6b-9198-24382b5df226:s3://spacenet-dataset/AOIs/AOI_1_Rio/srcData/mosaic_3band/013022223131.tif"}}'  # noqa: E501
            }
        ]
    },
}
