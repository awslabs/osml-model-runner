import mock
from botocore.stub import Stubber, ANY

from aws_oversightml_model_runner.feature_table import FeatureTable
from aws_oversightml_model_runner.metrics import configure_metrics

configure_metrics("test", "stdout")


@mock.patch.dict("os.environ", {"AWS_DEFAULT_REGION": "us-east-1"})
def test_get_all_features_paginated():
    feature_table = FeatureTable("TEST-TABLE-NAME", [1024, 1024], [100, 100])

    with Stubber(feature_table.ddb_feature_table.meta.client) as ddb_stubber:
        page_1_params = {
            'TableName': 'TEST-TABLE-NAME',
            'KeyConditionExpression': ANY,
        }
        page_1_response = {
            'Items': [
                test_feature_1
            ],
            'LastEvaluatedKey': {
                'hash_key': test_feature_1['hash_key'],
                'range_key': test_feature_1['range_key']
            }
        }

        page_2_params = {
            'TableName': 'TEST-TABLE-NAME',
            'KeyConditionExpression': ANY,
            'ExclusiveStartKey': ANY
        }
        page_2_response = {
            'Items': [
                test_feature_2
            ]
        }

        ddb_stubber.add_response('query', page_1_response, page_1_params)
        ddb_stubber.add_response('query', page_2_response, page_2_params)

        result = feature_table.get_all_features(
            "e7a8b923-c0e6-4f6b-9198-24382b5df226:s3://spacenet-dataset/AOIs/AOI_1_Rio/srcData/mosaic_3band/013022223131.tif")
        ddb_stubber.assert_no_pending_responses()
        assert len(result) == 4


test_feature_1 = {
    "hash_key": {
        "S": "e7a8b923-c0e6-4f6b-9198-24382b5df226:s3://spacenet-dataset/AOIs/AOI_1_Rio/srcData/mosaic_3band/013022223131.tif"
    },
    "range_key": {
        "S": "0:0:0:0"
    },
    "features": {
        "L": [
            {
                "S": "{\"type\": \"Feature\", \"id\": \"96128a11-2e46-47b8-a33b-55ce8150a455\", \"geometry\": {\"type\": \"Point\", \"coordinates\": [0.0, 0.0]}, \"properties\": {\"bounds_imcoords\": [429, 553, 440, 561], \"feature_types\": {\"ground_motor_passenger_vehicle\": 0.2961518466472626}, \"detection_score\": 0.2961518466472626, \"image_id\": \"e7a8b923-c0e6-4f6b-9198-24382b5df226:s3://spacenet-dataset/AOIs/AOI_1_Rio/srcData/mosaic_3band/013022223131.tif\"}}"
            },
            {
                "S": "{\"type\": \"Feature\", \"id\": \"fb033f8b-aefe-40a1-a56c-dc42e494477b\", \"geometry\": {\"type\": \"Point\", \"coordinates\": [0.0, 0.0]}, \"properties\": {\"bounds_imcoords\": [414, 505, 423, 515], \"feature_types\": {\"ground_motor_passenger_vehicle\": 0.2887503504753113}, \"detection_score\": 0.2887503504753113, \"image_id\": \"e7a8b923-c0e6-4f6b-9198-24382b5df226:s3://spacenet-dataset/AOIs/AOI_1_Rio/srcData/mosaic_3band/013022223131.tif\"}}"
            },
            {
                "S": "{\"type\": \"Feature\", \"id\": \"0c4970ba-228d-487b-a29a-71ed97adbd89\", \"geometry\": {\"type\": \"Point\", \"coordinates\": [0.0, 0.0]}, \"properties\": {\"bounds_imcoords\": [664, 597, 674, 607], \"feature_types\": {\"ground_motor_passenger_vehicle\": 0.27162906527519226}, \"detection_score\": 0.27162906527519226, \"image_id\": \"e7a8b923-c0e6-4f6b-9198-24382b5df226:s3://spacenet-dataset/AOIs/AOI_1_Rio/srcData/mosaic_3band/013022223131.tif\"}}"
            }
        ]
    }
}

test_feature_2 = {
    "hash_key": {
        "S": "e7a8b923-c0e6-4f6b-9198-24382b5df226:s3://spacenet-dataset/AOIs/AOI_1_Rio/srcData/mosaic_3band/013022223131.tif"
    },
    "range_key": {
        "S": "0:0:1:1"
    },
    "features": {
        "L": [
            {
                "S": "{\"type\": \"Feature\", \"id\": \"26c28104-4d3f-4595-b252-cb2af1dfff4b\", \"geometry\": {\"type\": \"Point\", \"coordinates\": [0.0, 0.0]}, \"properties\": {\"bounds_imcoords\": [646, 1649, 654, 1658], \"feature_types\": {\"ground_motor_passenger_vehicle\": 0.25180014967918396}, \"detection_score\": 0.25180014967918396, \"image_id\": \"e7a8b923-c0e6-4f6b-9198-24382b5df226:s3://spacenet-dataset/AOIs/AOI_1_Rio/srcData/mosaic_3band/013022223131.tif\"}}"
            }
        ]
    }
}
