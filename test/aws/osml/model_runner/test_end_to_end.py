#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

import os
import sys
from importlib import reload
from pathlib import Path
from unittest.mock import Mock, patch

import boto3
import geojson
import pytest
from botocore.stub import ANY, Stubber
from moto import mock_aws
from osgeo import gdal

from aws.osml.model_runner.database import RequestedJobsTable
from config import MOCK_MODEL_RESPONSE, TEST_CONFIG

# Add the test directory to sys.path to allow importing config
_test_dir = Path(__file__).parent
if str(_test_dir) not in sys.path:
    sys.path.insert(0, str(_test_dir))


@pytest.fixture
def end_to_end_setup():
    """
    Set up virtual AWS resources for use in unit tests.
    Creates DynamoDB tables, S3 buckets, SNS topics, SQS queues, and
    mock SageMaker endpoints required for the tests.
    """
    from aws.osml.model_runner.api import RegionRequest
    from aws.osml.model_runner.api.image_request import ImageRequest
    from aws.osml.model_runner.app_config import BotoConfig
    from aws.osml.model_runner.database.feature_table import FeatureTable
    from aws.osml.model_runner.database.image_request_table import ImageRequestTable
    from aws.osml.model_runner.database.region_request_table import RegionRequestTable
    from aws.osml.model_runner.model_runner import ModelRunner
    from aws.osml.model_runner.status import ImageStatusMonitor, RegionStatusMonitor

    with mock_aws():
        # Required to avoid warnings from GDAL
        gdal.DontUseExceptions()

        # Set default custom feature properties that should exist on the output features
        test_custom_feature_properties = {
            "modelMetadata": {
                "modelName": "test-model-name",
                "ontologyName": "test-ontology--name",
                "ontologyVersion": "test-ontology-version",
                "classification": "test-classification",
            }
        }

        # Set default feature properties that should exist on the output features
        test_feature_source_property = [
            {
                "location": TEST_CONFIG["IMAGE_FILE"],
                "format": "NITF",
                "category": "VIS",
                "sourceId": "Checks an uncompressed 1024x1024 8 bit mono image with GEOcentric data. Airfield",
                "sourceDT": "1996-12-17T10:26:30Z",
            }
        ]

        # Build a mock region request for testing
        region_request = RegionRequest(
            {
                "tile_size": (10, 10),
                "tile_overlap": (1, 1),
                "tile_format": "NITF",
                "image_id": TEST_CONFIG["IMAGE_ID"],
                "image_url": TEST_CONFIG["IMAGE_FILE"],
                "region_bounds": ((0, 0), (50, 50)),
                "model_name": TEST_CONFIG["MODEL_ENDPOINT"],
                "model_invoke_mode": "SM_ENDPOINT",
                "image_extension": TEST_CONFIG["IMAGE_EXTENSION"],
            }
        )

        # Build a mock image request for testing
        image_request = ImageRequest.from_external_message(
            {
                "jobName": TEST_CONFIG["IMAGE_ID"],
                "jobId": TEST_CONFIG["JOB_ID"],
                "imageUrls": [TEST_CONFIG["IMAGE_FILE"]],
                "outputs": [
                    {"type": "S3", "bucket": TEST_CONFIG["RESULTS_BUCKET"], "prefix": f"{TEST_CONFIG['IMAGE_ID']}"},
                    {"type": "Kinesis", "stream": TEST_CONFIG["RESULTS_STREAM"], "batchSize": 1000},
                ],
                "featureProperties": [test_custom_feature_properties],
                "imageProcessor": {"name": TEST_CONFIG["MODEL_ENDPOINT"], "type": "SM_ENDPOINT"},
                "imageProcessorParameters": {"TargetVariant": TEST_CONFIG["MODEL_VARIANT"]},
                "imageProcessorTileSize": 2048,
                "imageProcessorTileOverlap": 50,
                "imageProcessorTileFormat": "NITF",
                "imageProcessorTileCompression": "JPEG",
                "randomKey": "random-value",
            }
        )

        # Build the required virtual DDB tables
        ddb = boto3.resource("dynamodb", config=BotoConfig.default)

        image_request_ddb = ddb.create_table(
            TableName=os.environ["IMAGE_REQUEST_TABLE"],
            KeySchema=TEST_CONFIG["IMAGE_REQUEST_TABLE_KEY_SCHEMA"],
            AttributeDefinitions=TEST_CONFIG["IMAGE_REQUEST_TABLE_ATTRIBUTE_DEFINITIONS"],
            BillingMode="PAY_PER_REQUEST",
        )
        image_request_table = ImageRequestTable(os.environ["IMAGE_REQUEST_TABLE"])

        outstanding_jobs_table_ddb = ddb.create_table(
            TableName=os.environ["OUTSTANDING_IMAGE_REQUEST_TABLE"],
            KeySchema=TEST_CONFIG["OUTSTANDING_IMAGE_REQUEST_TABLE_KEY_SCHEMA"],
            AttributeDefinitions=TEST_CONFIG["OUTSTANDING_IMAGE_REQUEST_TABLE_ATTRIBUTE_DEFINITIONS"],
            BillingMode="PAY_PER_REQUEST",
        )
        outstanding_jobs_table = RequestedJobsTable(os.environ["OUTSTANDING_IMAGE_REQUEST_TABLE"])
        outstanding_jobs_table.add_new_request(image_request)

        region_request_ddb = ddb.create_table(
            TableName=os.environ["REGION_REQUEST_TABLE"],
            KeySchema=TEST_CONFIG["REGION_REQUEST_TABLE_KEY_SCHEMA"],
            AttributeDefinitions=TEST_CONFIG["REGION_REQUEST_TABLE_ATTRIBUTE_DEFINITIONS"],
            BillingMode="PAY_PER_REQUEST",
        )
        region_request_table = RegionRequestTable(os.environ["REGION_REQUEST_TABLE"])

        feature_ddb = ddb.create_table(
            TableName=os.environ["FEATURE_TABLE"],
            KeySchema=TEST_CONFIG["FEATURE_TABLE_KEY_SCHEMA"],
            AttributeDefinitions=TEST_CONFIG["FEATURE_TABLE_ATTRIBUTE_DEFINITIONS"],
            BillingMode="PAY_PER_REQUEST",
        )
        feature_table = FeatureTable(
            os.environ["FEATURE_TABLE"],
            image_request.tile_size,
            image_request.tile_overlap,
        )

        # Build a virtual S3 and Kinesis output sink
        s3 = boto3.client("s3", config=BotoConfig.default)

        s3.create_bucket(
            Bucket=TEST_CONFIG["IMAGE_BUCKET"],
            CreateBucketConfiguration={"LocationConstraint": os.environ["AWS_DEFAULT_REGION"]},
        )

        with open(TEST_CONFIG["IMAGE_FILE"], "rb") as data:
            s3.upload_fileobj(data, TEST_CONFIG["IMAGE_BUCKET"], TEST_CONFIG["IMAGE_KEY"])

        s3.create_bucket(
            Bucket=TEST_CONFIG["RESULTS_BUCKET"],
            CreateBucketConfiguration={"LocationConstraint": os.environ["AWS_DEFAULT_REGION"]},
        )

        kinesis = boto3.client("kinesis", config=BotoConfig.default)
        kinesis.create_stream(StreamName=TEST_CONFIG["RESULTS_STREAM"], StreamModeDetails={"StreamMode": "ON_DEMAND"})

        # Build a virtual image status topic and queue
        sns = boto3.client("sns", config=BotoConfig.default)
        image_status_topic_arn = sns.create_topic(Name=os.environ["IMAGE_STATUS_TOPIC"]).get("TopicArn")

        sqs = boto3.client("sqs", config=BotoConfig.default)
        image_status_queue_url = sqs.create_queue(QueueName="mock_queue").get("QueueUrl")
        image_status_queue_attributes = sqs.get_queue_attributes(
            QueueUrl=image_status_queue_url, AttributeNames=["QueueArn"]
        )
        image_status_queue_arn = image_status_queue_attributes.get("Attributes").get("QueueArn")

        sns.subscribe(TopicArn=image_status_topic_arn, Protocol="sqs", Endpoint=image_status_queue_arn)
        image_status_monitor = ImageStatusMonitor(image_status_topic_arn)

        # Build a virtual region status topic and queue
        region_status_topic_arn = sns.create_topic(Name=os.environ["REGION_STATUS_TOPIC"]).get("TopicArn")
        region_status_monitor = RegionStatusMonitor(region_status_topic_arn)

        region_status_queue_url = sqs.create_queue(QueueName="mock_region_queue").get("QueueUrl")
        region_status_queue_attributes = sqs.get_queue_attributes(
            QueueUrl=region_status_queue_url, AttributeNames=["QueueArn"]
        )
        region_status_queue_arn = region_status_queue_attributes.get("Attributes").get("QueueArn")

        sns.subscribe(TopicArn=region_status_topic_arn, Protocol="sqs", Endpoint=region_status_queue_arn)

        # Build a virtual SageMaker endpoint
        sm = boto3.client("sagemaker", config=BotoConfig.default)
        sm.create_model(
            ModelName=TEST_CONFIG["MODEL_NAME"],
            PrimaryContainer=TEST_CONFIG["SM_MODEL_CONTAINER"],
            ExecutionRoleArn=f"arn:aws:iam::{TEST_CONFIG['ACCOUNT_ID']}:role/FakeRole",
        )

        config_name = "TestConfig"
        production_variants = TEST_CONFIG["ENDPOINT_PRODUCTION_VARIANTS"]
        sm.create_endpoint_config(EndpointConfigName=config_name, ProductionVariants=production_variants)
        sm.create_endpoint(EndpointName=TEST_CONFIG["MODEL_ENDPOINT"], EndpointConfigName=config_name)

        # Plug in the required virtual resources to our ModelRunner instance
        model_runner = ModelRunner()
        model_runner.image_request_table = image_request_table
        model_runner.requested_jobs_table = outstanding_jobs_table
        model_runner.image_job_scheduler.image_request_queue.requested_jobs_table = outstanding_jobs_table
        model_runner.region_request_table = region_request_table
        model_runner.image_status_monitor = image_status_monitor
        model_runner.region_status_monitor = region_status_monitor
        model_runner.region_request_handler.image_request_table = image_request_table
        model_runner.region_request_handler.region_request_table = region_request_table
        model_runner.region_request_handler.region_status_monitor = region_status_monitor
        model_runner.region_request_handler.image_request_table = image_request_table
        model_runner.image_request_handler.region_request_table = region_request_table
        model_runner.image_request_handler.region_request_handler = model_runner.region_request_handler
        model_runner.image_request_handler.image_request_table = image_request_table
        model_runner.image_request_handler.image_status_monitor = image_status_monitor

        yield (
            model_runner,
            image_request_table,
            feature_table,
            s3,
            kinesis,
            region_request,
            image_request,
            test_custom_feature_properties,
            test_feature_source_property,
        )

        # Cleanup
        image_request_ddb.delete()
        feature_ddb.delete()
        region_request_ddb.delete()
        outstanding_jobs_table_ddb.delete()


def test_aws_osml_model_runner_importable():
    """
    Ensure that aws.osml.model_runner can be imported without errors.
    """
    import aws.osml.model_runner  # noqa: F401


def test_run(end_to_end_setup):
    """
    Test that the run method in ModelRunner initiates the work queue monitoring process.
    """
    (
        model_runner,
        image_request_table,
        feature_table,
        s3,
        kinesis,
        region_request,
        image_request,
        test_custom_feature_properties,
        test_feature_source_property,
    ) = end_to_end_setup

    model_runner.monitor_work_queues = Mock()
    model_runner.run()
    model_runner.monitor_work_queues.assert_called_once()


def test_stop(end_to_end_setup):
    """
    Test that the stop method stops the ModelRunner.
    """
    (
        model_runner,
        image_request_table,
        feature_table,
        s3,
        kinesis,
        region_request,
        image_request,
        test_custom_feature_properties,
        test_feature_source_property,
    ) = end_to_end_setup

    model_runner.running = True
    model_runner.stop()
    assert model_runner.running is False


@patch("aws.osml.model_runner.inference.sm_detector.boto3")
def test_end_to_end(mock_boto3, end_to_end_setup):
    """
    Test the process of handling an image request, ensuring that jobs are marked as complete,
    features are created, and the correct metadata is stored in S3. Checks that we calculated
    the max in progress regions with the test instance type is set to m5.12xl with 48 vcpus.
    """
    (
        model_runner,
        image_request_table,
        feature_table,
        s3,
        kinesis,
        region_request,
        image_request,
        test_custom_feature_properties,
        test_feature_source_property,
    ) = end_to_end_setup

    # Build stubbed model client for ModelRunner to interact with
    sm_runtime_client = get_stubbed_sm_boto_client()
    mock_boto3.client.return_value = sm_runtime_client
    model_runner.image_request_handler.process_image_request(image_request)

    # Ensure that the single region was processed successfully
    image_request_item = image_request_table.get_image_request(image_request.image_id)
    assert image_request_item.region_success == 1

    # Ensure that the detection outputs arrived in our DDB table
    features = feature_table.get_features(image_request.image_id)
    assert len(features) == 1
    assert features[0]["geometry"]["type"] == "Polygon"

    # Ensure that the detection outputs arrived in our output bucket
    results_key = s3.list_objects(Bucket=TEST_CONFIG["RESULTS_BUCKET"])["Contents"][0]["Key"]
    results_contents = s3.get_object(Bucket=TEST_CONFIG["RESULTS_BUCKET"], Key=results_key)["Body"].read()
    results_features = geojson.loads(results_contents.decode("utf-8"))["features"]
    assert len(results_features) > 0

    # Test that we get the correct model metadata appended to our feature outputs
    actual_model_metadata = results_features[0]["properties"]["modelMetadata"]
    expected_model_metadata = test_custom_feature_properties.get("modelMetadata")
    assert actual_model_metadata == expected_model_metadata

    # Test that we get the correct source metadata appended to our feature outputs
    actual_source_metadata = results_features[0]["properties"]["sourceMetadata"]
    expected_source_metadata = test_feature_source_property
    assert actual_source_metadata == expected_source_metadata


@patch.dict("os.environ", values={"ELEVATION_DATA_LOCATION": TEST_CONFIG["ELEVATION_DATA_LOCATION"]})
def test_create_elevation_model():
    """
    Test that the ModelRunner correctly creates an elevation model based on the SRTM DEM tile set.
    The import and reload statements are necessary to force the ServiceConfig to update with the
    patched environment variables.
    """
    import aws.osml.model_runner.app_config

    reload(aws.osml.model_runner.app_config)
    from aws.osml.gdal.gdal_dem_tile_factory import GDALDigitalElevationModelTileFactory
    from aws.osml.model_runner.app_config import ServiceConfig
    from aws.osml.photogrammetry.digital_elevation_model import DigitalElevationModel
    from aws.osml.photogrammetry.srtm_dem_tile_set import SRTMTileSet

    assert ServiceConfig.elevation_data_location == TEST_CONFIG["ELEVATION_DATA_LOCATION"]
    config = ServiceConfig()
    elevation_model = config.create_elevation_model()
    assert elevation_model
    assert isinstance(elevation_model, DigitalElevationModel)
    assert isinstance(elevation_model.tile_set, SRTMTileSet)
    assert isinstance(elevation_model.tile_factory, GDALDigitalElevationModelTileFactory)

    assert elevation_model.tile_set.format_extension == ".tif"
    assert elevation_model.tile_set.prefix == ""
    assert elevation_model.tile_set.version == "1arc_v3"

    assert elevation_model.tile_factory.tile_directory == TEST_CONFIG["ELEVATION_DATA_LOCATION"]


def test_create_elevation_model_disabled():
    """
    Test that no elevation model is created when ELEVATION_DATA_LOCATION is not set in the environment.
    The import and reload statements are necessary to force the ServiceConfig to update with the
    patched environment variables.
    """
    import aws.osml.model_runner.app_config

    reload(aws.osml.model_runner.app_config)
    from aws.osml.model_runner.app_config import ServiceConfig

    assert ServiceConfig.elevation_data_location is None
    config = ServiceConfig()
    elevation_model = config.create_elevation_model()

    assert not elevation_model


def get_stubbed_sm_boto_client() -> boto3.client:
    """
    Get stubbed SageMaker client for use in testing.

    :return: A stubbed SageMaker Runtime client.
    """
    expected_sm_runtime_calls = 1
    # Create and stub the SageMaker runtime client
    sm_runtime_client = boto3.client("sagemaker-runtime")
    sm_runtime_stub = Stubber(sm_runtime_client)
    for _ in range(expected_sm_runtime_calls):
        sm_runtime_stub.add_response(
            "invoke_endpoint",
            expected_params={"EndpointName": TEST_CONFIG["MODEL_ENDPOINT"], "Body": ANY, "TargetVariant": "variant1"},
            service_response=MOCK_MODEL_RESPONSE,
        )
    sm_runtime_stub.activate()

    return sm_runtime_client
