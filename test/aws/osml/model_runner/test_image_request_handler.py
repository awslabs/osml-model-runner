#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.
import json
from collections import Counter
from contextlib import nullcontext
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import boto3
import pytest
from aws_embedded_metrics import MetricsLogger
from botocore.stub import Stubber

from aws.osml.model_runner.api import ImageRequest, ModelInvokeMode
from aws.osml.model_runner.app_config import ServiceConfig
from aws.osml.model_runner.common import RequestStatus
from aws.osml.model_runner.database import ImageRequestItem, ImageRequestTable, RegionRequestTable
from aws.osml.model_runner.exceptions import (
    AggregateFeaturesException,
    AggregateOutputFeaturesException,
    LoadImageException,
    ProcessImageException,
    UnsupportedModelException,
)
from aws.osml.model_runner.image_request_handler import ImageRequestHandler
from aws.osml.model_runner.scheduler import RequestQueue
from aws.osml.model_runner.status import ImageStatusMonitor
from aws.osml.model_runner.tile_worker import TilingStrategy

MOCK_DESCRIBE_ENDPOINT_RESPONSE = {
    "EndpointName": "test-model-name",
    "EndpointArn": "arn:aws:sagemaker:region:account:endpoint/test-model-name",
    "EndpointConfigName": "test-config",
    "ProductionVariants": [{"VariantName": "variant1", "CurrentWeight": 1.0}],
    "EndpointStatus": "InService",
    "CreationTime": datetime(2025, 1, 1),
    "LastModifiedTime": datetime(2025, 1, 1),
}


@pytest.fixture
def handler_setup():
    # Mock dependencies
    mock_image_request_table = MagicMock(spec=ImageRequestTable)
    mock_image_status_monitor = MagicMock(spec=ImageStatusMonitor)
    mock_tiling_strategy = MagicMock(spec=TilingStrategy)
    mock_region_request_queue = MagicMock(spec=RequestQueue)
    mock_region_request_table = MagicMock(spec=RegionRequestTable)
    mock_config = MagicMock(spec=ServiceConfig)
    mock_config.region_size = "(256, 256)"

    # Set up config properties

    # Instantiate the handler with mocked dependencies
    handler = ImageRequestHandler(
        image_request_table=mock_image_request_table,
        image_status_monitor=mock_image_status_monitor,
        tiling_strategy=mock_tiling_strategy,
        region_request_queue=mock_region_request_queue,
        region_request_table=mock_region_request_table,
        config=mock_config,
        region_request_handler=MagicMock(),
    )

    # Mock request and items
    mock_image_request = ImageRequest.from_external_message(
        {
            "jobName": "test-job-name",
            "jobId": "test-job-id",
            "imageUrls": ["./test/data/small.ntf"],
            "outputs": [
                {"type": "S3", "bucket": "test-results-bucket", "prefix": "test-image-id"},
                {"type": "Kinesis", "stream": ":test-results-stream", "batchSize": 1000},
            ],
            "imageProcessor": {"name": "test-model", "type": "SM_ENDPOINT"},
            "imageProcessorTileSize": 2048,
            "imageProcessorTileOverlap": 50,
            "imageProcessorTileFormat": "NITF",
            "imageProcessorTileCompression": "JPEG",
            "randomKey": "random-value",
        }
    )

    mock_image_request_item = ImageRequestItem.from_image_request(mock_image_request)

    # Create and stub the SageMaker client
    sm_client = boto3.client("sagemaker")
    sm_client_stub = Stubber(sm_client)

    # Patch boto3.client to return our stubbed client
    boto3_patcher = patch("boto3.client")
    mock_boto3_client = boto3_patcher.start()
    mock_boto3_client.return_value = sm_client

    yield {
        "handler": handler,
        "mock_image_request_table": mock_image_request_table,
        "mock_image_status_monitor": mock_image_status_monitor,
        "mock_tiling_strategy": mock_tiling_strategy,
        "mock_region_request_queue": mock_region_request_queue,
        "mock_region_request_table": mock_region_request_table,
        "mock_config": mock_config,
        "mock_image_request": mock_image_request,
        "mock_image_request_item": mock_image_request_item,
        "sm_client": sm_client,
        "sm_client_stub": sm_client_stub,
    }

    sm_client_stub.deactivate()
    boto3_patcher.stop()


def test_process_image_request_success(handler_setup):
    """
    Test successful image request processing.
    """
    handler = handler_setup["handler"]
    mock_image_request_table = handler_setup["mock_image_request_table"]
    mock_image_status_monitor = handler_setup["mock_image_status_monitor"]
    mock_image_request = handler_setup["mock_image_request"]

    # Mock internal methods
    handler.load_image_request = MagicMock(return_value=("tif", MagicMock(), MagicMock(), [MagicMock()]))
    handler.queue_region_request = MagicMock()

    handler.set_default_model_endpoint_variant = MagicMock(return_value=mock_image_request)

    # Call process_image_request
    handler.process_image_request(mock_image_request)

    # Assert that the STARTED status was called first
    mock_image_request_table.start_image_request.assert_called_once()

    # Ensure the regions were queued
    handler.queue_region_request.assert_called_once()

    # Ensure processing events were emitted
    assert mock_image_status_monitor.process_event.call_count == 2


@patch("aws.osml.model_runner.image_request_handler.get_source_property")
def test_process_image_request_appends_source_metadata(mock_get_source_property, handler_setup):
    """
    Test processing adds source metadata to feature properties.
    """
    handler = handler_setup["handler"]
    mock_image_request_table = handler_setup["mock_image_request_table"]
    mock_image_request = handler_setup["mock_image_request"]

    mock_get_source_property.return_value = {"source": "metadata"}
    raster_dataset = MagicMock()
    raster_dataset.RasterXSize = 2048
    raster_dataset.RasterYSize = 1024
    handler.load_image_request = MagicMock(return_value=("tif", raster_dataset, MagicMock(), [MagicMock()]))
    handler.queue_region_request = MagicMock()
    handler.set_default_model_endpoint_variant = MagicMock(return_value=mock_image_request)

    handler.process_image_request(mock_image_request)

    updated_item = mock_image_request_table.update_image_request.call_args[0][0]
    feature_properties = json.loads(updated_item.feature_properties)
    assert {"source": "metadata"} in feature_properties


@patch("aws.osml.model_runner.image_request_handler.get_source_property", return_value=None)
def test_process_image_request_without_distillation_option(_mock_get_source_property, handler_setup):
    """
    Test processing skips feature distillation when not configured.
    """
    handler = handler_setup["handler"]
    mock_image_request_table = handler_setup["mock_image_request_table"]

    image_request = _build_request_data()
    image_request.post_processing = []
    raster_dataset = MagicMock()
    raster_dataset.RasterXSize = 2048
    raster_dataset.RasterYSize = 1024
    handler.load_image_request = MagicMock(return_value=("tif", raster_dataset, MagicMock(), [MagicMock()]))
    handler.queue_region_request = MagicMock()
    handler.set_default_model_endpoint_variant = MagicMock(return_value=image_request)

    handler.process_image_request(image_request)

    started_item = mock_image_request_table.start_image_request.call_args[0][0]
    assert started_item.feature_distillation_option is None


def test_process_image_request_skips_when_missing_regions(handler_setup):
    """
    Test processing skips queueing when no regions are available.
    """
    handler = handler_setup["handler"]
    mock_image_request_table = handler_setup["mock_image_request_table"]
    mock_image_request = handler_setup["mock_image_request"]

    handler.load_image_request = MagicMock(return_value=(None, None, None, None))
    handler.queue_region_request = MagicMock()
    handler.set_default_model_endpoint_variant = MagicMock(return_value=mock_image_request)

    handler.process_image_request(mock_image_request)

    handler.queue_region_request.assert_not_called()
    mock_image_request_table.update_image_request.assert_not_called()


def test_process_image_request_failure_calls_fail(handler_setup):
    """
    Test failure path calls fail_image_request for existing item.
    """
    handler = handler_setup["handler"]
    mock_image_status_monitor = handler_setup["mock_image_status_monitor"]
    mock_image_request = handler_setup["mock_image_request"]

    handler.load_image_request = MagicMock(side_effect=Exception("Test error"))
    handler.fail_image_request = MagicMock()
    handler.set_default_model_endpoint_variant = MagicMock(return_value=mock_image_request)

    with pytest.raises(ProcessImageException):
        handler.process_image_request(mock_image_request)

    mock_image_status_monitor.process_event.assert_called()
    handler.fail_image_request.assert_called_once()


def test_process_image_request_failure_before_item_created(handler_setup):
    """
    Test failure path uses minimal item when setup fails early.
    """
    handler = handler_setup["handler"]
    mock_image_request = handler_setup["mock_image_request"]

    handler.set_default_model_endpoint_variant = MagicMock(side_effect=Exception("Test error"))
    handler.fail_image_request = MagicMock()

    with pytest.raises(ProcessImageException):
        handler.process_image_request(mock_image_request)

    failed_item = handler.fail_image_request.call_args[0][0]
    assert failed_item.image_id == mock_image_request.image_id
    assert failed_item.job_id == mock_image_request.job_id


@patch("aws.osml.model_runner.image_request_handler.SinkFactory.sink_features")
@patch("aws.osml.model_runner.image_request_handler.ImageRequestHandler.deduplicate")
@patch("aws.osml.model_runner.image_request_handler.FeatureTable.aggregate_features")
def test_complete_image_request(mock_aggregate_features, mock_deduplicate, mock_sink_features, handler_setup):
    """
    Test successful completion of image request.
    """
    handler = handler_setup["handler"]
    mock_image_request_table = handler_setup["mock_image_request_table"]
    mock_image_status_monitor = handler_setup["mock_image_status_monitor"]
    mock_image_request_item = handler_setup["mock_image_request_item"]

    # Set up mock return values for our ImageRequestItem to complete
    mock_image_request_table.get_image_request.return_value = mock_image_request_item
    mock_image_request_item.processing_duration = 1000
    mock_image_request_item.region_error = 0

    # Set up mock return values for RegionRequest to complete
    mock_region_request = MagicMock()
    mock_raster_dataset = MagicMock()
    mock_sensor_model = MagicMock()
    mock_features = [
        {
            "type": "Feature",
            "properties": {
                "inferenceTime": datetime.now(tz=timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
            },
            "geometry": {"type": "Point", "coordinates": [-77.0364761352539, 38.89761287129639]},
        }
    ]
    mock_deduplicate.return_value = mock_features
    mock_aggregate_features.return_value = mock_features
    mock_sink_features.return_value = True

    # Call complete_image_request
    handler.complete_image_request(mock_region_request, "tif", mock_raster_dataset, mock_sensor_model)

    # Ensure sink_features was called correctly
    mock_sink_features.assert_called_once()

    # Ensure failure handling methods were called
    mock_image_status_monitor.process_event.assert_called()


@patch("aws.osml.model_runner.image_request_handler.FeatureTable.aggregate_features", side_effect=Exception("boom"))
def test_complete_image_request_raises(_mock_aggregate, handler_setup):
    """
    Test completion wraps aggregation errors.
    """
    handler = handler_setup["handler"]
    mock_image_request_table = handler_setup["mock_image_request_table"]
    mock_image_request_item = handler_setup["mock_image_request_item"]

    mock_image_request_table.get_image_request.return_value = mock_image_request_item
    with pytest.raises(AggregateFeaturesException):
        handler.complete_image_request(MagicMock(), "tif", MagicMock(), MagicMock())


def test_fail_image_request(handler_setup):
    """
    Test fail_image_request method behavior.
    """
    handler = handler_setup["handler"]
    mock_image_request_table = handler_setup["mock_image_request_table"]
    mock_image_status_monitor = handler_setup["mock_image_status_monitor"]
    mock_image_request_item = handler_setup["mock_image_request_item"]

    # Call fail_image_request
    handler.fail_image_request(mock_image_request_item, Exception("Test failure"))

    # Ensure status monitor was updated and job table was called
    mock_image_status_monitor.process_event.assert_called_once_with(
        mock_image_request_item, RequestStatus.FAILED, "Test failure"
    )
    mock_image_request_table.end_image_request.assert_called_once_with(mock_image_request_item.image_id)


def test_queue_region_request_processes_first_and_queues_rest(handler_setup):
    """
    Test queueing logic for regions and first region processing.
    """
    handler = handler_setup["handler"]
    mock_image_request_table = handler_setup["mock_image_request_table"]
    mock_region_request_queue = handler_setup["mock_region_request_queue"]
    mock_region_request_table = handler_setup["mock_region_request_table"]
    mock_image_request = handler_setup["mock_image_request"]

    regions = [((0, 0), (10, 10)), ((10, 0), (10, 10))]
    image_request_item = ImageRequestItem.from_image_request(mock_image_request)
    handler.region_request_handler.process_region_request = MagicMock(return_value=image_request_item)
    mock_image_request_table.is_image_request_complete.return_value = True
    handler.complete_image_request = MagicMock()

    handler.queue_region_request(regions.copy(), mock_image_request, MagicMock(), MagicMock(), "tif")

    mock_region_request_table.start_region_request.assert_called()
    mock_region_request_queue.send_request.assert_called_once()
    handler.region_request_handler.process_region_request.assert_called_once()
    handler.complete_image_request.assert_called_once()


def test_queue_region_request_does_not_complete_when_incomplete(handler_setup):
    """
    Test queueing logic does not complete when image not finished.
    """
    handler = handler_setup["handler"]
    mock_image_request_table = handler_setup["mock_image_request_table"]
    mock_image_request = handler_setup["mock_image_request"]

    regions = [((0, 0), (10, 10)), ((10, 0), (10, 10))]
    image_request_item = ImageRequestItem.from_image_request(mock_image_request)
    handler.region_request_handler.process_region_request = MagicMock(return_value=image_request_item)
    mock_image_request_table.is_image_request_complete.return_value = False
    handler.complete_image_request = MagicMock()

    handler.queue_region_request(regions.copy(), mock_image_request, MagicMock(), MagicMock(), "tif")

    handler.complete_image_request.assert_not_called()


@patch("aws.osml.model_runner.image_request_handler.calculate_processing_bounds")
@patch("aws.osml.model_runner.image_request_handler.get_image_extension")
@patch("aws.osml.model_runner.image_request_handler.load_gdal_dataset")
@patch("aws.osml.model_runner.image_request_handler.get_image_path")
@patch("aws.osml.model_runner.image_request_handler.GDALConfigEnv")
@patch("aws.osml.model_runner.image_request_handler.get_credentials_for_assumed_role")
def test_load_image_request_success_with_role(
    mock_get_credentials,
    mock_gdal_env,
    mock_get_image_path,
    mock_load_gdal,
    mock_get_extension,
    mock_processing_bounds,
    handler_setup,
):
    """
    Test load_image_request returns expected artifacts with assumed role.
    """
    handler = handler_setup["handler"]
    mock_tiling_strategy = handler_setup["mock_tiling_strategy"]
    mock_image_request_item = handler_setup["mock_image_request_item"]

    mock_get_credentials.return_value = {"AccessKeyId": "a", "SecretAccessKey": "b", "SessionToken": "c"}
    mock_get_image_path.return_value = "/vsis3/bucket/key"
    raster_dataset = MagicMock()
    sensor_model = MagicMock()
    mock_load_gdal.return_value = (raster_dataset, sensor_model)
    mock_get_extension.return_value = "tif"
    mock_processing_bounds.return_value = ((0, 0), (100, 100))
    mock_gdal_env.return_value.with_aws_credentials.return_value = nullcontext()
    mock_tiling_strategy.compute_regions.return_value = ["region"]
    mock_image_request_item.image_read_role = "arn:aws:iam::012345678910:role/TestRole"
    mock_image_request_item.tile_size = "(32, 32)"
    mock_image_request_item.tile_overlap = None

    extension, ds, model, regions = handler.load_image_request(mock_image_request_item, None)

    assert extension == "tif"
    assert ds is raster_dataset
    assert model is sensor_model
    assert regions == ["region"]
    mock_get_credentials.assert_called_once_with(mock_image_request_item.image_read_role)
    mock_tiling_strategy.compute_regions.assert_called_once_with(
        mock_processing_bounds.return_value,
        (256, 256),
        (32, 32),
        (0, 0),
    )


@patch("aws.osml.model_runner.image_request_handler.calculate_processing_bounds", return_value=((0, 0), (100, 100)))
@patch("aws.osml.model_runner.image_request_handler.get_image_extension", return_value="tif")
@patch("aws.osml.model_runner.image_request_handler.load_gdal_dataset", return_value=(MagicMock(), MagicMock()))
@patch("aws.osml.model_runner.image_request_handler.get_image_path", return_value="/vsis3/bucket/key")
@patch("aws.osml.model_runner.image_request_handler.GDALConfigEnv")
def test_load_image_request_with_overlap(
    _mock_gdal_env, _mock_get_image_path, _mock_load_gdal, _mock_get_extension, _mock_bounds, handler_setup
):
    """
    Test load_image_request uses explicit tile overlap.
    """
    handler = handler_setup["handler"]
    mock_tiling_strategy = handler_setup["mock_tiling_strategy"]
    mock_image_request_item = handler_setup["mock_image_request_item"]

    mock_tiling_strategy.compute_regions.return_value = ["region"]
    mock_image_request_item.tile_size = "(32, 32)"
    mock_image_request_item.tile_overlap = "(1, 1)"

    handler.load_image_request(mock_image_request_item, None)

    mock_tiling_strategy.compute_regions.assert_called_once_with(
        ((0, 0), (100, 100)),
        (256, 256),
        (32, 32),
        (1, 1),
    )


@patch("aws.osml.model_runner.image_request_handler.calculate_processing_bounds", return_value=None)
@patch("aws.osml.model_runner.image_request_handler.get_image_extension", return_value="tif")
@patch("aws.osml.model_runner.image_request_handler.load_gdal_dataset", return_value=(MagicMock(), MagicMock()))
@patch("aws.osml.model_runner.image_request_handler.get_image_path", return_value="/vsis3/bucket/key")
@patch("aws.osml.model_runner.image_request_handler.GDALConfigEnv")
def test_load_image_request_missing_bounds_raises(
    _mock_gdal_env, _mock_get_image_path, _mock_load_gdal, _mock_get_extension, _mock_bounds, handler_setup
):
    """
    Test load_image_request raises when bounds are empty.
    """
    handler = handler_setup["handler"]
    mock_image_request_item = handler_setup["mock_image_request_item"]

    mock_image_request_item.tile_size = "(32, 32)"
    with pytest.raises(LoadImageException):
        handler.load_image_request(mock_image_request_item, None)


def test_validate_model_hosting_invalid(handler_setup):
    """
    Test invalid model hosting raises and reports status.
    """
    handler = handler_setup["handler"]
    mock_image_status_monitor = handler_setup["mock_image_status_monitor"]

    image_request_item = ImageRequestItem(image_id="image-id", model_invoke_mode="BAD")
    with pytest.raises(UnsupportedModelException):
        handler.validate_model_hosting(image_request_item)
    mock_image_status_monitor.process_event.assert_called()


@patch("aws.osml.model_runner.image_request_handler.select_features", return_value=["deduped"])
@patch("aws.osml.model_runner.image_request_handler.ImageRequestHandler.calculate_processing_bounds")
def test_deduplicate_selects_features(mock_bounds, mock_select_features, handler_setup):
    """
    Test deduplicate calls select_features with processing bounds.
    """
    handler = handler_setup["handler"]
    mock_image_request = handler_setup["mock_image_request"]

    mock_bounds.return_value = ((0, 0), (10, 10))
    metrics = MetricsLogger(resolve_environment=MagicMock())
    metrics.set_dimensions = MagicMock()
    metrics.put_dimensions = MagicMock()
    image_request_item = ImageRequestItem.from_image_request(mock_image_request)

    result = ImageRequestHandler.deduplicate.__wrapped__(
        handler, image_request_item, [MagicMock()], MagicMock(), MagicMock(), metrics=metrics
    )

    assert result == ["deduped"]
    mock_select_features.assert_called_once()
    metrics.set_dimensions.assert_called_once()


@patch("aws.osml.model_runner.image_request_handler.select_features", return_value=["deduped"])
@patch("aws.osml.model_runner.image_request_handler.ImageRequestHandler.calculate_processing_bounds")
def test_deduplicate_without_metrics(mock_bounds, mock_select_features, handler_setup):
    """
    Test deduplicate works without metrics logger.
    """
    handler = handler_setup["handler"]
    mock_image_request = handler_setup["mock_image_request"]

    mock_bounds.return_value = ((0, 0), (10, 10))
    image_request_item = ImageRequestItem.from_image_request(mock_image_request)

    result = ImageRequestHandler.deduplicate.__wrapped__(
        handler, image_request_item, [MagicMock()], MagicMock(), MagicMock()
    )

    assert result == ["deduped"]
    mock_select_features.assert_called_once()


@patch("aws.osml.model_runner.image_request_handler.shapely.to_wkt", return_value="roi")
@patch("aws.osml.model_runner.image_request_handler.calculate_processing_bounds", return_value=None)
def test_calculate_processing_bounds_raises(_mock_bounds, _mock_to_wkt):
    """
    Test calculate_processing_bounds raises on empty bounds.
    """
    with pytest.raises(AggregateFeaturesException):
        ImageRequestHandler.calculate_processing_bounds(MagicMock(), MagicMock(), "POINT (1 2)")


@patch("aws.osml.model_runner.image_request_handler.shapely.to_wkt", return_value="roi")
@patch("aws.osml.model_runner.image_request_handler.calculate_processing_bounds", return_value=((0, 0), (1, 1)))
def test_calculate_processing_bounds_success(_mock_bounds, _mock_to_wkt):
    """
    Test calculate_processing_bounds returns bounds when present.
    """
    bounds = ImageRequestHandler.calculate_processing_bounds(MagicMock(), MagicMock(), "POINT (1 2)")
    assert bounds == ((0, 0), (1, 1))


@patch("aws.osml.model_runner.image_request_handler.calculate_processing_bounds", return_value=((0, 0), (1, 1)))
def test_calculate_processing_bounds_without_roi(_mock_bounds):
    """
    Test calculate_processing_bounds works without ROI.
    """
    bounds = ImageRequestHandler.calculate_processing_bounds(MagicMock(), MagicMock(), None)
    assert bounds == ((0, 0), (1, 1))


def test_end_image_request_metrics(handler_setup):
    """
    Test end_image_request emits metrics including errors.
    """
    handler = handler_setup["handler"]
    mock_image_request_table = handler_setup["mock_image_request_table"]
    mock_image_status_monitor = handler_setup["mock_image_status_monitor"]
    mock_image_request = handler_setup["mock_image_request"]

    image_request_item = ImageRequestItem.from_image_request(mock_image_request)
    image_request_item.processing_duration = 12
    image_request_item.region_error = 1
    mock_image_request_table.end_image_request.return_value = image_request_item
    mock_image_status_monitor.get_status.return_value = RequestStatus.SUCCESS
    metrics = MetricsLogger(resolve_environment=MagicMock())
    metrics.set_dimensions = MagicMock()
    metrics.put_dimensions = MagicMock()
    metrics.put_metric = MagicMock()

    ImageRequestHandler.end_image_request.__wrapped__(handler, image_request_item, "NITF", metrics=metrics)

    metrics.put_metric.assert_called()
    mock_image_status_monitor.process_event.assert_called()


def test_end_image_request_without_metrics(handler_setup):
    """
    Test end_image_request works without metrics logger.
    """
    handler = handler_setup["handler"]
    mock_image_request_table = handler_setup["mock_image_request_table"]
    mock_image_status_monitor = handler_setup["mock_image_status_monitor"]
    mock_image_request = handler_setup["mock_image_request"]

    image_request_item = ImageRequestItem.from_image_request(mock_image_request)
    mock_image_request_table.end_image_request.return_value = image_request_item
    mock_image_status_monitor.get_status.return_value = RequestStatus.SUCCESS

    ImageRequestHandler.end_image_request.__wrapped__(handler, image_request_item, "NITF")

    mock_image_status_monitor.process_event.assert_called()


@patch("aws.osml.model_runner.image_request_handler.SinkFactory.sink_features", return_value=False)
def test_sink_features_raises_on_failure(_mock_sink, handler_setup):
    """
    Test sink_features raises on write failure.
    """
    mock_image_request_item = handler_setup["mock_image_request_item"]

    with pytest.raises(AggregateOutputFeaturesException):
        ImageRequestHandler.sink_features.__wrapped__(mock_image_request_item, [MagicMock()])


@patch("aws.osml.model_runner.image_request_handler.SinkFactory.sink_features", return_value=True)
def test_sink_features_success_with_metrics(_mock_sink, handler_setup):
    """
    Test sink_features success path emits metrics.
    """
    mock_image_request_item = handler_setup["mock_image_request_item"]

    metrics = MetricsLogger(resolve_environment=MagicMock())
    metrics.set_dimensions = MagicMock()
    metrics.put_dimensions = MagicMock()
    ImageRequestHandler.sink_features.__wrapped__(mock_image_request_item, [MagicMock()], metrics=metrics)
    metrics.set_dimensions.assert_called_once()


def test_select_target_variant_single_variant(handler_setup):
    """
    Test selection when there's only one variant
    """
    sm_client_stub = handler_setup["sm_client_stub"]

    sm_client_stub.add_response(
        "describe_endpoint",
        expected_params={"EndpointName": "test-model-name"},
        service_response=MOCK_DESCRIBE_ENDPOINT_RESPONSE,
    )
    sm_client_stub.activate()

    image_request = _build_request_data()
    image_request.model_endpoint_parameters = None
    image_request = ImageRequestHandler.set_default_model_endpoint_variant(image_request)

    # Verify the selected variant
    assert image_request.model_endpoint_parameters["TargetVariant"] == "variant1"


def test_select_target_variant_http_endpoint(handler_setup):
    """
    Test selection when there's only one variant
    """
    image_request = _build_request_data()
    image_request.model_invoke_mode = ModelInvokeMode.HTTP_ENDPOINT
    expected_parameters = {"http_parameter": "not sagemaker"}
    image_request.model_endpoint_parameters = expected_parameters
    image_request = ImageRequestHandler.set_default_model_endpoint_variant(image_request)

    # Verify the parameters were not altered
    assert image_request.model_endpoint_parameters == expected_parameters


def test_select_target_variant_multiple_variants(handler_setup):
    """
    Test selection with multiple variants with different weights
    """
    sm_client_stub = handler_setup["sm_client_stub"]

    multiple_variants_response = {
        **MOCK_DESCRIBE_ENDPOINT_RESPONSE,
        "ProductionVariants": [
            {"VariantName": "variant1", "CurrentWeight": 0.6},
            {"VariantName": "variant2", "CurrentWeight": 0.3},
            {"VariantName": "variant3", "CurrentWeight": 0.1},
        ],
    }
    image_request = _build_request_data()
    # Test multiple selections to ensure all variants can be selected
    selections = Counter()
    for _ in range(100):
        sm_client_stub.add_response(
            "describe_endpoint",
            expected_params={"EndpointName": "test-model-name"},
            service_response=multiple_variants_response,
        )
    sm_client_stub.activate()
    for _ in range(100):
        image_request.model_endpoint_parameters = None
        image_request = ImageRequestHandler.set_default_model_endpoint_variant(image_request)
        selections[image_request.model_endpoint_parameters["TargetVariant"]] += 1
    sm_client_stub.deactivate()

    # Verify that variants were selected at least once
    assert len(selections) == 3
    assert "variant1" in selections
    assert "variant2" in selections
    assert "variant3" in selections
    assert selections.most_common(1)[0][0] == "variant1"


def test_select_target_variant_default_weight(handler_setup):
    """
    Test that variants without specified weights get default weight of 1.0
    """
    sm_client_stub = handler_setup["sm_client_stub"]

    default_weight_response = {
        **MOCK_DESCRIBE_ENDPOINT_RESPONSE,
        "ProductionVariants": [
            {"VariantName": "variant1"},
            {"VariantName": "variant2", "CurrentWeight": 1.0},
        ],
    }
    image_request = _build_request_data()
    # Run multiple selections to ensure both variants can be selected
    selections = set()
    for _ in range(100):
        sm_client_stub.add_response(
            "describe_endpoint",
            expected_params={"EndpointName": "test-model-name"},
            service_response=default_weight_response,
        )
    sm_client_stub.activate()
    for _ in range(100):
        image_request.model_endpoint_parameters = {"other_param": "important_value"}
        image_request = ImageRequestHandler.set_default_model_endpoint_variant(image_request)
        selections.add(image_request.model_endpoint_parameters["TargetVariant"])
    sm_client_stub.deactivate()

    # Verify that both variants were selected at least once
    assert len(selections) == 2
    assert "variant1" in selections
    assert "variant2" in selections


def _build_request_data():
    """
    Helper method to build sample image request data for tests.
    """
    return ImageRequest(
        job_id="test-job-id",
        image_id="test-image-id",
        image_url="test-image-url",
        image_read_role="arn:aws:iam::012345678910:role/TestRole",
        outputs=[
            {"type": "S3", "bucket": "test-bucket", "prefix": "test-bucket-prefix"},
            {"type": "Kinesis", "stream": "test-stream", "batchSize": 1000},
        ],
        tile_size=(1024, 1024),
        tile_overlap=(50, 50),
        tile_format="NITF",
        model_name="test-model-name",
        model_invoke_mode=ModelInvokeMode.SM_ENDPOINT,
        model_invocation_role="arn:aws:iam::012345678910:role/TestRole",
    )
