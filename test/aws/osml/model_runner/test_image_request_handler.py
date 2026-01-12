#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.
from collections import Counter
from datetime import datetime, timezone
from unittest import TestCase, main
from unittest.mock import MagicMock, patch

import boto3
from botocore.stub import Stubber

from aws.osml.model_runner.api import ImageRequest, ModelInvokeMode
from aws.osml.model_runner.app_config import ServiceConfig
from aws.osml.model_runner.common import RequestStatus
from aws.osml.model_runner.database import ImageRequestItem, ImageRequestTable, RegionRequestTable
from aws.osml.model_runner.exceptions import ProcessImageException
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


class TestImageRequestHandler(TestCase):
    def setUp(self):
        # Mock dependencies
        self.mock_image_request_table = MagicMock(spec=ImageRequestTable)
        self.mock_image_status_monitor = MagicMock(spec=ImageStatusMonitor)
        self.mock_tiling_strategy = MagicMock(spec=TilingStrategy)
        self.mock_region_request_queue = MagicMock(spec=RequestQueue)
        self.mock_region_request_table = MagicMock(spec=RegionRequestTable)
        self.mock_config = MagicMock(spec=ServiceConfig)

        # Set up config properties

        # Instantiate the handler with mocked dependencies
        self.handler = ImageRequestHandler(
            image_request_table=self.mock_image_request_table,
            image_status_monitor=self.mock_image_status_monitor,
            tiling_strategy=self.mock_tiling_strategy,
            region_request_queue=self.mock_region_request_queue,
            region_request_table=self.mock_region_request_table,
            config=self.mock_config,
            region_request_handler=MagicMock(),
        )

        # Mock request and items
        self.mock_image_request = self.image_request = ImageRequest.from_external_message(
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

        self.mock_image_request_item = ImageRequestItem.from_image_request(self.mock_image_request)

        # Create and stub the SageMaker client
        self.sm_client = boto3.client("sagemaker")
        self.sm_client_stub = Stubber(self.sm_client)

        # Patch boto3.client to return our stubbed client
        self.boto3_patcher = patch("boto3.client")
        self.mock_boto3_client = self.boto3_patcher.start()
        self.mock_boto3_client.return_value = self.sm_client

    def tearDown(self):
        self.sm_client_stub.deactivate()
        self.boto3_patcher.stop()

    def test_process_image_request_success(self):
        """
        Test successful image request processing.
        """
        # Mock internal methods
        self.handler.load_image_request = MagicMock(return_value=("tif", MagicMock(), MagicMock(), [MagicMock()]))
        self.handler.queue_region_request = MagicMock()

        self.handler.set_default_model_endpoint_variant = MagicMock(return_value=self.mock_image_request)

        # Call process_image_request
        self.handler.process_image_request(self.mock_image_request)

        # Assert that the STARTED status was called first
        self.mock_image_request_table.start_image_request.assert_called_once()

        # Ensure the regions were queued
        self.handler.queue_region_request.assert_called_once()

        # Ensure processing events were emitted
        self.assertEqual(self.mock_image_status_monitor.process_event.call_count, 2)

    def test_process_image_request_failure(self):
        """
        Test failure during image request processing.
        """
        # Simulate an exception in load_image_request
        self.handler.load_image_request = MagicMock(side_effect=Exception("Test error"))

        # Call process_image_request and assert the exception is raised
        with self.assertRaises(ProcessImageException):
            self.handler.process_image_request(self.mock_image_request)

        # Ensure failure handling methods were called
        self.mock_image_status_monitor.process_event.assert_called()

    @patch("aws.osml.model_runner.image_request_handler.SinkFactory.sink_features")
    @patch("aws.osml.model_runner.image_request_handler.ImageRequestHandler.deduplicate")
    @patch("aws.osml.model_runner.image_request_handler.FeatureTable.aggregate_features")
    def test_complete_image_request(self, mock_aggregate_features, mock_deduplicate, mock_sink_features):
        """
        Test successful completion of image request.
        """
        # Set up mock return values for our ImageRequestItem to complete
        self.mock_image_request_table.get_image_request.return_value = self.mock_image_request_item
        self.mock_image_request_item.processing_duration = 1000
        self.mock_image_request_item.region_error = 0

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
        self.handler.complete_image_request(mock_region_request, "tif", mock_raster_dataset, mock_sensor_model)

        # Ensure sink_features was called correctly
        mock_sink_features.assert_called_once()

        # Ensure failure handling methods were called
        self.mock_image_status_monitor.process_event.assert_called()

    def test_fail_image_request(self):
        """
        Test fail_image_request method behavior.
        """
        # Call fail_image_request
        self.handler.fail_image_request(self.mock_image_request_item, Exception("Test failure"))

        # Ensure status monitor was updated and job table was called
        self.mock_image_status_monitor.process_event.assert_called_once_with(
            self.mock_image_request_item, RequestStatus.FAILED, "Test failure"
        )
        self.mock_image_request_table.end_image_request.assert_called_once_with(self.mock_image_request_item.image_id)

    def test_select_target_variant_single_variant(self):
        """
        Test selection when there's only one variant
        """
        self.sm_client_stub.add_response(
            "describe_endpoint",
            expected_params={"EndpointName": "test-model-name"},
            service_response=MOCK_DESCRIBE_ENDPOINT_RESPONSE,
        )
        self.sm_client_stub.activate()

        image_request = self._build_request_data()
        image_request.model_endpoint_parameters = None
        image_request = ImageRequestHandler.set_default_model_endpoint_variant(image_request)

        # Verify the selected variant
        assert image_request.model_endpoint_parameters["TargetVariant"] == "variant1"

    def test_select_target_variant_http_endpoint(self):
        """
        Test selection when there's only one variant
        """
        image_request = self._build_request_data()
        image_request.model_invoke_mode = ModelInvokeMode.HTTP_ENDPOINT
        expected_parameters = {"http_parameter": "not sagemaker"}
        image_request.model_endpoint_parameters = expected_parameters
        image_request = ImageRequestHandler.set_default_model_endpoint_variant(image_request)

        # Verify the parameters were not altered
        assert image_request.model_endpoint_parameters == expected_parameters

    def test_select_target_variant_multiple_variants(self):
        """
        Test selection with multiple variants with different weights
        """
        multiple_variants_response = {
            **MOCK_DESCRIBE_ENDPOINT_RESPONSE,
            "ProductionVariants": [
                {"VariantName": "variant1", "CurrentWeight": 0.6},
                {"VariantName": "variant2", "CurrentWeight": 0.3},
                {"VariantName": "variant3", "CurrentWeight": 0.1},
            ],
        }
        image_request = self._build_request_data()
        # Test multiple selections to ensure all variants can be selected
        selections = Counter()
        for _ in range(100):
            self.sm_client_stub.add_response(
                "describe_endpoint",
                expected_params={"EndpointName": "test-model-name"},
                service_response=multiple_variants_response,
            )
            self.sm_client_stub.activate()
            image_request.model_endpoint_parameters = None
            image_request = ImageRequestHandler.set_default_model_endpoint_variant(image_request)
            selections[image_request.model_endpoint_parameters["TargetVariant"]] += 1
            self.sm_client_stub.deactivate()

        # Verify that variants were selected at least once
        assert len(selections) == 3
        assert "variant1" in selections
        assert "variant2" in selections
        assert "variant3" in selections
        assert selections.most_common(1)[0][0] == "variant1"

    def test_select_target_variant_default_weight(self):
        """
        Test that variants without specified weights get default weight of 1.0
        """
        default_weight_response = {
            **MOCK_DESCRIBE_ENDPOINT_RESPONSE,
            "ProductionVariants": [
                {"VariantName": "variant1"},
                {"VariantName": "variant2", "CurrentWeight": 1.0},
            ],
        }
        image_request = self._build_request_data()
        # Run multiple selections to ensure both variants can be selected
        selections = set()
        for _ in range(100):
            self.sm_client_stub.add_response(
                "describe_endpoint",
                expected_params={"EndpointName": "test-model-name"},
                service_response=default_weight_response,
            )
            self.sm_client_stub.activate()
            image_request.model_endpoint_parameters = {"other_param": "important_value"}
            image_request = ImageRequestHandler.set_default_model_endpoint_variant(image_request)
            selections.add(image_request.model_endpoint_parameters["TargetVariant"])
            self.sm_client_stub.deactivate()

        # Verify that both variants were selected at least once
        assert len(selections) == 2
        assert "variant1" in selections
        assert "variant2" in selections

    @staticmethod
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


if __name__ == "__main__":
    main()
