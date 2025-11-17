#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

from unittest import TestCase, main
from unittest.mock import Mock, patch


class TestAsyncTileWorkerUtils(TestCase):
    @patch("aws.osml.model_runner.tile_worker.async_tile_worker_utils.FeatureDetectorFactory")
    @patch("aws.osml.model_runner.tile_worker.async_tile_worker_utils.AsyncResultsWorker", autospec=True)
    @patch("aws.osml.model_runner.tile_worker.async_tile_worker_utils.ServiceConfig", autospec=True)
    def test_setup_result_tile_workers(self, mock_service_config, mock_results_worker, mock_feature_detector_factory):
        """
        Test the setup of async result tile workers, ensuring the correct number of workers is initialized
        based on the configuration and that workers are started correctly.
        """
        from aws.osml.model_runner.api import TileRequest
        from aws.osml.model_runner.tile_worker.async_tile_worker_utils import setup_result_tile_workers

        mock_feature_detector = Mock()
        mock_feature_detector.endpoint = "test-model-endpoint"
        mock_feature_detector_factory.return_value.build.return_value = mock_feature_detector
        mock_results_worker.return_value.start = Mock()
        mock_num_workers = 3
        mock_service_config.async_endpoint_config.polling_workers = mock_num_workers
        mock_service_config.feature_table = "test-feature-table"
        mock_service_config.region_request_table = "test-region-table"

        mock_tile_request = TileRequest(
            tile_id="test-tile-123",
            image_id="test-image-456",
            region_id="test-region-789",
            job_id="test-job-001",
            image_url="s3://bucket/image.tif",
            image_path="s3://bucket/image.tif",
            tile_bounds=[[0, 0], [1024, 1024]],
            model_name="test-model",
            tile_size=[512, 512],
            tile_overlap=[50, 50],
            model_invoke_mode="ASYNC",
        )

        work_queue, tile_worker_list = setup_result_tile_workers(mock_tile_request)

        # Assert that the correct number of tile workers are created
        assert len(tile_worker_list) == mock_num_workers
        # Verify that each worker's start method is called
        assert mock_results_worker.return_value.start.call_count == mock_num_workers

    @patch("aws.osml.model_runner.tile_worker.async_tile_worker_utils.FeatureDetectorFactory")
    @patch("aws.osml.model_runner.tile_worker.async_tile_worker_utils.AsyncResultsWorker", autospec=True)
    @patch("aws.osml.model_runner.tile_worker.async_tile_worker_utils.ServiceConfig", autospec=True)
    def test_setup_result_tile_workers_with_completion_queue(
        self, mock_service_config, mock_results_worker, mock_feature_detector_factory
    ):
        """
        Test the setup of async result tile workers with a completion queue.
        """
        from queue import Queue

        from aws.osml.model_runner.api import TileRequest
        from aws.osml.model_runner.tile_worker.async_tile_worker_utils import setup_result_tile_workers

        mock_feature_detector = Mock()
        mock_feature_detector.endpoint = "test-model-endpoint"
        mock_feature_detector_factory.return_value.build.return_value = mock_feature_detector
        mock_results_worker.return_value.start = Mock()
        mock_num_workers = 2
        mock_service_config.async_endpoint_config.polling_workers = mock_num_workers
        mock_service_config.feature_table = "test-feature-table"
        mock_service_config.region_request_table = "test-region-table"

        mock_tile_request = TileRequest(
            tile_id="test-tile-123",
            image_id="test-image-456",
            region_id="test-region-789",
            job_id="test-job-001",
            image_url="s3://bucket/image.tif",
            image_path="s3://bucket/image.tif",
            tile_bounds=[[0, 0], [1024, 1024]],
            model_name="test-model",
            tile_size=[512, 512],
            tile_overlap=[50, 50],
            model_invoke_mode="ASYNC",
        )

        completion_queue = Queue()
        work_queue, tile_worker_list = setup_result_tile_workers(mock_tile_request, completion_queue=completion_queue)

        # Assert that the correct number of tile workers are created
        assert len(tile_worker_list) == mock_num_workers
        # Verify completion queue was passed to workers
        assert mock_results_worker.call_count == mock_num_workers

    @patch("aws.osml.model_runner.tile_worker.async_tile_worker_utils.FeatureTable", autospec=True)
    @patch("aws.osml.model_runner.tile_worker.async_tile_worker_utils.AsyncResultsWorker", autospec=True)
    @patch("aws.osml.model_runner.tile_worker.async_tile_worker_utils.ServiceConfig", autospec=True)
    def test_setup_result_tile_workers_exception(
        self, mock_service_config, mock_results_worker, mock_feature_table
    ):
        """
        Test that an exception during result tile worker setup raises a SetupTileWorkersException.
        """
        from aws.osml.model_runner.api import TileRequest
        from aws.osml.model_runner.tile_worker.async_tile_worker_utils import setup_result_tile_workers
        from aws.osml.model_runner.tile_worker.exceptions import SetupTileWorkersException

        mock_results_worker.return_value.start = Mock()
        mock_num_workers = 2
        mock_service_config.async_endpoint_config.polling_workers = mock_num_workers
        mock_feature_table.side_effect = Exception("Mock processing exception")

        mock_tile_request = TileRequest(
            tile_id="test-tile-123",
            image_id="test-image-456",
            region_id="test-region-789",
            job_id="test-job-001",
            image_url="s3://bucket/image.tif",
            image_path="s3://bucket/image.tif",
            tile_bounds=[[0, 0], [1024, 1024]],
            model_name="test-model",
            tile_size=[512, 512],
            tile_overlap=[50, 50],
            model_invoke_mode="ASYNC",
        )

        with self.assertRaises(SetupTileWorkersException):
            # Attempt to set up workers should fail and raise the specified exception
            setup_result_tile_workers(mock_tile_request)

    @patch("aws.osml.model_runner.tile_worker.async_tile_worker_utils.FeatureDetectorFactory")
    @patch("aws.osml.model_runner.tile_worker.async_tile_worker_utils.AsyncSubmissionWorker", autospec=True)
    @patch("aws.osml.model_runner.tile_worker.async_tile_worker_utils.ServiceConfig", autospec=True)
    def test_setup_submission_tile_workers(
        self, mock_service_config, mock_submission_worker, mock_feature_detector_factory
    ):
        """
        Test the setup of async submission tile workers.
        """
        from aws.osml.model_runner.api import RegionRequest
        from aws.osml.model_runner.tile_worker.async_tile_worker_utils import setup_submission_tile_workers

        mock_feature_detector = Mock()
        mock_feature_detector.endpoint = "test-model-endpoint"
        mock_feature_detector_factory.return_value.build.return_value = mock_feature_detector
        mock_submission_worker.return_value.start = Mock()
        mock_num_workers = 4
        mock_service_config.async_endpoint_config.submission_workers = mock_num_workers

        mock_region_request = RegionRequest(
            {
                "tile_size": (512, 512),
                "tile_overlap": (50, 50),
                "tile_format": "NITF",
                "image_id": "test-image-1",
                "image_url": "s3://bucket/image.tif",
                "region_bounds": ((0, 0), (2048, 2048)),
                "model_invoke_mode": "ASYNC",
                "model_name": "test-model",
                "image_extension": "tif",
            }
        )

        work_queue, tile_worker_list = setup_submission_tile_workers(mock_region_request)

        # Assert that the correct number of tile workers are created
        assert len(tile_worker_list) == mock_num_workers
        # Verify that each worker's start method is called
        assert mock_submission_worker.return_value.start.call_count == mock_num_workers

    @patch("aws.osml.model_runner.tile_worker.async_tile_worker_utils.FeatureDetectorFactory")
    @patch("aws.osml.model_runner.tile_worker.async_tile_worker_utils.AsyncSubmissionWorker", autospec=True)
    @patch("aws.osml.model_runner.tile_worker.async_tile_worker_utils.ServiceConfig", autospec=True)
    def test_setup_submission_tile_workers_with_credentials(
        self, mock_service_config, mock_submission_worker, mock_feature_detector_factory
    ):
        """
        Test the setup of async submission tile workers with assumed role credentials.
        """
        from aws.osml.model_runner.api import RegionRequest
        from aws.osml.model_runner.tile_worker.async_tile_worker_utils import setup_submission_tile_workers

        mock_feature_detector = Mock()
        mock_feature_detector.endpoint = "test-model-endpoint"
        mock_feature_detector_factory.return_value.build.return_value = mock_feature_detector
        mock_submission_worker.return_value.start = Mock()
        mock_num_workers = 2
        mock_service_config.async_endpoint_config.submission_workers = mock_num_workers

        mock_region_request = RegionRequest(
            {
                "tile_size": (512, 512),
                "tile_overlap": (50, 50),
                "tile_format": "NITF",
                "image_id": "test-image-1",
                "image_url": "s3://bucket/image.tif",
                "region_bounds": ((0, 0), (2048, 2048)),
                "model_invoke_mode": "ASYNC",
                "model_name": "test-model",
                "model_invocation_role": "arn:aws:iam::123456789012:role/test-role",
                "image_extension": "tif",
            }
        )

        with patch("aws.osml.model_runner.tile_worker.async_tile_worker_utils.get_credentials_for_assumed_role"):
            work_queue, tile_worker_list = setup_submission_tile_workers(mock_region_request)

            # Assert that the correct number of tile workers are created
            assert len(tile_worker_list) == mock_num_workers

    @patch("aws.osml.model_runner.tile_worker.async_tile_worker_utils.FeatureDetectorFactory")
    @patch("aws.osml.model_runner.tile_worker.async_tile_worker_utils.AsyncSubmissionWorker", autospec=True)
    @patch("aws.osml.model_runner.tile_worker.async_tile_worker_utils.ServiceConfig", autospec=True)
    def test_setup_submission_tile_workers_exception(
        self, mock_service_config, mock_submission_worker, mock_feature_detector_factory
    ):
        """
        Test that an exception during submission tile worker setup raises a SetupTileWorkersException.
        """
        from aws.osml.model_runner.api import RegionRequest
        from aws.osml.model_runner.tile_worker.async_tile_worker_utils import setup_submission_tile_workers
        from aws.osml.model_runner.tile_worker.exceptions import SetupTileWorkersException

        mock_submission_worker.return_value.start = Mock()
        mock_num_workers = 2
        mock_service_config.async_endpoint_config.submission_workers = mock_num_workers
        mock_feature_detector_factory.side_effect = Exception("Mock detector creation exception")

        mock_region_request = RegionRequest(
            {
                "tile_size": (512, 512),
                "tile_overlap": (50, 50),
                "tile_format": "NITF",
                "image_id": "test-image-1",
                "image_url": "s3://bucket/image.tif",
                "region_bounds": ((0, 0), (2048, 2048)),
                "model_invoke_mode": "ASYNC",
                "model_name": "test-model",
                "image_extension": "tif",
            }
        )

        with self.assertRaises(SetupTileWorkersException):
            # Attempt to set up workers should fail and raise the specified exception
            setup_submission_tile_workers(mock_region_request)


if __name__ == "__main__":
    main()
