#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

from unittest import TestCase, main
from unittest.mock import Mock, patch


class TestBatchTileWorkerUtils(TestCase):
    @patch("aws.osml.model_runner.tile_worker.batch_tile_worker_utils.FeatureDetectorFactory")
    @patch("aws.osml.model_runner.tile_worker.batch_tile_worker_utils.BatchSubmissionWorker", autospec=True)
    @patch("aws.osml.model_runner.tile_worker.batch_tile_worker_utils.ServiceConfig", autospec=True)
    def test_setup_batch_submission_worker(
        self, mock_service_config, mock_submission_worker, mock_feature_detector_factory
    ):
        """
        Test the setup of batch submission worker, ensuring the worker is initialized
        and started correctly.
        """
        from aws.osml.model_runner.api import ImageRequest
        from aws.osml.model_runner.tile_worker.batch_tile_worker_utils import setup_batch_submission_worker

        mock_feature_detector = Mock()
        mock_feature_detector.endpoint = "test-model-endpoint"
        mock_feature_detector_factory.return_value.build.return_value = mock_feature_detector
        mock_submission_worker.return_value.start = Mock()

        mock_image_request = ImageRequest(
            {
                "jobId": "test-job-001",
                "imageId": "test-image-456",
                "imageUrl": "s3://bucket/image.tif",
                "outputs": [{"type": "S3", "bucket": "output-bucket", "prefix": "results/"}],
                "imageProcessor": {
                    "name": "test-model",
                    "type": "BATCH",
                },
            }
        )

        work_queue, worker = setup_batch_submission_worker(mock_image_request)

        # Assert that the worker was created and started
        assert worker is not None
        mock_submission_worker.return_value.start.assert_called_once()

    @patch("aws.osml.model_runner.tile_worker.batch_tile_worker_utils.FeatureDetectorFactory")
    @patch("aws.osml.model_runner.tile_worker.batch_tile_worker_utils.BatchSubmissionWorker", autospec=True)
    @patch("aws.osml.model_runner.tile_worker.batch_tile_worker_utils.ServiceConfig", autospec=True)
    def test_setup_batch_submission_worker_with_credentials(
        self, mock_service_config, mock_submission_worker, mock_feature_detector_factory
    ):
        """
        Test the setup of batch submission worker with assumed role credentials.
        """
        from aws.osml.model_runner.api import ImageRequest
        from aws.osml.model_runner.tile_worker.batch_tile_worker_utils import setup_batch_submission_worker

        mock_feature_detector = Mock()
        mock_feature_detector.endpoint = "test-model-endpoint"
        mock_feature_detector_factory.return_value.build.return_value = mock_feature_detector
        mock_submission_worker.return_value.start = Mock()

        mock_image_request = ImageRequest(
            {
                "jobId": "test-job-001",
                "imageId": "test-image-456",
                "imageUrl": "s3://bucket/image.tif",
                "outputs": [{"type": "S3", "bucket": "output-bucket", "prefix": "results/"}],
                "imageProcessor": {
                    "name": "test-model",
                    "type": "BATCH",
                },
                "imageProcessorRole": "arn:aws:iam::123456789012:role/test-role",
            }
        )

        with patch("aws.osml.model_runner.tile_worker.batch_tile_worker_utils.get_credentials_for_assumed_role"):
            work_queue, worker = setup_batch_submission_worker(mock_image_request)

            # Assert that the worker was created
            assert worker is not None

    @patch("aws.osml.model_runner.tile_worker.batch_tile_worker_utils.FeatureDetectorFactory")
    @patch("aws.osml.model_runner.tile_worker.batch_tile_worker_utils.BatchSubmissionWorker", autospec=True)
    @patch("aws.osml.model_runner.tile_worker.batch_tile_worker_utils.ServiceConfig", autospec=True)
    def test_setup_batch_submission_worker_exception(
        self, mock_service_config, mock_submission_worker, mock_feature_detector_factory
    ):
        """
        Test that an exception during batch submission worker setup raises a SetupTileWorkersException.
        """
        from aws.osml.model_runner.api import ImageRequest
        from aws.osml.model_runner.tile_worker.batch_tile_worker_utils import setup_batch_submission_worker
        from aws.osml.model_runner.tile_worker.exceptions import SetupTileWorkersException

        mock_submission_worker.return_value.start = Mock()
        mock_feature_detector_factory.side_effect = Exception("Mock detector creation exception")

        mock_image_request = ImageRequest(
            {
                "jobId": "test-job-001",
                "imageId": "test-image-456",
                "imageUrl": "s3://bucket/image.tif",
                "outputs": [{"type": "S3", "bucket": "output-bucket", "prefix": "results/"}],
                "imageProcessor": {
                    "name": "test-model",
                    "type": "BATCH",
                },
            }
        )

        with self.assertRaises(SetupTileWorkersException):
            # Attempt to set up worker should fail and raise the specified exception
            setup_batch_submission_worker(mock_image_request)

    @patch("aws.osml.model_runner.tile_worker.batch_tile_worker_utils.FeatureDetectorFactory")
    @patch("aws.osml.model_runner.tile_worker.batch_tile_worker_utils.BatchUploadWorker", autospec=True)
    @patch("aws.osml.model_runner.tile_worker.batch_tile_worker_utils.ServiceConfig", autospec=True)
    def test_setup_upload_tile_workers(self, mock_service_config, mock_upload_worker, mock_feature_detector_factory):
        """
        Test the setup of batch upload tile workers, ensuring the correct number of workers is initialized
        based on the configuration and that workers are started correctly.
        """
        from aws.osml.model_runner.api import ImageRequest
        from aws.osml.model_runner.tile_worker.batch_tile_worker_utils import setup_upload_tile_workers

        mock_feature_detector = Mock()
        mock_feature_detector.endpoint = "test-model-endpoint"
        mock_feature_detector_factory.return_value.build.return_value = mock_feature_detector
        mock_upload_worker.return_value.start = Mock()
        mock_num_workers = 3
        mock_service_config.async_endpoint_config.submission_workers = mock_num_workers

        mock_image_request = ImageRequest(
            {
                "jobId": "test-job-001",
                "imageId": "test-image-456",
                "imageUrl": "s3://bucket/image.tif",
                "outputs": [{"type": "S3", "bucket": "output-bucket", "prefix": "results/"}],
                "imageProcessor": {
                    "name": "test-model",
                    "type": "BATCH",
                },
            }
        )

        work_queue, tile_worker_list = setup_upload_tile_workers(mock_image_request)

        # Assert that the correct number of tile workers are created
        assert len(tile_worker_list) == mock_num_workers
        # Verify that each worker's start method is called
        assert mock_upload_worker.return_value.start.call_count == mock_num_workers

    @patch("aws.osml.model_runner.tile_worker.batch_tile_worker_utils.FeatureDetectorFactory")
    @patch("aws.osml.model_runner.tile_worker.batch_tile_worker_utils.BatchUploadWorker", autospec=True)
    @patch("aws.osml.model_runner.tile_worker.batch_tile_worker_utils.ServiceConfig", autospec=True)
    def test_setup_upload_tile_workers_with_credentials(
        self, mock_service_config, mock_upload_worker, mock_feature_detector_factory
    ):
        """
        Test the setup of batch upload tile workers with assumed role credentials.
        """
        from aws.osml.model_runner.api import ImageRequest
        from aws.osml.model_runner.tile_worker.batch_tile_worker_utils import setup_upload_tile_workers

        mock_feature_detector = Mock()
        mock_feature_detector.endpoint = "test-model-endpoint"
        mock_feature_detector_factory.return_value.build.return_value = mock_feature_detector
        mock_upload_worker.return_value.start = Mock()
        mock_num_workers = 2
        mock_service_config.async_endpoint_config.submission_workers = mock_num_workers

        mock_image_request = ImageRequest(
            {
                "jobId": "test-job-001",
                "imageId": "test-image-456",
                "imageUrl": "s3://bucket/image.tif",
                "outputs": [{"type": "S3", "bucket": "output-bucket", "prefix": "results/"}],
                "imageProcessor": {
                    "name": "test-model",
                    "type": "BATCH",
                },
                "imageProcessorRole": "arn:aws:iam::123456789012:role/test-role",
            }
        )

        with patch("aws.osml.model_runner.tile_worker.batch_tile_worker_utils.get_credentials_for_assumed_role"):
            work_queue, tile_worker_list = setup_upload_tile_workers(mock_image_request)

            # Assert that the correct number of tile workers are created
            assert len(tile_worker_list) == mock_num_workers

    @patch("aws.osml.model_runner.tile_worker.batch_tile_worker_utils.FeatureDetectorFactory")
    @patch("aws.osml.model_runner.tile_worker.batch_tile_worker_utils.BatchUploadWorker", autospec=True)
    @patch("aws.osml.model_runner.tile_worker.batch_tile_worker_utils.ServiceConfig", autospec=True)
    def test_setup_upload_tile_workers_exception(
        self, mock_service_config, mock_upload_worker, mock_feature_detector_factory
    ):
        """
        Test that an exception during upload tile worker setup raises a SetupTileWorkersException.
        """
        from aws.osml.model_runner.api import ImageRequest
        from aws.osml.model_runner.tile_worker.batch_tile_worker_utils import setup_upload_tile_workers
        from aws.osml.model_runner.tile_worker.exceptions import SetupTileWorkersException

        mock_upload_worker.return_value.start = Mock()
        mock_num_workers = 2
        mock_service_config.async_endpoint_config.submission_workers = mock_num_workers
        mock_feature_detector_factory.side_effect = Exception("Mock detector creation exception")

        mock_image_request = ImageRequest(
            {
                "jobId": "test-job-001",
                "imageId": "test-image-456",
                "imageUrl": "s3://bucket/image.tif",
                "outputs": [{"type": "S3", "bucket": "output-bucket", "prefix": "results/"}],
                "imageProcessor": {
                    "name": "test-model",
                    "type": "BATCH",
                },
            }
        )

        with self.assertRaises(SetupTileWorkersException):
            # Attempt to set up workers should fail and raise the specified exception
            setup_upload_tile_workers(mock_image_request)

    @patch("aws.osml.model_runner.tile_worker.batch_tile_worker_utils.FeatureDetectorFactory")
    @patch("aws.osml.model_runner.tile_worker.batch_tile_worker_utils.BatchUploadWorker", autospec=True)
    @patch("aws.osml.model_runner.tile_worker.batch_tile_worker_utils.ServiceConfig", autospec=True)
    def test_setup_upload_tile_workers_with_sensor_model(
        self, mock_service_config, mock_upload_worker, mock_feature_detector_factory
    ):
        """
        Test the setup of batch upload tile workers with sensor and elevation models.
        """
        from aws.osml.model_runner.api import ImageRequest
        from aws.osml.model_runner.tile_worker.batch_tile_worker_utils import setup_upload_tile_workers

        mock_feature_detector = Mock()
        mock_feature_detector.endpoint = "test-model-endpoint"
        mock_feature_detector_factory.return_value.build.return_value = mock_feature_detector
        mock_upload_worker.return_value.start = Mock()
        mock_num_workers = 2
        mock_service_config.async_endpoint_config.submission_workers = mock_num_workers

        mock_image_request = ImageRequest(
            {
                "jobId": "test-job-001",
                "imageId": "test-image-456",
                "imageUrl": "s3://bucket/image.tif",
                "outputs": [{"type": "S3", "bucket": "output-bucket", "prefix": "results/"}],
                "imageProcessor": {
                    "name": "test-model",
                    "type": "BATCH",
                },
            }
        )

        mock_sensor_model = Mock()
        mock_elevation_model = Mock()

        work_queue, tile_worker_list = setup_upload_tile_workers(
            mock_image_request, sensor_model=mock_sensor_model, elevation_model=mock_elevation_model
        )

        # Assert that the correct number of tile workers are created
        assert len(tile_worker_list) == mock_num_workers


if __name__ == "__main__":
    main()
