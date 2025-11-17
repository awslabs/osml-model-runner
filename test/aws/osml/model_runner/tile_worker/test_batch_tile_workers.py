#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

import os
import tempfile
from queue import Queue
from unittest import TestCase
from unittest.mock import MagicMock, Mock, patch

from aws.osml.model_runner.common import RequestStatus
from aws.osml.model_runner.tile_worker.batch_tile_workers import BatchUploadWorker, BatchSubmissionWorker


class TestBatchUploadWorker(TestCase):
    """Test cases for BatchUploadWorker"""

    def setUp(self):
        """Set up test fixtures"""
        self.in_queue = Queue()
        self.mock_detector = MagicMock()
        
        # Create temp file for testing
        self.temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".tif")
        self.temp_file.write(b"test image data")
        self.temp_file.close()

    def tearDown(self):
        """Clean up test fixtures"""
        if os.path.exists(self.temp_file.name):
            os.remove(self.temp_file.name)

    @patch("aws.osml.model_runner.tile_worker.batch_tile_workers.S3_MANAGER")
    @patch("aws.osml.model_runner.tile_worker.batch_tile_workers.TileRequestTable")
    def test_worker_initialization(self, mock_table_class, mock_s3):
        """Test worker initializes correctly"""
        worker = BatchUploadWorker(
            worker_id=1,
            in_queue=self.in_queue,
            feature_detector=self.mock_detector,
        )
        
        assert worker.worker_id == 1
        assert worker.in_queue == self.in_queue
        assert worker.feature_detector == self.mock_detector
        assert worker.failed_tile_count == 0
        assert worker.processed_tile_count == 0
        assert worker.running is True

    @patch("aws.osml.model_runner.tile_worker.batch_tile_workers.S3_MANAGER")
    @patch("aws.osml.model_runner.tile_worker.batch_tile_workers.TileRequestTable")
    @patch("aws.osml.model_runner.tile_worker.batch_tile_workers.ServiceConfig")
    def test_process_tile_submission_success(self, mock_config, mock_table_class, mock_s3):
        """Test successful tile upload"""
        # Setup mocks
        mock_config.batch_input_prefix = "batch/input/"
        mock_config.batch_output_prefix = "batch/output/"
        mock_config.input_bucket = "test-bucket"
        mock_s3.upload_payload.return_value = "s3://bucket/input/tile.tif"
        mock_table = MagicMock()
        mock_table_class.return_value = mock_table
        
        worker = BatchUploadWorker(
            worker_id=1,
            in_queue=self.in_queue,
            feature_detector=self.mock_detector,
        )
        worker.tile_request_table = mock_table
        
        tile_info = {
            "job_id": "test-job",
            "tile_id": "tile-123",
            "region_id": "region-456",
            "image_path": self.temp_file.name,
            "region": "test-region",
        }
        
        # Process tile
        result = worker.process_tile_submission(tile_info)
        
        assert result is True
        mock_s3.upload_payload.assert_called_once()
        mock_table.update_tile_status.assert_called()
        mock_table.update_tile_inference_info.assert_called_once()

    @patch("aws.osml.model_runner.tile_worker.batch_tile_workers.S3_MANAGER")
    @patch("aws.osml.model_runner.tile_worker.batch_tile_workers.TileRequestTable")
    @patch("aws.osml.model_runner.tile_worker.batch_tile_workers.ServiceConfig")
    def test_process_tile_submission_upload_failure(self, mock_config, mock_table_class, mock_s3):
        """Test tile upload with S3 failure"""
        # Setup mocks
        mock_config.batch_input_prefix = "batch/input/"
        mock_s3.upload_payload.side_effect = Exception("S3 upload failed")
        mock_table = MagicMock()
        mock_table_class.return_value = mock_table
        
        worker = BatchUploadWorker(
            worker_id=1,
            in_queue=self.in_queue,
            feature_detector=self.mock_detector,
        )
        worker.tile_request_table = mock_table
        
        tile_info = {
            "job_id": "test-job",
            "tile_id": "tile-123",
            "region_id": "region-456",
            "image_path": self.temp_file.name,
            "region": "test-region",
        }
        
        # Process tile
        result = worker.process_tile_submission(tile_info)
        
        assert result is False
        mock_table.update_tile_status.assert_called_with(
            "tile-123", "region-456", RequestStatus.FAILED, "Submission error: S3 upload failed"
        )

    @patch("aws.osml.model_runner.tile_worker.batch_tile_workers.S3_MANAGER")
    @patch("aws.osml.model_runner.tile_worker.batch_tile_workers.TileRequestTable")
    @patch("aws.osml.model_runner.tile_worker.batch_tile_workers.ServiceConfig")
    def test_process_tile_submission_status_update_failure(self, mock_config, mock_table_class, mock_s3):
        """Test tile upload when status update fails"""
        # Setup mocks
        mock_config.batch_input_prefix = "batch/input/"
        mock_config.batch_output_prefix = "batch/output/"
        mock_config.input_bucket = "test-bucket"
        mock_s3.upload_payload.return_value = "s3://bucket/input/tile.tif"
        mock_table = MagicMock()
        mock_table.update_tile_status.side_effect = Exception("DDB error")
        mock_table_class.return_value = mock_table
        
        worker = BatchUploadWorker(
            worker_id=1,
            in_queue=self.in_queue,
            feature_detector=self.mock_detector,
        )
        worker.tile_request_table = mock_table
        
        tile_info = {
            "job_id": "test-job",
            "tile_id": "tile-123",
            "region_id": "region-456",
            "image_path": self.temp_file.name,
            "region": "test-region",
        }
        
        # Process tile - should still return True despite status update failure
        result = worker.process_tile_submission(tile_info)
        
        assert result is True

    @patch("aws.osml.model_runner.tile_worker.batch_tile_workers.TileRequestTable")
    def test_stop_worker(self, mock_table_class):
        """Test stopping the worker"""
        worker = BatchUploadWorker(
            worker_id=1,
            in_queue=self.in_queue,
            feature_detector=self.mock_detector,
        )
        
        assert worker.running is True
        worker.stop()
        assert worker.running is False


class TestBatchSubmissionWorker(TestCase):
    """Test cases for BatchSubmissionWorker"""

    def setUp(self):
        """Set up test fixtures"""
        self.in_queue = Queue()
        self.mock_detector = MagicMock()

    @patch("aws.osml.model_runner.tile_worker.batch_tile_workers.TileRequestTable")
    def test_worker_initialization(self, mock_table_class):
        """Test worker initializes correctly"""
        worker = BatchSubmissionWorker(
            worker_id=1,
            in_queue=self.in_queue,
            feature_detector=self.mock_detector,
        )
        
        assert worker.worker_id == 1
        assert worker.in_queue == self.in_queue
        assert worker.feature_detector == self.mock_detector
        assert worker.failed_tile_count == 0
        assert worker.processed_tile_count == 0
        assert worker.running is True

    @patch("aws.osml.model_runner.tile_worker.batch_tile_workers.TileRequestTable")
    @patch("aws.osml.model_runner.tile_worker.batch_tile_workers.ServiceConfig")
    def test_process_tile_submission_success(self, mock_config, mock_table_class):
        """Test successful batch job submission"""
        # Setup mocks
        mock_config.input_bucket = "test-bucket"
        mock_config.batch_input_prefix = "batch/input/"
        mock_config.batch_output_prefix = "batch/output/"
        
        worker = BatchSubmissionWorker(
            worker_id=1,
            in_queue=self.in_queue,
            feature_detector=self.mock_detector,
        )
        
        job_info = {
            "job_id": "test-job-123",
            "instance_type": "ml.m5.xlarge",
            "instance_count": "2",
        }
        
        # Process job
        result = worker.process_tile_submission(job_info)
        
        assert result is True
        self.mock_detector._submit_batch_job.assert_called_once()

    @patch("aws.osml.model_runner.tile_worker.batch_tile_workers.TileRequestTable")
    @patch("aws.osml.model_runner.tile_worker.batch_tile_workers.ServiceConfig")
    def test_process_tile_submission_failure(self, mock_config, mock_table_class):
        """Test batch job submission failure"""
        # Setup mocks
        mock_config.input_bucket = "test-bucket"
        mock_config.batch_input_prefix = "batch/input/"
        mock_config.batch_output_prefix = "batch/output/"
        self.mock_detector._submit_batch_job.side_effect = Exception("Batch submission failed")
        
        worker = BatchSubmissionWorker(
            worker_id=1,
            in_queue=self.in_queue,
            feature_detector=self.mock_detector,
        )
        
        job_info = {
            "job_id": "test-job-123",
            "instance_type": "ml.m5.xlarge",
            "instance_count": "2",
        }
        
        # Process job
        result = worker.process_tile_submission(job_info)
        
        assert result is False

    @patch("aws.osml.model_runner.tile_worker.batch_tile_workers.TileRequestTable")
    def test_stop_worker(self, mock_table_class):
        """Test stopping the worker"""
        worker = BatchSubmissionWorker(
            worker_id=1,
            in_queue=self.in_queue,
            feature_detector=self.mock_detector,
        )
        
        assert worker.running is True
        worker.stop()
        assert worker.running is False
