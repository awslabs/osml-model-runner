#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import os
import tempfile
import unittest
from queue import Queue
from unittest import TestCase
from unittest.mock import Mock, patch, MagicMock, mock_open

import pytest


class TestBatchUploadWorker(TestCase):
    """Unit tests for BatchUploadWorker class"""

    def test_worker_initialization(self):
        """Test BatchUploadWorker initialization"""
        from aws.osml.model_runner.tile_worker.batch_tile_workers import BatchUploadWorker

        in_queue = Queue()
        mock_detector = Mock()

        with patch("aws.osml.model_runner.tile_worker.batch_tile_workers.TileRequestTable"):
            worker = BatchUploadWorker(
                worker_id=1, in_queue=in_queue, feature_detector=mock_detector
            )

            assert worker.worker_id == 1
            assert worker.in_queue == in_queue
            assert worker.feature_detector == mock_detector
            assert worker.failed_tile_count == 0
            assert worker.processed_tile_count == 0
            assert worker.running is True

    def test_worker_name(self):
        """Test that worker has correct thread name"""
        from aws.osml.model_runner.tile_worker.batch_tile_workers import BatchUploadWorker

        in_queue = Queue()
        mock_detector = Mock()

        with patch("aws.osml.model_runner.tile_worker.batch_tile_workers.TileRequestTable"):
            worker = BatchUploadWorker(
                worker_id=3, in_queue=in_queue, feature_detector=mock_detector
            )

            assert worker.name == "BatchUploadWorker-3"

    def test_process_tile_submission_success(self):
        """Test successful tile submission"""
        from aws.osml.model_runner.tile_worker.batch_tile_workers import BatchUploadWorker

        in_queue = Queue()
        mock_detector = Mock()

        # Create a temporary file to simulate tile image
        with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.tif') as tmp_file:
            tmp_file.write(b"fake image data")
            tmp_path = tmp_file.name

        try:
            tile_info = {
                "job_id": "test-job-123",
                "tile_id": "test-tile-456",
                "region_id": "test-region-789",
                "image_path": tmp_path,
                "region": "test-region",
            }

            with patch("aws.osml.model_runner.tile_worker.batch_tile_workers.TileRequestTable"):
                with patch("aws.osml.model_runner.tile_worker.batch_tile_workers.S3_MANAGER") as mock_s3:
                    mock_s3.upload_payload.return_value = "s3://bucket/input/test.tif"

                    worker = BatchUploadWorker(
                        worker_id=1, in_queue=in_queue, feature_detector=mock_detector
                    )

                    result = worker.process_tile_submission(tile_info)

                    assert result is True
                    mock_s3.upload_payload.assert_called_once()

        finally:
            # Cleanup - file should be removed by the worker, but check anyway
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def test_process_tile_submission_failure(self):
        """Test tile submission failure handling"""
        from aws.osml.model_runner.tile_worker.batch_tile_workers import BatchUploadWorker

        in_queue = Queue()
        mock_detector = Mock()

        # Create a temporary file
        with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.tif') as tmp_file:
            tmp_file.write(b"fake image data")
            tmp_path = tmp_file.name

        try:
            tile_info = {
                "job_id": "test-job-123",
                "tile_id": "test-tile-456",
                "region_id": "test-region-789",
                "image_path": tmp_path,
                "region": "test-region",
            }

            with patch("aws.osml.model_runner.tile_worker.batch_tile_workers.TileRequestTable"):
                with patch("aws.osml.model_runner.tile_worker.batch_tile_workers.S3_MANAGER") as mock_s3:
                    # Simulate S3 upload failure
                    mock_s3.upload_payload.side_effect = Exception("S3 upload failed")

                    worker = BatchUploadWorker(
                        worker_id=1, in_queue=in_queue, feature_detector=mock_detector
                    )

                    result = worker.process_tile_submission(tile_info)

                    assert result is False

        finally:
            # Cleanup
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def test_stop_worker(self):
        """Test stopping the worker"""
        from aws.osml.model_runner.tile_worker.batch_tile_workers import BatchUploadWorker

        in_queue = Queue()
        mock_detector = Mock()

        with patch("aws.osml.model_runner.tile_worker.batch_tile_workers.TileRequestTable"):
            worker = BatchUploadWorker(
                worker_id=1, in_queue=in_queue, feature_detector=mock_detector
            )

            assert worker.running is True
            worker.stop()
            assert worker.running is False

    def test_shutdown_signal_handling(self):
        """Test that worker handles shutdown signal (None in queue)"""
        from aws.osml.model_runner.tile_worker.batch_tile_workers import BatchUploadWorker

        in_queue = Queue()
        mock_detector = Mock()

        with patch("aws.osml.model_runner.tile_worker.batch_tile_workers.TileRequestTable"):
            worker = BatchUploadWorker(
                worker_id=1, in_queue=in_queue, feature_detector=mock_detector
            )

            # Put shutdown signal in queue
            in_queue.put(None)

            # Verify worker can handle shutdown signal
            assert worker.running is True


class TestBatchSubmissionWorker(TestCase):
    """Unit tests for BatchSubmissionWorker class"""

    def test_worker_initialization(self):
        """Test BatchSubmissionWorker initialization"""
        from aws.osml.model_runner.tile_worker.batch_tile_workers import BatchSubmissionWorker

        in_queue = Queue()
        mock_detector = Mock()

        with patch("aws.osml.model_runner.tile_worker.batch_tile_workers.TileRequestTable"):
            worker = BatchSubmissionWorker(
                worker_id=1, in_queue=in_queue, feature_detector=mock_detector
            )

            assert worker.worker_id == 1
            assert worker.in_queue == in_queue
            assert worker.feature_detector == mock_detector
            assert worker.failed_tile_count == 0
            assert worker.processed_tile_count == 0
            assert worker.running is True

    def test_worker_name(self):
        """Test that worker has correct thread name"""
        from aws.osml.model_runner.tile_worker.batch_tile_workers import BatchSubmissionWorker

        in_queue = Queue()
        mock_detector = Mock()

        with patch("aws.osml.model_runner.tile_worker.batch_tile_workers.TileRequestTable"):
            worker = BatchSubmissionWorker(
                worker_id=2, in_queue=in_queue, feature_detector=mock_detector
            )

            assert worker.name == "BatchSubmissionWorker-2"

    def test_process_tile_submission_success(self):
        """Test successful batch job submission"""
        from aws.osml.model_runner.tile_worker.batch_tile_workers import BatchSubmissionWorker

        in_queue = Queue()
        mock_detector = Mock()
        mock_detector._submit_batch_job = Mock()

        job_info = {
            "job_id": "test-job-123",
            "instance_type": "ml.m5.xlarge",
            "instance_count": "2",
        }

        with patch("aws.osml.model_runner.tile_worker.batch_tile_workers.TileRequestTable"):
            worker = BatchSubmissionWorker(
                worker_id=1, in_queue=in_queue, feature_detector=mock_detector
            )

            result = worker.process_tile_submission(job_info)

            assert result is True
            mock_detector._submit_batch_job.assert_called_once()

            # Verify the call arguments
            call_args = mock_detector._submit_batch_job.call_args
            assert "batch-test-job-123" in call_args[0]  # transform_job_name
            assert "ml.m5.xlarge" in call_args[1].values()  # instance_type
            assert 2 in call_args[1].values()  # instance_count

    def test_process_tile_submission_failure(self):
        """Test batch job submission failure handling"""
        from aws.osml.model_runner.tile_worker.batch_tile_workers import BatchSubmissionWorker

        in_queue = Queue()
        mock_detector = Mock()
        mock_detector._submit_batch_job = Mock(side_effect=Exception("Batch job submission failed"))

        job_info = {
            "job_id": "test-job-123",
            "instance_type": "ml.m5.xlarge",
            "instance_count": "1",
        }

        with patch("aws.osml.model_runner.tile_worker.batch_tile_workers.TileRequestTable"):
            worker = BatchSubmissionWorker(
                worker_id=1, in_queue=in_queue, feature_detector=mock_detector
            )

            result = worker.process_tile_submission(job_info)

            assert result is False

    def test_process_tile_submission_with_multiple_instances(self):
        """Test batch job submission with multiple instances"""
        from aws.osml.model_runner.tile_worker.batch_tile_workers import BatchSubmissionWorker

        in_queue = Queue()
        mock_detector = Mock()
        mock_detector._submit_batch_job = Mock()

        job_info = {
            "job_id": "test-job-456",
            "instance_type": "ml.p3.2xlarge",
            "instance_count": "5",
        }

        with patch("aws.osml.model_runner.tile_worker.batch_tile_workers.TileRequestTable"):
            worker = BatchSubmissionWorker(
                worker_id=1, in_queue=in_queue, feature_detector=mock_detector
            )

            result = worker.process_tile_submission(job_info)

            assert result is True

            # Verify instance count is converted to int
            call_args = mock_detector._submit_batch_job.call_args
            assert 5 in call_args[1].values()

    def test_stop_worker(self):
        """Test stopping the worker"""
        from aws.osml.model_runner.tile_worker.batch_tile_workers import BatchSubmissionWorker

        in_queue = Queue()
        mock_detector = Mock()

        with patch("aws.osml.model_runner.tile_worker.batch_tile_workers.TileRequestTable"):
            worker = BatchSubmissionWorker(
                worker_id=1, in_queue=in_queue, feature_detector=mock_detector
            )

            assert worker.running is True
            worker.stop()
            assert worker.running is False

    def test_shutdown_signal_handling(self):
        """Test that worker handles shutdown signal (None in queue)"""
        from aws.osml.model_runner.tile_worker.batch_tile_workers import BatchSubmissionWorker

        in_queue = Queue()
        mock_detector = Mock()

        with patch("aws.osml.model_runner.tile_worker.batch_tile_workers.TileRequestTable"):
            worker = BatchSubmissionWorker(
                worker_id=1, in_queue=in_queue, feature_detector=mock_detector
            )

            # Put shutdown signal in queue
            in_queue.put(None)

            # Verify worker can handle shutdown signal
            assert worker.running is True


if __name__ == "__main__":
    unittest.main()
