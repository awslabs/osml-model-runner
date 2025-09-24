#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import time
import unittest
from queue import Queue
from unittest.mock import Mock, patch

from ..src.osml_extensions.config import AsyncEndpointConfig
from ..src.osml_extensions.metrics import AsyncMetricsTracker
from ..src.osml_extensions.workers.async_tile_worker_pool import (
    AsyncInferenceJob,
    AsyncPollingWorker,
    AsyncSubmissionWorker,
    AsyncTileWorkerPool,
)


class TestAsyncInferenceJob(unittest.TestCase):
    """Test cases for AsyncInferenceJob."""

    def test_initialization(self):
        """Test AsyncInferenceJob initialization."""
        inference_id = "test-inference-123"
        tile_info = {"region": [0, 0, 100, 100], "image_path": "/tmp/test.jpg"}
        input_s3_uri = "s3://input-bucket/input.json"
        output_s3_uri = "s3://output-bucket/output.json"
        submitted_time = time.time()

        job = AsyncInferenceJob(
            inference_id=inference_id,
            tile_info=tile_info,
            input_s3_uri=input_s3_uri,
            output_s3_uri=output_s3_uri,
            submitted_time=submitted_time,
        )

        self.assertEqual(job.inference_id, inference_id)
        self.assertEqual(job.tile_info, tile_info)
        self.assertEqual(job.input_s3_uri, input_s3_uri)
        self.assertEqual(job.output_s3_uri, output_s3_uri)
        self.assertEqual(job.submitted_time, submitted_time)
        self.assertEqual(job.poll_count, 0)
        self.assertEqual(job.last_poll_time, submitted_time)


class TestAsyncSubmissionWorker(unittest.TestCase):
    """Test cases for AsyncSubmissionWorker."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = AsyncEndpointConfig(
            input_bucket="test-input-bucket", output_bucket="test-output-bucket", job_queue_timeout=5
        )

        self.tile_queue = Queue()
        self.job_queue = Queue()
        self.mock_detector = Mock()
        self.mock_metrics = Mock()

        # Setup mock detector methods
        self.mock_detector.s3_manager.generate_unique_key.side_effect = ["input-key", "output-key"]
        self.mock_detector._upload_to_s3.return_value = "s3://input-bucket/input-key"
        self.mock_detector._invoke_async_endpoint.return_value = "test-inference-123"
        self.mock_detector.async_config.get_output_s3_uri.return_value = "s3://output-bucket/output-key"

        self.worker = AsyncSubmissionWorker(
            worker_id=1,
            tile_queue=self.tile_queue,
            job_queue=self.job_queue,
            async_detector=self.mock_detector,
            config=self.config,
            metrics_tracker=self.mock_metrics,
        )

    def test_initialization(self):
        """Test AsyncSubmissionWorker initialization."""
        self.assertEqual(self.worker.worker_id, 1)
        self.assertEqual(self.worker.tile_queue, self.tile_queue)
        self.assertEqual(self.worker.job_queue, self.job_queue)
        self.assertEqual(self.worker.async_detector, self.mock_detector)
        self.assertEqual(self.worker.config, self.config)
        self.assertEqual(self.worker.failed_tile_count, 0)
        self.assertEqual(self.worker.processed_tile_count, 0)
        self.assertTrue(self.worker.running)

    @patch("builtins.open", create=True)
    def test_process_tile_submission_success(self, mock_open):
        """Test successful tile submission."""
        mock_file = Mock()
        mock_open.return_value.__enter__.return_value = mock_file

        tile_info = {
            "region": [0, 0, 100, 100],
            "image_path": "/tmp/test.jpg",
            "image_id": "test-image",
            "job_id": "test-job",
            "region_id": "test-region",
        }

        result = self.worker.process_tile_submission(tile_info)

        self.assertTrue(result)
        self.mock_detector._upload_to_s3.assert_called_once()
        self.mock_detector._invoke_async_endpoint.assert_called_once()

        # Check that job was added to queue
        self.assertEqual(self.job_queue.qsize(), 1)
        job = self.job_queue.get()
        self.assertEqual(job.inference_id, "test-inference-123")
        self.assertEqual(job.tile_info, tile_info)

    @patch("builtins.open", create=True)
    def test_process_tile_submission_upload_failure(self, mock_open):
        """Test tile submission with upload failure."""
        mock_file = Mock()
        mock_open.return_value.__enter__.return_value = mock_file

        # Mock upload failure
        self.mock_detector._upload_to_s3.side_effect = Exception("Upload failed")

        tile_info = {"region": [0, 0, 100, 100], "image_path": "/tmp/test.jpg"}

        result = self.worker.process_tile_submission(tile_info)

        self.assertFalse(result)
        self.assertEqual(self.job_queue.qsize(), 0)

    @patch("builtins.open", create=True)
    def test_process_tile_submission_endpoint_failure(self, mock_open):
        """Test tile submission with endpoint failure."""
        mock_file = Mock()
        mock_open.return_value.__enter__.return_value = mock_file

        # Mock endpoint failure
        self.mock_detector._invoke_async_endpoint.side_effect = Exception("Endpoint failed")

        tile_info = {"region": [0, 0, 100, 100], "image_path": "/tmp/test.jpg"}

        result = self.worker.process_tile_submission(tile_info)

        self.assertFalse(result)
        self.assertEqual(self.job_queue.qsize(), 0)

    def test_stop(self):
        """Test worker stop functionality."""
        self.assertTrue(self.worker.running)
        self.worker.stop()
        self.assertFalse(self.worker.running)


class TestAsyncPollingWorker(unittest.TestCase):
    """Test cases for AsyncPollingWorker."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = AsyncEndpointConfig(
            input_bucket="test-input-bucket",
            output_bucket="test-output-bucket",
            max_concurrent_jobs=10,
            max_wait_time=300,
            polling_interval=10,
            max_polling_interval=60,
            exponential_backoff_multiplier=2.0,
        )

        self.job_queue = Queue()
        self.result_queue = Queue()
        self.mock_detector = Mock()
        self.mock_metrics = Mock()

        # Setup mock detector methods
        self.mock_detector.poller.get_job_status.return_value = ("InProgress", None)
        self.mock_detector._download_from_s3.return_value = {"type": "FeatureCollection", "features": []}
        self.mock_detector.s3_manager.cleanup_s3_objects.return_value = None

        self.worker = AsyncPollingWorker(
            worker_id=1,
            job_queue=self.job_queue,
            result_queue=self.result_queue,
            async_detector=self.mock_detector,
            config=self.config,
            metrics_tracker=self.mock_metrics,
        )

    def test_initialization(self):
        """Test AsyncPollingWorker initialization."""
        self.assertEqual(self.worker.worker_id, 1)
        self.assertEqual(self.worker.job_queue, self.job_queue)
        self.assertEqual(self.worker.result_queue, self.result_queue)
        self.assertEqual(self.worker.async_detector, self.mock_detector)
        self.assertEqual(self.worker.config, self.config)
        self.assertEqual(len(self.worker.active_jobs), 0)
        self.assertEqual(self.worker.completed_job_count, 0)
        self.assertEqual(self.worker.failed_job_count, 0)
        self.assertTrue(self.worker.running)

    def test_collect_new_jobs(self):
        """Test collecting new jobs from queue."""
        # Add jobs to queue
        job1 = AsyncInferenceJob("job1", {}, "s3://input1", "s3://output1", time.time())
        job2 = AsyncInferenceJob("job2", {}, "s3://input2", "s3://output2", time.time())

        self.job_queue.put(job1)
        self.job_queue.put(job2)

        # Collect jobs
        self.worker._collect_new_jobs()

        # Verify jobs were collected
        self.assertEqual(len(self.worker.active_jobs), 2)
        self.assertIn("job1", self.worker.active_jobs)
        self.assertIn("job2", self.worker.active_jobs)

    def test_calculate_polling_interval(self):
        """Test polling interval calculation."""
        job = AsyncInferenceJob("test-job", {}, "s3://input", "s3://output", time.time())

        # Test initial interval
        interval1 = self.worker._calculate_polling_interval(job)
        self.assertEqual(interval1, 10.0)  # base interval

        # Test after first poll
        job.poll_count = 1
        interval2 = self.worker._calculate_polling_interval(job)
        self.assertEqual(interval2, 20.0)  # base * 2^1

        # Test after second poll
        job.poll_count = 2
        interval3 = self.worker._calculate_polling_interval(job)
        self.assertEqual(interval3, 40.0)  # base * 2^2

        # Test capping at max interval
        job.poll_count = 10
        interval_max = self.worker._calculate_polling_interval(job)
        self.assertEqual(interval_max, 60.0)  # capped at max_polling_interval

    def test_process_completed_job(self):
        """Test processing a completed job."""
        job = AsyncInferenceJob(
            "test-job", {"region": [0, 0, 100, 100]}, "s3://input", "s3://output", time.time() - 10  # 10 seconds ago
        )
        job.poll_count = 3

        output_location = "s3://output-bucket/results.json"

        self.worker._process_completed_job(job, output_location)

        # Verify result was added to queue
        self.assertEqual(self.result_queue.qsize(), 1)
        result = self.result_queue.get()

        self.assertEqual(result["tile_info"], job.tile_info)
        self.assertEqual(result["inference_id"], job.inference_id)
        self.assertEqual(result["poll_count"], 3)
        self.assertGreater(result["processing_time"], 0)

        # Verify cleanup was called
        self.mock_detector.s3_manager.cleanup_s3_objects.assert_called_once()

    def test_handle_failed_job(self):
        """Test handling a failed job."""
        job = AsyncInferenceJob("test-job", {}, "s3://input", "s3://output", time.time())

        self.worker._handle_failed_job(job, "Test failure reason")

        # Verify cleanup was called
        self.mock_detector.s3_manager.cleanup_s3_objects.assert_called_once()

    def test_stop(self):
        """Test worker stop functionality."""
        self.assertTrue(self.worker.running)
        self.worker.stop()
        self.assertFalse(self.worker.running)


class TestAsyncTileWorkerPool(unittest.TestCase):
    """Test cases for AsyncTileWorkerPool."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = AsyncEndpointConfig(
            input_bucket="test-input-bucket",
            output_bucket="test-output-bucket",
            submission_workers=2,
            polling_workers=1,
            max_concurrent_jobs=10,
        )

        self.mock_detector = Mock()
        self.mock_metrics = Mock()

        self.pool = AsyncTileWorkerPool(
            async_detector=self.mock_detector, config=self.config, metrics_tracker=self.mock_metrics
        )

    def test_initialization(self):
        """Test AsyncTileWorkerPool initialization."""
        self.assertEqual(self.pool.async_detector, self.mock_detector)
        self.assertEqual(self.pool.config, self.config)
        self.assertEqual(self.pool.metrics_tracker, self.mock_metrics)
        self.assertEqual(len(self.pool.submission_workers), 0)
        self.assertEqual(len(self.pool.polling_workers), 0)

    @patch("osml_extensions.workers.async_tile_worker_pool.AsyncSubmissionWorker")
    @patch("osml_extensions.workers.async_tile_worker_pool.AsyncPollingWorker")
    def test_start_workers(self, mock_polling_worker_class, mock_submission_worker_class):
        """Test starting workers."""
        # Setup mocks
        mock_submission_workers = [Mock() for _ in range(2)]
        mock_polling_workers = [Mock()]

        mock_submission_worker_class.side_effect = mock_submission_workers
        mock_polling_worker_class.side_effect = mock_polling_workers

        tile_queue = Queue()

        self.pool._start_workers(tile_queue)

        # Verify workers were created and started
        self.assertEqual(len(self.pool.submission_workers), 2)
        self.assertEqual(len(self.pool.polling_workers), 1)

        for worker in mock_submission_workers:
            worker.start.assert_called_once()

        for worker in mock_polling_workers:
            worker.start.assert_called_once()

    def test_stop_workers(self):
        """Test stopping workers."""
        # Create mock workers
        mock_submission_worker = Mock()
        mock_polling_worker = Mock()

        self.pool.submission_workers = [mock_submission_worker]
        self.pool.polling_workers = [mock_polling_worker]

        self.pool._stop_workers()

        # Verify workers were stopped
        mock_submission_worker.stop.assert_called_once()
        mock_polling_worker.stop.assert_called_once()
        mock_submission_worker.join.assert_called_once()
        mock_polling_worker.join.assert_called_once()

    def test_get_worker_stats(self):
        """Test getting worker statistics."""
        # Create mock workers with stats
        mock_submission_worker1 = Mock()
        mock_submission_worker1.processed_tile_count = 5
        mock_submission_worker1.failed_tile_count = 1

        mock_submission_worker2 = Mock()
        mock_submission_worker2.processed_tile_count = 3
        mock_submission_worker2.failed_tile_count = 0

        mock_polling_worker = Mock()
        mock_polling_worker.completed_job_count = 7
        mock_polling_worker.failed_job_count = 1
        mock_polling_worker.active_jobs = {"job1": Mock(), "job2": Mock()}

        self.pool.submission_workers = [mock_submission_worker1, mock_submission_worker2]
        self.pool.polling_workers = [mock_polling_worker]

        stats = self.pool.get_worker_stats()

        # Verify stats
        self.assertEqual(stats["submission_workers"]["total_processed"], 8)
        self.assertEqual(stats["submission_workers"]["total_failed"], 1)
        self.assertEqual(stats["submission_workers"]["workers"], 2)

        self.assertEqual(stats["polling_workers"]["total_completed"], 7)
        self.assertEqual(stats["polling_workers"]["total_failed"], 1)
        self.assertEqual(stats["polling_workers"]["active_jobs"], 2)
        self.assertEqual(stats["polling_workers"]["workers"], 1)

    def test_process_result(self):
        """Test processing a completed result."""
        result = {
            "tile_info": {"region": [0, 0, 100, 100]},
            "feature_collection": {"type": "FeatureCollection", "features": [{"type": "Feature"}]},
            "inference_id": "test-job",
            "processing_time": 15.5,
            "poll_count": 3,
        }

        # Should not raise exception
        self.pool._process_result(result)

        # Verify metrics were updated
        self.mock_metrics.increment_counter.assert_called()
        self.mock_metrics.set_counter.assert_called()


class TestAsyncTileWorkerPoolIntegration(unittest.TestCase):
    """Integration tests for AsyncTileWorkerPool."""

    def setUp(self):
        """Set up integration test fixtures."""
        self.config = AsyncEndpointConfig(
            input_bucket="test-input-bucket",
            output_bucket="test-output-bucket",
            submission_workers=1,
            polling_workers=1,
            max_concurrent_jobs=5,
            max_wait_time=30,
        )

        self.mock_detector = Mock()
        self.mock_metrics = AsyncMetricsTracker()

        # Setup mock detector for successful processing
        self.mock_detector.s3_manager.generate_unique_key.side_effect = lambda prefix: f"{prefix}-{time.time()}"
        self.mock_detector._upload_to_s3.return_value = "s3://input-bucket/input.json"
        self.mock_detector._invoke_async_endpoint.return_value = "test-inference-123"
        self.mock_detector.async_config.get_output_s3_uri.return_value = "s3://output-bucket/output.json"
        self.mock_detector.poller.get_job_status.return_value = ("Completed", "s3://output-bucket/output.json")
        self.mock_detector._download_from_s3.return_value = {"type": "FeatureCollection", "features": []}
        self.mock_detector.s3_manager.cleanup_s3_objects.return_value = None

        self.pool = AsyncTileWorkerPool(
            async_detector=self.mock_detector, config=self.config, metrics_tracker=self.mock_metrics
        )

    @patch("builtins.open", create=True)
    @patch("time.sleep")  # Speed up test by mocking sleep
    def test_complete_workflow_simulation(self, mock_sleep, mock_open):
        """Test complete async tile processing workflow simulation."""
        mock_file = Mock()
        mock_open.return_value.__enter__.return_value = mock_file

        # Create test tiles
        tile_queue = Queue()
        test_tiles = [
            {"region": [0, 0, 100, 100], "image_path": "/tmp/tile1.jpg"},
            {"region": [100, 0, 200, 100], "image_path": "/tmp/tile2.jpg"},
        ]

        for tile in test_tiles:
            tile_queue.put(tile)

        # Add shutdown signals
        tile_queue.put(None)  # For submission worker

        # Process tiles (this would normally take longer in real scenario)
        with patch.object(self.pool, "_wait_for_completion", return_value=(2, 0)):
            total_processed, total_failed = self.pool.process_tiles_async(tile_queue)

        # Verify results
        self.assertEqual(total_processed, 2)
        self.assertEqual(total_failed, 0)


class TestAsyncTileWorkerPoolResourceManagement(unittest.TestCase):
    """Test cases for AsyncTileWorkerPool resource management functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = AsyncEndpointConfig(
            input_bucket="test-input-bucket",
            output_bucket="test-output-bucket",
            submission_workers=2,
            polling_workers=1,
            cleanup_enabled=True,
        )

        self.mock_detector = Mock()
        self.mock_detector.s3_client = Mock()

        # Mock the resource manager
        with patch("osml_extensions.workers.async_tile_worker_pool.ResourceManager") as mock_rm_class:
            self.mock_resource_manager = Mock()
            mock_rm_class.return_value = self.mock_resource_manager

            self.pool = AsyncTileWorkerPool(async_detector=self.mock_detector, config=self.config)

    def test_resource_manager_initialization(self):
        """Test that ResourceManager is properly initialized in worker pool."""
        # Verify ResourceManager was created
        self.assertIsNotNone(self.pool.resource_manager)

    @patch("osml_extensions.workers.async_tile_worker_pool.AsyncSubmissionWorker")
    @patch("osml_extensions.workers.async_tile_worker_pool.AsyncPollingWorker")
    def test_worker_thread_registration(self, mock_polling_worker_class, mock_submission_worker_class):
        """Test that worker threads are registered for resource management."""
        # Create mock workers
        mock_submission_worker = Mock()
        mock_submission_worker.start.return_value = None
        mock_submission_worker_class.return_value = mock_submission_worker

        mock_polling_worker = Mock()
        mock_polling_worker.start.return_value = None
        mock_polling_worker_class.return_value = mock_polling_worker

        # Create tile queue
        tile_queue = Queue()
        tile_queue.put(None)  # Shutdown signal

        # Start workers
        self.pool._start_workers(tile_queue)

        # Verify workers were registered with resource manager
        expected_calls = self.config.submission_workers + self.config.polling_workers
        self.assertEqual(self.mock_resource_manager.register_worker_thread.call_count, expected_calls)

    @patch("osml_extensions.workers.async_tile_worker_pool.ResourceType")
    def test_worker_cleanup_on_stop(self, mock_resource_type):
        """Test that worker threads are cleaned up when pool stops."""
        # Mock workers
        mock_submission_worker = Mock()
        mock_submission_worker.join.return_value = None
        mock_submission_worker.is_alive.return_value = False
        self.pool.submission_workers = [mock_submission_worker]

        mock_polling_worker = Mock()
        mock_polling_worker.join.return_value = None
        mock_polling_worker.is_alive.return_value = False
        self.pool.polling_workers = [mock_polling_worker]

        # Stop workers
        self.pool._stop_workers()

        # Verify resource cleanup was called
        self.mock_resource_manager.cleanup_all_resources.assert_called_once()
        self.mock_resource_manager.stop_cleanup_worker.assert_called_once()

    def test_get_worker_stats_with_resource_info(self):
        """Test worker statistics include resource management info."""
        # Mock workers with stats
        mock_submission_worker = Mock()
        mock_submission_worker.processed_tile_count = 5
        mock_submission_worker.failed_tile_count = 1
        self.pool.submission_workers = [mock_submission_worker]

        mock_polling_worker = Mock()
        mock_polling_worker.completed_job_count = 4
        mock_polling_worker.failed_job_count = 1
        mock_polling_worker.active_jobs = {"job1": Mock(), "job2": Mock()}
        self.pool.polling_workers = [mock_polling_worker]

        # Mock queue sizes
        self.pool.job_queue.qsize = Mock(return_value=2)
        self.pool.result_queue.qsize = Mock(return_value=1)

        stats = self.pool.get_worker_stats()

        # Verify stats structure
        self.assertIn("submission_workers", stats)
        self.assertIn("polling_workers", stats)
        self.assertEqual(stats["submission_workers"]["total_processed"], 5)
        self.assertEqual(stats["polling_workers"]["active_jobs"], 2)
        self.assertEqual(stats["job_queue_size"], 2)
        self.assertEqual(stats["result_queue_size"], 1)


if __name__ == "__main__":
    unittest.main()
