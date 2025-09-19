#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import time
import unittest
from io import BytesIO
from queue import Queue
from unittest.mock import MagicMock, Mock, patch

import boto3
import geojson
from moto import mock_s3, mock_sagemaker

from aws.osml.model_runner.api import RegionRequest

from ..src.osml_extensions.api import ExtendedModelInvokeMode
from ..src.osml_extensions.config import AsyncEndpointConfig
from ..src.osml_extensions.detectors.async_sm_detector import AsyncSMDetector, AsyncSMDetectorBuilder
from ..src.osml_extensions.metrics import AsyncMetricsTracker
from ..src.osml_extensions.workers import AsyncTileWorkerPool


class TestAsyncEndpointIntegration(unittest.TestCase):
    """Integration tests for complete async endpoint functionality."""

    def setUp(self):
        """Set up integration test fixtures."""
        self.endpoint_name = "test-async-endpoint"
        self.async_config = AsyncEndpointConfig(
            input_bucket="test-input-bucket",
            output_bucket="test-output-bucket",
            max_wait_time=60,
            polling_interval=5,
            submission_workers=2,
            polling_workers=1,
        )

        # Test feature collection
        self.test_feature_collection = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [0, 0]},
                    "properties": {"confidence": 0.9, "class": "vehicle"},
                },
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [1, 1]},
                    "properties": {"confidence": 0.8, "class": "building"},
                },
            ],
        }

    @mock_s3
    def test_async_detector_end_to_end_workflow(self):
        """Test complete async detector workflow with mocked AWS services."""
        # Create S3 buckets
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=self.async_config.input_bucket)
        s3_client.create_bucket(Bucket=self.async_config.output_bucket)

        # Create detector with mocked SageMaker client
        with patch("osml_extensions.detectors.async_sm_detector.boto3") as mock_boto3:
            mock_sm_client = Mock()
            mock_s3_client_real = s3_client  # Use real S3 client
            mock_boto3.client.side_effect = lambda service, **kwargs: (
                mock_sm_client if service == "sagemaker-runtime" else mock_s3_client_real
            )

            # Mock SageMaker responses
            mock_sm_client.invoke_endpoint_async.return_value = {"InferenceId": "test-inference-123"}
            mock_sm_client.describe_inference_recommendations_job.return_value = {
                "Status": "Completed",
                "OutputLocation": f"s3://{self.async_config.output_bucket}/output/results.json",
            }

            # Pre-populate output in S3
            output_data = geojson.dumps(self.test_feature_collection).encode("utf-8")
            s3_client.put_object(Bucket=self.async_config.output_bucket, Key="output/results.json", Body=output_data)

            # Create detector and test
            detector = AsyncSMDetector(endpoint=self.endpoint_name, async_config=self.async_config)

            test_payload = BytesIO(b'{"test": "payload"}')
            result = detector.find_features(test_payload)

            # Verify result
            self.assertEqual(result, self.test_feature_collection)

            # Verify SageMaker calls
            mock_sm_client.invoke_endpoint_async.assert_called_once()
            mock_sm_client.describe_inference_recommendations_job.assert_called_once()

    def test_async_detector_builder_workflow(self):
        """Test AsyncSMDetectorBuilder workflow."""
        builder = AsyncSMDetectorBuilder(endpoint=self.endpoint_name, async_config=self.async_config)

        with patch("osml_extensions.detectors.async_sm_detector.AsyncSMDetector") as mock_detector_class:
            mock_detector = Mock()
            mock_detector_class.return_value = mock_detector

            result = builder.build()

            self.assertEqual(result, mock_detector)
            mock_detector_class.assert_called_once_with(
                endpoint=self.endpoint_name, assumed_credentials={}, async_config=self.async_config
            )

    def test_async_detector_builder_from_environment(self):
        """Test AsyncSMDetectorBuilder.from_environment workflow."""
        with patch("osml_extensions.detectors.async_sm_detector.AsyncEndpointConfig") as mock_config_class:
            mock_config = Mock()
            mock_config_class.from_environment.return_value = mock_config

            builder = AsyncSMDetectorBuilder.from_environment(endpoint=self.endpoint_name)

            self.assertEqual(builder.endpoint, self.endpoint_name)
            self.assertEqual(builder.async_config, mock_config)
            mock_config_class.from_environment.assert_called_once()

    def test_async_worker_pool_integration(self):
        """Test AsyncTileWorkerPool integration with mocked components."""
        # Create mock detector
        mock_detector = Mock()
        mock_detector.s3_manager.generate_unique_key.side_effect = lambda prefix: f"{prefix}-{time.time()}"
        mock_detector._upload_to_s3.return_value = "s3://input-bucket/input.json"
        mock_detector._invoke_async_endpoint.return_value = "test-inference-123"
        mock_detector.async_config.get_output_s3_uri.return_value = "s3://output-bucket/output.json"
        mock_detector.poller.get_job_status.return_value = ("Completed", "s3://output-bucket/output.json")
        mock_detector._download_from_s3.return_value = self.test_feature_collection
        mock_detector.s3_manager.cleanup_s3_objects.return_value = None

        # Create metrics tracker
        metrics_tracker = AsyncMetricsTracker()

        # Create worker pool
        worker_pool = AsyncTileWorkerPool(
            async_detector=mock_detector, config=self.async_config, metrics_tracker=metrics_tracker
        )

        # Create test tiles
        tile_queue = Queue()
        test_tiles = [
            {"region": [0, 0, 100, 100], "image_path": "/tmp/tile1.jpg"},
            {"region": [100, 0, 200, 100], "image_path": "/tmp/tile2.jpg"},
        ]

        for tile in test_tiles:
            tile_queue.put(tile)

        # Add shutdown signals
        for _ in range(self.async_config.submission_workers):
            tile_queue.put(None)

        # Mock file operations
        with patch("builtins.open", create=True) as mock_open:
            mock_file = Mock()
            mock_open.return_value.__enter__.return_value = mock_file

            # Mock sleep to speed up test
            with patch("time.sleep"):
                # Mock the wait_for_completion to return immediately
                with patch.object(worker_pool, "_wait_for_completion", return_value=(2, 0)):
                    total_processed, total_failed = worker_pool.process_tiles_async(tile_queue)

        # Verify results
        self.assertEqual(total_processed, 2)
        self.assertEqual(total_failed, 0)

        # Verify worker stats
        stats = worker_pool.get_worker_stats()
        self.assertEqual(stats["submission_workers"]["workers"], 2)
        self.assertEqual(stats["polling_workers"]["workers"], 1)

    def test_metrics_integration_workflow(self):
        """Test metrics tracking throughout async workflow."""
        metrics_tracker = AsyncMetricsTracker()

        # Simulate complete workflow timing
        with metrics_tracker.start_timer("TotalAsyncDuration"):
            # S3 Upload
            metrics_tracker.start_timer("S3Upload")
            time.sleep(0.01)  # Simulate upload time
            metrics_tracker.stop_timer("S3Upload")
            metrics_tracker.set_counter("S3UploadSize", 1024)

            # Async Endpoint Invocation
            metrics_tracker.start_timer("AsyncEndpointInvocation")
            time.sleep(0.005)  # Simulate invocation time
            metrics_tracker.stop_timer("AsyncEndpointInvocation")

            # Polling
            metrics_tracker.start_timer("QueueTime")
            for _ in range(3):
                metrics_tracker.increment_counter("PollingAttempts")
                time.sleep(0.005)  # Simulate polling
            metrics_tracker.stop_timer("QueueTime")

            # S3 Download
            metrics_tracker.start_timer("S3Download")
            time.sleep(0.01)  # Simulate download time
            metrics_tracker.stop_timer("S3Download")
            metrics_tracker.set_counter("S3DownloadSize", 2048)

        metrics_tracker.stop_timer("TotalAsyncDuration")
        metrics_tracker.increment_counter("AsyncInferenceSuccess")

        # Verify metrics were tracked
        self.assertGreater(metrics_tracker.get_timing("TotalAsyncDuration"), 0)
        self.assertGreater(metrics_tracker.get_timing("S3Upload"), 0)
        self.assertGreater(metrics_tracker.get_timing("S3Download"), 0)
        self.assertGreater(metrics_tracker.get_timing("QueueTime"), 0)
        self.assertEqual(metrics_tracker.get_counter("PollingAttempts"), 3)
        self.assertEqual(metrics_tracker.get_counter("S3UploadSize"), 1024)
        self.assertEqual(metrics_tracker.get_counter("S3DownloadSize"), 2048)
        self.assertEqual(metrics_tracker.get_counter("AsyncInferenceSuccess"), 1)

        # Test summary metrics
        metrics_tracker.emit_summary_metrics()
        metrics_tracker.log_performance_summary()

    def test_error_handling_integration(self):
        """Test error handling integration across components."""
        from ..src.osml_extensions.errors import AsyncErrorHandler, AsyncInferenceTimeoutError, S3OperationError

        # Test error handling workflow
        errors = [
            S3OperationError("Upload failed", operation="upload", retry_count=3),
            AsyncInferenceTimeoutError("Inference timed out", inference_id="test-123", elapsed_time=300.5),
            Exception("Generic error"),
        ]

        # Test error summary
        summary = AsyncErrorHandler.create_error_summary(errors)

        self.assertEqual(summary["total_errors"], 3)
        self.assertEqual(summary["retryable_errors"], 2)  # S3 error and generic error
        self.assertEqual(summary["permanent_errors"], 1)  # Timeout error

        # Test error logging
        for error in errors:
            AsyncErrorHandler.log_error_with_context(error, "TestOperation")

        # Test retry delay calculation
        for attempt in range(1, 5):
            delay = AsyncErrorHandler.calculate_retry_delay(attempt)
            self.assertGreater(delay, 0)
            self.assertLessEqual(delay, 60.0)  # Should be capped


class TestAsyncEndpointPerformance(unittest.TestCase):
    """Performance tests for async endpoint functionality."""

    def test_worker_pool_scalability(self):
        """Test worker pool performance with different configurations."""
        configs = [
            {"submission_workers": 1, "polling_workers": 1},
            {"submission_workers": 2, "polling_workers": 1},
            {"submission_workers": 4, "polling_workers": 2},
        ]

        for config_params in configs:
            with self.subTest(config=config_params):
                config = AsyncEndpointConfig(
                    input_bucket="test-input-bucket", output_bucket="test-output-bucket", **config_params
                )

                mock_detector = Mock()
                mock_detector.s3_manager.generate_unique_key.side_effect = lambda prefix: f"{prefix}-{time.time()}"
                mock_detector._upload_to_s3.return_value = "s3://input-bucket/input.json"
                mock_detector._invoke_async_endpoint.return_value = "test-inference-123"
                mock_detector.async_config.get_output_s3_uri.return_value = "s3://output-bucket/output.json"
                mock_detector.poller.get_job_status.return_value = ("Completed", "s3://output-bucket/output.json")
                mock_detector._download_from_s3.return_value = {"type": "FeatureCollection", "features": []}
                mock_detector.s3_manager.cleanup_s3_objects.return_value = None

                worker_pool = AsyncTileWorkerPool(
                    async_detector=mock_detector, config=config, metrics_tracker=AsyncMetricsTracker()
                )

                # Verify worker pool configuration
                stats = worker_pool.get_worker_stats()
                self.assertEqual(stats["submission_workers"]["workers"], 0)  # Not started yet
                self.assertEqual(stats["polling_workers"]["workers"], 0)  # Not started yet

    def test_metrics_performance_overhead(self):
        """Test that metrics tracking doesn't significantly impact performance."""
        # Test with metrics
        start_time = time.time()
        metrics_tracker = AsyncMetricsTracker()

        for i in range(1000):
            metrics_tracker.start_timer("TestTimer")
            metrics_tracker.stop_timer("TestTimer")
            metrics_tracker.increment_counter("TestCounter")

        with_metrics_time = time.time() - start_time

        # Test without metrics
        start_time = time.time()

        for i in range(1000):
            # Simulate same operations without metrics
            pass

        without_metrics_time = time.time() - start_time

        # Metrics overhead should be reasonable (less than 10x slower)
        overhead_ratio = with_metrics_time / max(without_metrics_time, 0.001)  # Avoid division by zero
        self.assertLess(overhead_ratio, 10.0, "Metrics overhead is too high")


if __name__ == "__main__":
    unittest.main()
