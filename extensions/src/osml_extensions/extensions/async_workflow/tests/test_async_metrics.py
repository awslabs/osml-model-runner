#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import time
import unittest
from unittest.mock import Mock, patch

from ..src.osml_extensions.metrics import AsyncMetricsContext, AsyncMetricsTracker


class TestAsyncMetricsTracker(unittest.TestCase):
    """Test cases for AsyncMetricsTracker."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_metrics_logger = Mock()
        self.tracker = AsyncMetricsTracker(self.mock_metrics_logger)

    def test_initialization(self):
        """Test AsyncMetricsTracker initialization."""
        self.assertEqual(self.tracker.metrics_logger, self.mock_metrics_logger)

        # Check that all timing categories are initialized
        expected_timings = [
            "S3Upload",
            "S3Download",
            "AsyncEndpointInvocation",
            "QueueTime",
            "TotalAsyncDuration",
            "PollingDuration",
        ]
        for category in expected_timings:
            self.assertEqual(self.tracker.timings[category], 0.0)

        # Check that all counter categories are initialized
        expected_counters = [
            "PollingAttempts",
            "S3UploadRetries",
            "S3DownloadRetries",
            "AsyncInferenceSuccess",
            "AsyncInferenceFailures",
            "S3UploadSize",
            "S3DownloadSize",
        ]
        for category in expected_counters:
            self.assertEqual(self.tracker.counters[category], 0)

    def test_initialization_without_logger(self):
        """Test AsyncMetricsTracker initialization without logger."""
        tracker = AsyncMetricsTracker()
        self.assertIsNone(tracker.metrics_logger)

    def test_start_stop_timer(self):
        """Test timer start and stop functionality."""
        category = "S3Upload"

        # Start timer
        self.tracker.start_timer(category)
        self.assertIn(category, self.tracker.start_times)

        # Wait a small amount
        time.sleep(0.01)

        # Stop timer
        elapsed = self.tracker.stop_timer(category)

        # Verify timing was recorded
        self.assertGreater(elapsed, 0)
        self.assertEqual(self.tracker.timings[category], elapsed)

        # Verify metric was emitted
        self.mock_metrics_logger.put_metric.assert_called()

    def test_start_timer_unknown_category(self):
        """Test starting timer with unknown category."""
        with patch("osml_extensions.metrics.async_metrics.logger") as mock_logger:
            self.tracker.start_timer("UnknownCategory")
            mock_logger.warning.assert_called_with("Unknown timing category: UnknownCategory")

    def test_stop_timer_unknown_category(self):
        """Test stopping timer with unknown category."""
        with patch("osml_extensions.metrics.async_metrics.logger") as mock_logger:
            result = self.tracker.stop_timer("UnknownCategory")
            self.assertEqual(result, 0.0)
            mock_logger.warning.assert_called_with("Unknown timing category: UnknownCategory")

    def test_stop_timer_not_started(self):
        """Test stopping timer that was not started."""
        with patch("osml_extensions.metrics.async_metrics.logger") as mock_logger:
            result = self.tracker.stop_timer("S3Upload")
            self.assertEqual(result, 0.0)
            mock_logger.warning.assert_called_with("Timer for S3Upload was not started")

    def test_increment_counter(self):
        """Test counter increment functionality."""
        category = "PollingAttempts"

        # Increment by default (1)
        self.tracker.increment_counter(category)
        self.assertEqual(self.tracker.counters[category], 1)

        # Increment by specific value
        self.tracker.increment_counter(category, 5)
        self.assertEqual(self.tracker.counters[category], 6)

        # Verify metrics were emitted
        self.assertEqual(self.mock_metrics_logger.put_metric.call_count, 2)

    def test_increment_counter_unknown_category(self):
        """Test incrementing counter with unknown category."""
        with patch("osml_extensions.metrics.async_metrics.logger") as mock_logger:
            self.tracker.increment_counter("UnknownCounter")
            mock_logger.warning.assert_called_with("Unknown counter category: UnknownCounter")

    def test_set_counter(self):
        """Test counter set functionality."""
        category = "S3UploadSize"
        value = 1024

        self.tracker.set_counter(category, value)
        self.assertEqual(self.tracker.counters[category], value)

        # Verify metric was emitted
        self.mock_metrics_logger.put_metric.assert_called()

    def test_set_counter_unknown_category(self):
        """Test setting counter with unknown category."""
        with patch("osml_extensions.metrics.async_metrics.logger") as mock_logger:
            self.tracker.set_counter("UnknownCounter", 10)
            mock_logger.warning.assert_called_with("Unknown counter category: UnknownCounter")

    def test_get_timing(self):
        """Test getting timing values."""
        category = "S3Upload"
        expected_time = 1.5

        self.tracker.timings[category] = expected_time
        result = self.tracker.get_timing(category)

        self.assertEqual(result, expected_time)

    def test_get_timing_unknown_category(self):
        """Test getting timing for unknown category."""
        result = self.tracker.get_timing("UnknownCategory")
        self.assertEqual(result, 0.0)

    def test_get_counter(self):
        """Test getting counter values."""
        category = "PollingAttempts"
        expected_count = 5

        self.tracker.counters[category] = expected_count
        result = self.tracker.get_counter(category)

        self.assertEqual(result, expected_count)

    def test_get_counter_unknown_category(self):
        """Test getting counter for unknown category."""
        result = self.tracker.get_counter("UnknownCounter")
        self.assertEqual(result, 0)

    def test_get_all_timings(self):
        """Test getting all timing values."""
        # Set some test values
        self.tracker.timings["S3Upload"] = 1.0
        self.tracker.timings["S3Download"] = 2.0

        result = self.tracker.get_all_timings()

        # Should be a copy, not the original dict
        self.assertIsNot(result, self.tracker.timings)
        self.assertEqual(result["S3Upload"], 1.0)
        self.assertEqual(result["S3Download"], 2.0)

    def test_get_all_counters(self):
        """Test getting all counter values."""
        # Set some test values
        self.tracker.counters["PollingAttempts"] = 3
        self.tracker.counters["S3UploadSize"] = 1024

        result = self.tracker.get_all_counters()

        # Should be a copy, not the original dict
        self.assertIsNot(result, self.tracker.counters)
        self.assertEqual(result["PollingAttempts"], 3)
        self.assertEqual(result["S3UploadSize"], 1024)

    def test_emit_summary_metrics(self):
        """Test emitting summary metrics."""
        # Set up test data
        self.tracker.timings["TotalAsyncDuration"] = 10.0
        self.tracker.timings["S3Upload"] = 2.0
        self.tracker.timings["S3Download"] = 1.0
        self.tracker.timings["QueueTime"] = 6.0
        self.tracker.counters["PollingAttempts"] = 3
        self.tracker.counters["S3UploadSize"] = 1024
        self.tracker.counters["S3DownloadSize"] = 2048

        self.tracker.emit_summary_metrics()

        # Verify summary metrics were emitted
        self.mock_metrics_logger.put_metric.assert_called()

        # Check that percentage metrics were calculated
        call_args_list = self.mock_metrics_logger.put_metric.call_args_list
        metric_names = [call[0][0] for call in call_args_list]

        self.assertIn("S3UploadPercentage", metric_names)
        self.assertIn("S3DownloadPercentage", metric_names)
        self.assertIn("QueueTimePercentage", metric_names)
        self.assertIn("AveragePollingInterval", metric_names)

    def test_emit_summary_metrics_without_logger(self):
        """Test emitting summary metrics without logger."""
        tracker = AsyncMetricsTracker()  # No logger

        # Should not raise exception
        tracker.emit_summary_metrics()

    def test_log_performance_summary(self):
        """Test logging performance summary."""
        # Set up test data
        self.tracker.timings["TotalAsyncDuration"] = 10.0
        self.tracker.timings["S3Upload"] = 2.0
        self.tracker.timings["S3Download"] = 1.0
        self.tracker.timings["QueueTime"] = 6.0
        self.tracker.counters["PollingAttempts"] = 3

        with patch("osml_extensions.metrics.async_metrics.logger") as mock_logger:
            self.tracker.log_performance_summary()

            # Verify info log was called
            mock_logger.info.assert_called()

            # Check log message contains expected information
            log_message = mock_logger.info.call_args[0][0]
            self.assertIn("Total=10.000s", log_message)
            self.assertIn("Upload=2.000s", log_message)
            self.assertIn("Queue=6.000s", log_message)
            self.assertIn("Download=1.000s", log_message)
            self.assertIn("Polls=3", log_message)

    def test_reset(self):
        """Test resetting all metrics."""
        # Set some test values
        self.tracker.timings["S3Upload"] = 1.0
        self.tracker.counters["PollingAttempts"] = 5
        self.tracker.start_times["S3Download"] = time.time()

        # Reset
        self.tracker.reset()

        # Verify all values are reset
        self.assertEqual(self.tracker.timings["S3Upload"], 0.0)
        self.assertEqual(self.tracker.counters["PollingAttempts"], 0)
        self.assertEqual(len(self.tracker.start_times), 0)


class TestAsyncMetricsContext(unittest.TestCase):
    """Test cases for AsyncMetricsContext."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_metrics_logger = Mock()
        self.tracker = AsyncMetricsTracker(self.mock_metrics_logger)
        self.category = "S3Upload"

    def test_context_manager_success(self):
        """Test context manager with successful operation."""
        with AsyncMetricsContext(self.tracker, self.category):
            # Simulate some work
            time.sleep(0.01)

        # Verify timing was recorded
        self.assertGreater(self.tracker.timings[self.category], 0)
        self.mock_metrics_logger.put_metric.assert_called()

    def test_context_manager_with_exception(self):
        """Test context manager with exception."""
        try:
            with AsyncMetricsContext(self.tracker, self.category):
                time.sleep(0.01)
                raise ValueError("Test exception")
        except ValueError:
            pass

        # Verify timing was still recorded despite exception
        self.assertGreater(self.tracker.timings[self.category], 0)
        self.mock_metrics_logger.put_metric.assert_called()

    def test_context_manager_return_value(self):
        """Test context manager return value."""
        with AsyncMetricsContext(self.tracker, self.category) as context:
            self.assertIsInstance(context, AsyncMetricsContext)
            self.assertEqual(context.tracker, self.tracker)
            self.assertEqual(context.category, self.category)


class TestAsyncMetricsIntegration(unittest.TestCase):
    """Integration tests for AsyncMetrics components."""

    def test_complete_workflow_tracking(self):
        """Test tracking a complete async workflow."""
        mock_logger = Mock()
        tracker = AsyncMetricsTracker(mock_logger)

        # Simulate complete async workflow
        with AsyncMetricsContext(tracker, "TotalAsyncDuration"):
            # S3 Upload
            with AsyncMetricsContext(tracker, "S3Upload"):
                time.sleep(0.01)
            tracker.set_counter("S3UploadSize", 1024)

            # Async Endpoint Invocation
            with AsyncMetricsContext(tracker, "AsyncEndpointInvocation"):
                time.sleep(0.005)

            # Polling
            with AsyncMetricsContext(tracker, "QueueTime"):
                for _ in range(3):
                    tracker.increment_counter("PollingAttempts")
                    time.sleep(0.005)

            # S3 Download
            with AsyncMetricsContext(tracker, "S3Download"):
                time.sleep(0.01)
            tracker.set_counter("S3DownloadSize", 2048)

        # Mark as successful
        tracker.increment_counter("AsyncInferenceSuccess")

        # Emit summary and log performance
        tracker.emit_summary_metrics()
        tracker.log_performance_summary()

        # Verify all expected metrics were tracked
        self.assertGreater(tracker.get_timing("TotalAsyncDuration"), 0)
        self.assertGreater(tracker.get_timing("S3Upload"), 0)
        self.assertGreater(tracker.get_timing("S3Download"), 0)
        self.assertGreater(tracker.get_timing("QueueTime"), 0)
        self.assertEqual(tracker.get_counter("PollingAttempts"), 3)
        self.assertEqual(tracker.get_counter("S3UploadSize"), 1024)
        self.assertEqual(tracker.get_counter("S3DownloadSize"), 2048)
        self.assertEqual(tracker.get_counter("AsyncInferenceSuccess"), 1)

        # Verify metrics were emitted to logger
        self.assertGreater(mock_logger.put_metric.call_count, 10)


if __name__ == "__main__":
    unittest.main()
