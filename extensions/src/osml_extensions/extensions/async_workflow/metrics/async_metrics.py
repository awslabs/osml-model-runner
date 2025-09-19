#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
import time
from typing import Dict, Optional

from aws_embedded_metrics.logger.metrics_logger import MetricsLogger
from aws_embedded_metrics.unit import Unit

logger = logging.getLogger(__name__)


class AsyncMetricsTracker:
    """
    Comprehensive metrics tracker for async endpoint operations.

    This class provides detailed timing and performance metrics for all aspects
    of async inference processing including S3 operations, endpoint invocation,
    polling, and overall request duration.
    """

    def __init__(self, metrics_logger: Optional[MetricsLogger] = None):
        """
        Initialize AsyncMetricsTracker.

        :param metrics_logger: Optional MetricsLogger instance for emitting metrics
        """
        self.metrics_logger = metrics_logger
        self.timings: Dict[str, float] = {}
        self.counters: Dict[str, int] = {}
        self.start_times: Dict[str, float] = {}

        # Initialize timing categories
        self.timing_categories = [
            "S3Upload",
            "S3Download",
            "AsyncEndpointInvocation",
            "QueueTime",
            "TotalAsyncDuration",
            "PollingDuration",
        ]

        # Initialize counter categories
        self.counter_categories = [
            "PollingAttempts",
            "S3UploadRetries",
            "S3DownloadRetries",
            "AsyncInferenceSuccess",
            "AsyncInferenceFailures",
            "S3UploadSize",
            "S3DownloadSize",
        ]

        # Initialize all metrics
        for category in self.timing_categories:
            self.timings[category] = 0.0

        for category in self.counter_categories:
            self.counters[category] = 0

    def start_timer(self, category: str) -> None:
        """
        Start timing for a specific category.

        :param category: The timing category to start
        """
        if category not in self.timing_categories:
            logger.warning(f"Unknown timing category: {category}")
            return

        self.start_times[category] = time.time()
        logger.debug(f"Started timer for {category}")

    def stop_timer(self, category: str) -> float:
        """
        Stop timing for a specific category and record the duration.

        :param category: The timing category to stop
        :return: The elapsed time in seconds
        """
        if category not in self.timing_categories:
            logger.warning(f"Unknown timing category: {category}")
            return 0.0

        if category not in self.start_times:
            logger.warning(f"Timer for {category} was not started")
            return 0.0

        elapsed_time = time.time() - self.start_times[category]
        self.timings[category] = elapsed_time

        # Emit metric if logger is available
        if isinstance(self.metrics_logger, MetricsLogger):
            self.metrics_logger.put_metric(category, elapsed_time, str(Unit.SECONDS.value))

        logger.debug(f"Stopped timer for {category}: {elapsed_time:.3f}s")
        return elapsed_time

    def increment_counter(self, category: str, value: int = 1) -> None:
        """
        Increment a counter metric.

        :param category: The counter category to increment
        :param value: The value to increment by (default: 1)
        """
        if category not in self.counter_categories:
            logger.warning(f"Unknown counter category: {category}")
            return

        self.counters[category] += value

        # Emit metric if logger is available
        if isinstance(self.metrics_logger, MetricsLogger):
            unit = Unit.BYTES.value if "Size" in category else Unit.COUNT.value
            self.metrics_logger.put_metric(category, value, str(unit))

        logger.debug(f"Incremented {category} by {value} (total: {self.counters[category]})")

    def set_counter(self, category: str, value: int) -> None:
        """
        Set a counter metric to a specific value.

        :param category: The counter category to set
        :param value: The value to set
        """
        if category not in self.counter_categories:
            logger.warning(f"Unknown counter category: {category}")
            return

        self.counters[category] = value

        # Emit metric if logger is available
        if isinstance(self.metrics_logger, MetricsLogger):
            unit = Unit.BYTES.value if "Size" in category else Unit.COUNT.value
            self.metrics_logger.put_metric(category, value, str(unit))

        logger.debug(f"Set {category} to {value}")

    def get_timing(self, category: str) -> float:
        """
        Get the recorded timing for a category.

        :param category: The timing category to retrieve
        :return: The recorded time in seconds
        """
        return self.timings.get(category, 0.0)

    def get_counter(self, category: str) -> int:
        """
        Get the recorded counter value for a category.

        :param category: The counter category to retrieve
        :return: The recorded counter value
        """
        return self.counters.get(category, 0)

    def get_all_timings(self) -> Dict[str, float]:
        """
        Get all recorded timings.

        :return: Dictionary of all timing measurements
        """
        return self.timings.copy()

    def get_all_counters(self) -> Dict[str, int]:
        """
        Get all recorded counter values.

        :return: Dictionary of all counter values
        """
        return self.counters.copy()

    def emit_summary_metrics(self) -> None:
        """
        Emit summary metrics for the entire async operation.
        """
        if not isinstance(self.metrics_logger, MetricsLogger):
            return

        # Emit timing breakdown metrics
        total_time = self.timings.get("TotalAsyncDuration", 0.0)
        if total_time > 0:
            # Calculate percentages of total time
            for category in ["S3Upload", "S3Download", "QueueTime"]:
                category_time = self.timings.get(category, 0.0)
                percentage = (category_time / total_time) * 100
                self.metrics_logger.put_metric(f"{category}Percentage", percentage, str(Unit.PERCENT.value))

        # Emit efficiency metrics
        polling_attempts = self.counters.get("PollingAttempts", 0)
        if polling_attempts > 0:
            queue_time = self.timings.get("QueueTime", 0.0)
            avg_poll_interval = queue_time / polling_attempts if polling_attempts > 0 else 0
            self.metrics_logger.put_metric("AveragePollingInterval", avg_poll_interval, str(Unit.SECONDS.value))

        # Emit throughput metrics
        upload_size = self.counters.get("S3UploadSize", 0)
        download_size = self.counters.get("S3DownloadSize", 0)
        upload_time = self.timings.get("S3Upload", 0.0)
        download_time = self.timings.get("S3Download", 0.0)

        if upload_size > 0 and upload_time > 0:
            upload_throughput = upload_size / upload_time  # bytes per second
            self.metrics_logger.put_metric("S3UploadThroughput", upload_throughput, str(Unit.BYTES_SECOND.value))

        if download_size > 0 and download_time > 0:
            download_throughput = download_size / download_time  # bytes per second
            self.metrics_logger.put_metric("S3DownloadThroughput", download_throughput, str(Unit.BYTES_SECOND.value))

        logger.debug("Emitted summary metrics for async operation")

    def log_performance_summary(self) -> None:
        """
        Log a comprehensive performance summary.
        """
        total_time = self.timings.get("TotalAsyncDuration", 0.0)
        upload_time = self.timings.get("S3Upload", 0.0)
        download_time = self.timings.get("S3Download", 0.0)
        queue_time = self.timings.get("QueueTime", 0.0)
        polling_attempts = self.counters.get("PollingAttempts", 0)

        logger.info(
            f"Async Inference Performance Summary: "
            f"Total={total_time:.3f}s, "
            f"Upload={upload_time:.3f}s, "
            f"Queue={queue_time:.3f}s, "
            f"Download={download_time:.3f}s, "
            f"Polls={polling_attempts}"
        )

        # Log detailed breakdown if debug logging is enabled
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Detailed timing breakdown:")
            for category, timing in self.timings.items():
                if timing > 0:
                    percentage = (timing / total_time * 100) if total_time > 0 else 0
                    logger.debug(f"  {category}: {timing:.3f}s ({percentage:.1f}%)")

            logger.debug("Counter summary:")
            for category, count in self.counters.items():
                if count > 0:
                    logger.debug(f"  {category}: {count}")

    def reset(self) -> None:
        """
        Reset all metrics and timers.
        """
        self.timings.clear()
        self.counters.clear()
        self.start_times.clear()

        # Re-initialize all metrics
        for category in self.timing_categories:
            self.timings[category] = 0.0

        for category in self.counter_categories:
            self.counters[category] = 0

        logger.debug("Reset all metrics and timers")


class AsyncMetricsContext:
    """
    Context manager for automatic timing of async operations.
    """

    def __init__(self, metrics_tracker: AsyncMetricsTracker, category: str):
        """
        Initialize context manager for timing.

        :param metrics_tracker: AsyncMetricsTracker instance
        :param category: Timing category to track
        """
        self.metrics_tracker = metrics_tracker
        self.category = category

    def __enter__(self):
        """Start timing when entering context."""
        self.metrics_tracker.start_timer(self.category)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Stop timing when exiting context."""
        self.metrics_tracker.stop_timer(self.category)
        return False  # Don't suppress exceptions
