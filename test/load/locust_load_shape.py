#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
Time-based load shape for Model Runner load tests.

This module provides a custom Locust LoadTestShape that runs for a specified
time window and then stops.
"""

from typing import Optional, Tuple

from locust import LoadTestShape, events


@events.init_command_line_parser.add_listener
def add_custom_arguments(parser) -> None:
    """
    Add custom command line arguments for time-based load shape.

    :param parser: ArgumentParser instance to add arguments to
    """
    parser.add_argument(
        "--processing-window-min",
        type=int,
        default=1,
        help="Processing window duration in minutes (default: 1)",
    )


class TimeWindowLoadShape(LoadTestShape):
    """
    Custom load shape that runs for a specified time window.

    Maintains the user count set via Locust's --users argument or web UI
    for the duration of the processing window, then stops the test.
    """

    def tick(self) -> Optional[Tuple[int, float]]:
        """
        Calculate the target user count for the current time.

        :return: Tuple of (user_count, spawn_rate) or None to stop the test
        """
        if self.runner is None:
            return None

        processing_window_min = self.runner.environment.parsed_options.processing_window_min
        processing_window_sec = processing_window_min * 60
        run_time = round(self.get_run_time())

        # Stop the test if we've exceeded the processing window
        if run_time >= processing_window_sec:
            return None

        # Maintain current user count (set via --users or web UI)
        # Return None to keep current count, or (count, rate) to change it
        return None
