# Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

"""
Optional Locust load shape(s) for Model Runner load tests.
"""

import math
from typing import Optional, Tuple

from _load_utils import safe_add_argument
from locust import LoadTestShape, events


@events.init_command_line_parser.add_listener
def add_custom_arguments(parser) -> None:
    """
    Register custom CLI arguments for `PeriodicBurstLoadShape`.

    :param parser: Locust/argparse parser instance.
    :returns: None
    """
    safe_add_argument(
        parser,
        "--pbls-repeat-period",
        type=int,
        default=600,
        help="PeriodicBurstLoadShape: Repeat cycle in seconds",
    )
    safe_add_argument(
        parser,
        "--pbls-min-concurrency",
        type=int,
        default=5,
        help="PeriodicBurstLoadShape: Minimum number of users",
    )
    safe_add_argument(
        parser,
        "--pbls-peak-concurrency",
        type=int,
        default=40,
        help="PeriodicBurstLoadShape: Peak number of users",
    )
    safe_add_argument(
        parser,
        "--pbls-peak-std",
        type=int,
        default=None,
        help="PeriodicBurstLoadShape: Peak standard deviation in seconds",
    )
    safe_add_argument(
        parser,
        "--pbls-peak-mean",
        type=int,
        default=None,
        help="PeriodicBurstLoadShape: Mean in seconds",
    )


class PeriodicBurstLoadShape(LoadTestShape):
    """
    Custom load shape that generates periodic bursts of load.
    """

    def tick(self) -> Optional[Tuple[int, float]]:
        """
        Compute the next desired user count and spawn rate.

        :returns: Tuple of `(user_count, spawn_rate)` or `None` to stop shaping.
        """
        if self.runner is None:
            return None

        run_time = round(self.get_run_time())
        return self.calculate_load_at_time(run_time)

    def calculate_load_at_time(self, run_time) -> Optional[Tuple[int, float]]:
        """
        Calculate the load curve value at a given runtime.

        This uses a Gaussian-shaped pulse per repeat period.

        :param run_time: Elapsed runtime in seconds.
        :returns: Tuple of `(user_count, spawn_rate)` or `None` to stop shaping.
        """
        repeat_period = self.runner.environment.parsed_options.pbls_repeat_period
        min_concurrency = self.runner.environment.parsed_options.pbls_min_concurrency
        peak_concurrency = self.runner.environment.parsed_options.pbls_peak_concurrency
        peak_mean = self.runner.environment.parsed_options.pbls_peak_mean
        peak_std = self.runner.environment.parsed_options.pbls_peak_std

        if peak_mean is None:
            peak_mean = repeat_period / 2
        if peak_std is None:
            peak_std = repeat_period / 10

        relative_time = run_time % repeat_period
        z = (relative_time - peak_mean) / peak_std
        user_count = (peak_concurrency - min_concurrency) * math.e ** -(math.pi * z**2) + min_concurrency
        return round(user_count), round(user_count)
