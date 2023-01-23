import argparse
import logging
import os
import signal
from types import FrameType
from typing import Optional

# from codeguru_profiler_agent import Profiler

from aws_oversightml_model_runner.app import ModelRunner

# CODEGURU_PROFILING_GROUP = os.environ.get("CODEGURU_PROFILING_GROUP")

# Create an instance of model runner
model_runner = ModelRunner()


# Build the default stop signal handler
def handler_stop_signals(signal: int, frame: Optional[FrameType]) -> None:
    model_runner.stop()


# Map the signals to the handler
signal.signal(signal.SIGINT, handler_stop_signals)
signal.signal(signal.SIGTERM, handler_stop_signals)


def configure_logging(verbose: bool):
    """
    Setup logging for this application
    """
    logging_level = logging.INFO
    if verbose:
        logging_level = logging.DEBUG

    root_logger = logging.getLogger()
    root_logger.setLevel(logging_level)

    ch = logging.StreamHandler()
    ch.setLevel(logging_level)
    formatter = logging.Formatter("%(levelname)-8s %(message)s")
    ch.setFormatter(formatter)

    root_logger.addHandler(ch)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    configure_logging(args.verbose)

    # if CODEGURU_PROFILING_GROUP:
        # Profiler(profiling_group_name=CODEGURU_PROFILING_GROUP).start()

    # Start monitoring the Queues
    model_runner.run()
