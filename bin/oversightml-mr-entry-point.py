#  Copyright 2023 Amazon.com, Inc. or its affiliates.

import argparse
import logging
import os
import signal
import sys
from types import FrameType
from typing import Optional

from codeguru_profiler_agent import Profiler
from pythonjsonlogger import jsonlogger

from aws.osml.model_runner import ModelRunner
from aws.osml.model_runner.common import ThreadingLocalContextFilter

logger = logging.getLogger(__name__)


def handler_stop_signals(signal_num: int, frame: Optional[FrameType], model_runner: ModelRunner) -> None:
    model_runner.stop()


def configure_logging(verbose: bool) -> None:
    """
    This function configures the Python logging module to use a JSON formatter with and thread local context
    variables.

    :param verbose: if true the logging level will be set to DEBUG, otherwise it will be set to INFO.
    """

    logging_level = os.getenv("LOG_LEVEL") or (logging.DEBUG if verbose else logging.INFO)
    logging_level = "DEBUG"  # TODO: Get from env vars
    logging_level = "INFO"  # TODO: Get from env vars

    root_logger = logging.getLogger()
    root_logger.setLevel(logging_level)

    ch = logging.StreamHandler()
    ch.setLevel(logging_level)
    ch.addFilter(ThreadingLocalContextFilter(["job_id", "image_id"]))

    formatter = jsonlogger.JsonFormatter(
        fmt="%(levelname)s %(message)s %(job_id)s %(image_id)s", datefmt="%Y-%m-%dT%H:%M:%S"
    )
    ch.setFormatter(formatter)
    root_logger.addHandler(ch)


def map_signals(model_runner: ModelRunner) -> None:
    signal.signal(signal.SIGINT, lambda signum, frame: handler_stop_signals(signum, frame, model_runner))
    signal.signal(signal.SIGTERM, lambda signum, frame: handler_stop_signals(signum, frame, model_runner))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("--disable-extensions", action="store_true", 
                       help="Disable extensions even if available and configured")
    return parser.parse_args()


def setup_code_profiling() -> None:
    codeguru_profiling_group = os.environ.get("CODEGURU_PROFILING_GROUP")
    if codeguru_profiling_group:
        Profiler(profiling_group_name=codeguru_profiling_group).start()


def main() -> int:
    try:
        # Parse command line arguments
        args = parse_args()
        
        # Configure logging first
        configure_logging(args.verbose)
        
        # Set up extensions if available
        setup_extensions(args)
        
        # Create and configure model runner
        model_runner = ModelRunner()

        map_signals(model_runner)
        # setup_code_profiling()

        model_runner.run()
        
        return 0
        
    except KeyboardInterrupt:
        logger.info("Model runner interrupted by user")
        return 130  # Standard exit code for SIGINT
    except Exception as e:
        logger.error(f"Model runner failed with error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
