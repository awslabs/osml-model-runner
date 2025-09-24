#  Copyright 2023 Amazon.com, Inc. or its affiliates.

import argparse
import logging
import os
import signal
import sys
from types import FrameType
from typing import Optional

# from codeguru_profiler_agent import Profiler
from pythonjsonlogger import jsonlogger

from aws.osml.model_runner import ModelRunner
from aws.osml.model_runner.common import ThreadingLocalContextFilter

logger = logging.getLogger(__name__)

# Check if extensions are available
EXTENSIONS_AVAILABLE = False
try:
    # import osml_extensions
    from osml_extensions.enhanced_model_runner import EnhancedModelRunner
    # from osml_extensions.extensions.async_workflow.async_app_config import AsyncServiceConfig

    EXTENSIONS_AVAILABLE = True
    logger.info("Extensions package found and imported successfully")
except ImportError as e:
    EXTENSIONS_AVAILABLE = False
    logger.info(f"Extensions package not available: {e}")


def handler_stop_signals(signal_num: int, frame: Optional[FrameType], model_runner: ModelRunner) -> None:
    model_runner.stop()


def configure_logging(verbose: bool) -> None:
    """
    This function configures the Python logging module to use a JSON formatter with and thread local context
    variables.

    :param verbose: if true the logging level will be set to DEBUG, otherwise it will be set to INFO.
    """


    logging_level = os.getenv("LOG_LEVEL") or (logging.DEBUG if verbose else logging.INFO)
    logging_level = "DEBUG" # TODO: Get from env vars
    logging_level = "INFO" # TODO: Get from env vars

    root_logger = logging.getLogger()
    root_logger.setLevel(logging_level)

    ch = logging.StreamHandler()
    ch.setLevel(logging_level)
    ch.addFilter(ThreadingLocalContextFilter(["job_id", "image_id", "runner_type"]))

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
    parser.add_argument(
        "--disable-extensions", action="store_true", help="Disable extensions even if available and configured"
    )
    return parser.parse_args()


def use_extensions() -> bool:
    """
    Check if extensions should be used based on environment variable.

    :return: bool = True if extensions should be used, False otherwise
    """
    env_value = os.getenv("USE_EXTENSIONS", "true").lower()
    return env_value in ("true", "1")


def should_use_extensions(args: argparse.Namespace) -> bool:
    """
    Determine if extensions should be used based on command line arguments and environment.

    :param args: Parsed command line arguments
    :return: True if extensions should be used, False otherwise
    """
    if not EXTENSIONS_AVAILABLE:
        logger.info("Extensions not available (osml_extensions package not installed)")
        return False

    if args.disable_extensions:
        logger.info("Extensions disabled via --disable-extensions command line argument")
        return False

    if not use_extensions():
        logger.info("Extensions disabled via USE_EXTENSIONS environment variable")
        return False

    logger.info("Extensions are available and enabled")
    return True


# def setup_code_profiling() -> None:
#     codeguru_profiling_group = os.environ.get("CODEGURU_PROFILING_GROUP")
#     if codeguru_profiling_group:
#         Profiler(profiling_group_name=codeguru_profiling_group).start()


def create_model_runner(use_enhanced: bool) -> ModelRunner:
    """
    Create the appropriate model runner based on configuration.

    :param use_enhanced: Whether to use the enhanced model runner
    :return: ModelRunner instance (base or enhanced)
    """
    if use_enhanced and EXTENSIONS_AVAILABLE:
        try:
            logger.info("Creating EnhancedModelRunner with extension support")
            return EnhancedModelRunner()
        except Exception as e:
            logger.error(f"Failed to create EnhancedModelRunner: {e}")
            logger.info("Falling back to base ModelRunner")

    logger.info("Creating base ModelRunner")
    return ModelRunner()


def main() -> int:
    try:
        # Parse command line arguments
        args = parse_args()

        # Configure logging first
        configure_logging(args.verbose)

        # Determine if extensions should be used
        use_enhanced = should_use_extensions(args)

        # Create and configure model runner
        model_runner = create_model_runner(use_enhanced)

        logger.info(f"Running model runner version: {model_runner}")

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
