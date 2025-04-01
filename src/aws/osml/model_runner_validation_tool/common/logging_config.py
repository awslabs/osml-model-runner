#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
Centralized logging configuration for the OSML Model Validation Tool.
"""

import logging
import os
import sys


def configure_logging(name=None):
    """
    Configure logging based on environment variables.
    Sets the root logger level based on LOG_LEVEL environment variable,
    while keeping third-party libraries at a higher log level to reduce noise.

    Args:
        name (str, optional): Logger name. If None, returns the root logger.

    Returns:
        logging.Logger: Configured logger
    """
    # Configure the root logger first if it hasn't been configured
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.WARNING, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", stream=sys.stdout
        )

    # Now configure the named logger, or adjust the root logger level if no name provided
    logger = logging.getLogger(name)

    # Configure log level from environment variable
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    log_level_map = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }
    logger.setLevel(log_level_map.get(log_level, logging.INFO))
    numeric_level = log_level_map.get(log_level, logging.INFO)
    logger.setLevel(numeric_level)

    # Finally, add a handler to the named logger, if the logger doesn't have one and isn't the root logger
    if not logger.handlers and name is not None and not logger.propagate:
        logger.debug(f"Adding handler to logger {name} with level {log_level}")
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(numeric_level)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    # Only log the configuration message if this is the root logger
    if name is None:
        logger.info(f"Name is None: Root log level set to {log_level}")

    # Set higher log levels for noisy third-party libraries
    # regardless of the root logger's level
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("s3transfer").setLevel(logging.WARNING)
    logging.getLogger("matplotlib").setLevel(logging.WARNING)
    logging.getLogger("PIL").setLevel(logging.WARNING)

    return logger
