#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

from enum import auto
import logging

from aws.osml.model_runner.common import AutoStringEnum
from aws.osml.model_runner.api import VALID_MODEL_HOSTING_OPTIONS

logger = logging.getLogger(__name__)


class ExtendedModelInvokeMode(str, AutoStringEnum):
    """
    Extended model invocation modes for enhanced detectors.

    This enum extends the base ModelInvokeMode to support additional
    detector types while maintaining full compatibility with existing modes.
    """

    SM_ENDPOINT_ASYNC = auto()
    # Future extension modes can be added here
    # HTTP_ENDPOINT_ASYNC = "HTTP_ENDPOINT_ASYNC"  # Example future extension


VALID_MODEL_HOSTING_OPTIONS.extend([item.value for item in ExtendedModelInvokeMode])
logger.info(f"valid hosting options: {VALID_MODEL_HOSTING_OPTIONS}")
