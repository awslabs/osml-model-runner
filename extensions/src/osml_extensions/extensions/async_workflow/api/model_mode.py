#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

from enum import Enum


class ExtendedModelInvokeMode(Enum):
    """
    Extended model invocation modes for enhanced detectors.

    This enum extends the base ModelInvokeMode to support additional
    detector types while maintaining full compatibility with existing modes.
    """

    SM_ENDPOINT_ASYNC = "SM_ENDPOINT_ASYNC"
    # Future extension modes can be added here
    # HTTP_ENDPOINT_ASYNC = "HTTP_ENDPOINT_ASYNC"  # Example future extension
