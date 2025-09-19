#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
Polling module for OSML extensions.
"""

from .async_inference_poller import AsyncInferencePoller, AsyncInferenceTimeoutError

__all__ = [
    "AsyncInferencePoller",
    "AsyncInferenceTimeoutError"
]