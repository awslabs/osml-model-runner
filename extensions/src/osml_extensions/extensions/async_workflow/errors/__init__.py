#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
Error classes for OSML extensions.
"""

from .async_errors import (
    AsyncEndpointError,
    AsyncErrorHandler,
    AsyncInferenceError,
    AsyncInferenceTimeoutError,
    S3OperationError,
    WorkerPoolError,
)
from .extension_errors import ExtensionConfigurationError, ExtensionRuntimeError

__all__ = [
    "ExtensionConfigurationError",
    "ExtensionRuntimeError",
    "AsyncInferenceError",
    "AsyncInferenceTimeoutError",
    "S3OperationError",
    "AsyncEndpointError",
    "WorkerPoolError",
    "AsyncErrorHandler",
]
