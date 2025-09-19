#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
Error classes for OSML extensions.
"""

from .extension_errors import ExtensionConfigurationError, ExtensionRuntimeError
from .async_errors import (
    AsyncInferenceError,
    AsyncInferenceTimeoutError,
    S3OperationError,
    AsyncEndpointError,
    WorkerPoolError,
    AsyncErrorHandler
)

__all__ = [
    "ExtensionConfigurationError",
    "ExtensionRuntimeError",
    "AsyncInferenceError",
    "AsyncInferenceTimeoutError",
    "S3OperationError",
    "AsyncEndpointError",
    "WorkerPoolError",
    "AsyncErrorHandler"
]