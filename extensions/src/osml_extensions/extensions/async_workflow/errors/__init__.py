# Telling flake8 to not flag errors in this file. It is normal that these classes are imported but not used in an
# __init__.py file.
# flake8: noqa
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
    ProcessTileException,
)
from .extension_errors import (
    SelfThrottledTileException,
    RetryableJobException,
    ExtensionError,
    ExtensionRuntimeError,
    ExtensionConfigurationError,
)
