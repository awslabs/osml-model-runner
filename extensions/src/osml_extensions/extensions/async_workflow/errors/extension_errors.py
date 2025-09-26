#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
Base error classes for OSML extensions.
"""


class SelfThrottledTileException(Exception):
    pass


class RetryableJobException(Exception):
    """Exception for jobs that can be retried."""

    pass


class ExtensionError(Exception):
    """Base exception for extension-related errors."""

    pass


class ExtensionRuntimeError(ExtensionError):
    """Runtime extension errors that should trigger fallback."""

    pass


class ExtensionConfigurationError(ExtensionRuntimeError):
    """Raised when there are configuration errors in OSML extensions."""

    pass
