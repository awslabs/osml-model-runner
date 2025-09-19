#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
Base error classes for OSML extensions.
"""


class ExtensionRuntimeError(Exception):
    """Base class for runtime errors in OSML extensions."""

    pass


class ExtensionConfigurationError(ExtensionRuntimeError):
    """Raised when there are configuration errors in OSML extensions."""

    pass
