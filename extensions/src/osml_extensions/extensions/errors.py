#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
Minimal error classes for OSML Model Runner Extensions.
"""


class SelfThrottledTileException(Exception):
    pass


class ExtensionError(Exception):
    """Base exception for extension-related errors."""

    pass


class ExtensionConfigurationError(ExtensionError):
    """Configuration-related extension errors."""

    pass


class ExtensionRuntimeError(ExtensionError):
    """Runtime extension errors that should trigger fallback."""

    pass
