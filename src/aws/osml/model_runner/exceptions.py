#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.


# ModelRunner Exceptions
class RetryableJobException(Exception):
    pass


class AggregateFeaturesException(Exception):
    pass


class ProcessRegionException(Exception):
    pass


class LoadImageException(Exception):
    pass


class ProcessImageException(Exception):
    pass


class UnsupportedModelException(Exception):
    pass


class InvalidImageURLException(Exception):
    pass


class SelfThrottledRegionException(Exception):
    pass


class AggregateOutputFeaturesException(Exception):
    pass


class SelfThrottledTileException(Exception):
    pass


class InvocationFailure(RetryableJobException):
    """Model failure"""

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


class ProcessTileException(Exception):
    pass


class AsyncInferenceError(ExtensionRuntimeError):
    """Base class for async inference-related errors."""

    pass
