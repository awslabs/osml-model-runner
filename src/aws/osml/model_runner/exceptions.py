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


class ProcessTileException(Exception):
    pass


class AsyncInferenceError(Exception):
    """Base class for async inference-related errors."""

    pass


class S3OperationError(Exception):
    """Raised when S3 upload/download operations fail."""

    pass


class SkipException(Exception):
    pass
