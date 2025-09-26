#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
Async-specific error classes and error handling utilities for OSML extensions.
"""

import logging
from typing import Any, Dict, Optional

from botocore.exceptions import ClientError

from .extension_errors import ExtensionRuntimeError

logger = logging.getLogger(__name__)


class ProcessTileException(Exception):
    pass


class AsyncInferenceError(ExtensionRuntimeError):
    """Base class for async inference-related errors."""

    pass


class AsyncInferenceTimeoutError(AsyncInferenceError):
    """Raised when async inference exceeds maximum wait time."""

    def __init__(
        self,
        message: str,
        inference_id: Optional[str] = None,
        elapsed_time: Optional[float] = None,
        max_wait_time: Optional[float] = None,
    ):
        """
        Initialize AsyncInferenceTimeoutError.

        :param message: Error message
        :param inference_id: The inference job ID that timed out
        :param elapsed_time: Time elapsed before timeout
        :param max_wait_time: Maximum allowed wait time
        """
        super().__init__(message)
        self.inference_id = inference_id
        self.elapsed_time = elapsed_time
        self.max_wait_time = max_wait_time


class S3OperationError(AsyncInferenceError):
    """Raised when S3 upload/download operations fail."""

    def __init__(
        self, message: str, operation: Optional[str] = None, s3_uri: Optional[str] = None, retry_count: Optional[int] = None
    ):
        """
        Initialize S3OperationError.

        :param message: Error message
        :param operation: The S3 operation that failed (upload/download/delete)
        :param s3_uri: The S3 URI involved in the operation
        :param retry_count: Number of retries attempted
        """
        super().__init__(message)
        self.operation = operation
        self.s3_uri = s3_uri
        self.retry_count = retry_count


class AsyncEndpointError(AsyncInferenceError):
    """Raised when SageMaker async endpoint operations fail."""

    def __init__(
        self,
        message: str,
        endpoint_name: Optional[str] = None,
        error_code: Optional[str] = None,
        http_status_code: Optional[int] = None,
    ):
        """
        Initialize AsyncEndpointError.

        :param message: Error message
        :param endpoint_name: The SageMaker endpoint name
        :param error_code: AWS error code
        :param http_status_code: HTTP status code
        """
        super().__init__(message)
        self.endpoint_name = endpoint_name
        self.error_code = error_code
        self.http_status_code = http_status_code


class WorkerPoolError(AsyncInferenceError):
    """Raised when async worker pool operations fail."""

    def __init__(self, message: str, worker_type: Optional[str] = None, worker_id: Optional[int] = None):
        """
        Initialize WorkerPoolError.

        :param message: Error message
        :param worker_type: Type of worker (submission/polling)
        :param worker_id: Worker ID
        """
        super().__init__(message)
        self.worker_type = worker_type
        self.worker_id = worker_id


class AsyncErrorHandler:
    """
    Centralized error handling utilities for async operations.

    This class provides standardized error handling, logging, and recovery
    mechanisms for async inference operations.
    """

    @staticmethod
    def handle_client_error(
        error: ClientError, operation: str, context: Optional[Dict[str, Any]] = None
    ) -> AsyncInferenceError:
        """
        Handle AWS ClientError and convert to appropriate async error.

        :param error: The ClientError to handle
        :param operation: The operation that failed
        :param context: Additional context information
        :return: Appropriate AsyncInferenceError subclass
        """
        error_code = error.response.get("Error", {}).get("Code", "Unknown")
        error_message = error.response.get("Error", {}).get("Message", str(error))
        http_status_code = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        context = context or {}

        logger.error(f"AWS ClientError in {operation}: {error_code} - {error_message}")

        # Handle S3-specific errors
        if operation.startswith("S3"):
            s3_uri = context.get("s3_uri")
            retry_count = context.get("retry_count", 0)

            if error_code in ["NoSuchBucket", "NoSuchKey"]:
                message = f"S3 resource not found during {operation}: {error_message}"
            elif error_code in ["AccessDenied", "Forbidden"]:
                message = f"S3 access denied during {operation}: {error_message}"
            elif error_code in ["ServiceUnavailable", "SlowDown"]:
                message = f"S3 service unavailable during {operation}: {error_message}"
            else:
                message = f"S3 operation failed during {operation}: {error_message}"

            return S3OperationError(message=message, operation=operation, s3_uri=s3_uri, retry_count=retry_count)

        # Handle SageMaker-specific errors
        elif operation.startswith("SageMaker") or operation.startswith("Async"):
            endpoint_name = context.get("endpoint_name")

            if error_code in ["ValidationException", "ResourceNotFound"]:
                message = f"SageMaker resource error in {operation}: {error_message}"
            elif error_code in ["ServiceUnavailable", "ThrottlingException"]:
                message = f"SageMaker service unavailable in {operation}: {error_message}"
            elif error_code in ["AccessDenied", "UnauthorizedOperation"]:
                message = f"SageMaker access denied in {operation}: {error_message}"
            else:
                message = f"SageMaker operation failed in {operation}: {error_message}"

            return AsyncEndpointError(
                message=message, endpoint_name=endpoint_name, error_code=error_code, http_status_code=http_status_code
            )

        # Generic async inference error
        else:
            message = f"AWS operation failed in {operation}: {error_message}"
            return AsyncInferenceError(message)

    @staticmethod
    def is_retryable_error(error: Exception) -> bool:
        """
        Determine if an error is retryable.

        :param error: The error to check
        :return: True if the error is retryable
        """
        # ClientError retryability
        if isinstance(error, ClientError):
            error_code = error.response.get("Error", {}).get("Code", "")

            # Retryable AWS error codes
            retryable_codes = [
                "ServiceUnavailable",
                "ThrottlingException",
                "SlowDown",
                "RequestTimeout",
                "InternalError",
                "InternalFailure",
            ]

            return error_code in retryable_codes

        # S3OperationError retryability
        elif isinstance(error, S3OperationError):
            # Don't retry permission or not found errors
            if any(keyword in str(error).lower() for keyword in ["access denied", "not found", "forbidden"]):
                return False
            return True

        # AsyncEndpointError retryability
        elif isinstance(error, AsyncEndpointError):
            # Don't retry validation or permission errors
            if error.error_code in ["ValidationException", "AccessDenied", "UnauthorizedOperation"]:
                return False
            return True

        # AsyncInferenceTimeoutError is not retryable
        elif isinstance(error, AsyncInferenceTimeoutError):
            return False

        # Other exceptions might be retryable
        else:
            return True

    @staticmethod
    def calculate_retry_delay(
        attempt: int, base_delay: float = 1.0, max_delay: float = 60.0, multiplier: float = 2.0
    ) -> float:
        """
        Calculate retry delay with exponential backoff.

        :param attempt: Current attempt number (1-based)
        :param base_delay: Base delay in seconds
        :param max_delay: Maximum delay in seconds
        :param multiplier: Exponential backoff multiplier
        :return: Delay in seconds
        """
        delay = base_delay * (multiplier ** (attempt - 1))
        return min(delay, max_delay)

    @staticmethod
    def log_error_with_context(error: Exception, operation: str, context: Optional[Dict[str, Any]] = None) -> None:
        """
        Log error with comprehensive context information.

        :param error: The error to log
        :param operation: The operation that failed
        :param context: Additional context information
        """
        context = context or {}

        # Build context string
        context_parts = []
        for key, value in context.items():
            if value is not None:
                context_parts.append(f"{key}={value}")

        context_str = f" ({', '.join(context_parts)})" if context_parts else ""

        # Log based on error type
        if isinstance(error, AsyncInferenceTimeoutError):
            logger.error(
                f"Async inference timeout in {operation}{context_str}: "
                f"elapsed={error.elapsed_time}s, max={error.max_wait_time}s, "
                f"inference_id={error.inference_id}"
            )

        elif isinstance(error, S3OperationError):
            logger.error(
                f"S3 operation error in {operation}{context_str}: "
                f"operation={error.operation}, s3_uri={error.s3_uri}, "
                f"retries={error.retry_count}, error={str(error)}"
            )

        elif isinstance(error, AsyncEndpointError):
            logger.error(
                f"Async endpoint error in {operation}{context_str}: "
                f"endpoint={error.endpoint_name}, code={error.error_code}, "
                f"status={error.http_status_code}, error={str(error)}"
            )

        elif isinstance(error, WorkerPoolError):
            logger.error(
                f"Worker pool error in {operation}{context_str}: "
                f"worker_type={error.worker_type}, worker_id={error.worker_id}, "
                f"error={str(error)}"
            )

        else:
            logger.error(f"Error in {operation}{context_str}: {str(error)}")

        # Log stack trace for debugging
        logger.debug(f"Stack trace for {operation} error:", exc_info=error)

    @staticmethod
    def create_error_summary(errors: list) -> Dict[str, Any]:
        """
        Create a summary of multiple errors for reporting.

        :param errors: List of errors to summarize
        :return: Error summary dictionary
        """
        if not errors:
            return {"total_errors": 0}

        summary = {
            "total_errors": len(errors),
            "error_types": {},
            "retryable_errors": 0,
            "permanent_errors": 0,
            "most_common_error": None,
        }

        error_type_counts = {}

        for error in errors:
            error_type = type(error).__name__
            error_type_counts[error_type] = error_type_counts.get(error_type, 0) + 1

            if AsyncErrorHandler.is_retryable_error(error):
                summary["retryable_errors"] += 1
            else:
                summary["permanent_errors"] += 1

        summary["error_types"] = error_type_counts

        # Find most common error type
        if error_type_counts:
            most_common = max(error_type_counts.items(), key=lambda x: x[1])
            summary["most_common_error"] = {"type": most_common[0], "count": most_common[1]}

        return summary
