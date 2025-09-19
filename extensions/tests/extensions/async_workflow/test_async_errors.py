#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import unittest
from unittest.mock import patch

from botocore.exceptions import ClientError

from ..src.osml_extensions.errors import (
    AsyncInferenceError,
    AsyncInferenceTimeoutError,
    S3OperationError,
    AsyncEndpointError,
    WorkerPoolError,
    AsyncErrorHandler
)


class TestAsyncErrors(unittest.TestCase):
    """Test cases for async error classes."""
    
    def test_async_inference_timeout_error(self):
        """Test AsyncInferenceTimeoutError initialization."""
        error = AsyncInferenceTimeoutError(
            message="Inference timed out",
            inference_id="test-inference-123",
            elapsed_time=300.5,
            max_wait_time=300.0
        )
        
        self.assertEqual(str(error), "Inference timed out")
        self.assertEqual(error.inference_id, "test-inference-123")
        self.assertEqual(error.elapsed_time, 300.5)
        self.assertEqual(error.max_wait_time, 300.0)
    
    def test_s3_operation_error(self):
        """Test S3OperationError initialization."""
        error = S3OperationError(
            message="S3 upload failed",
            operation="upload",
            s3_uri="s3://test-bucket/test-key",
            retry_count=3
        )
        
        self.assertEqual(str(error), "S3 upload failed")
        self.assertEqual(error.operation, "upload")
        self.assertEqual(error.s3_uri, "s3://test-bucket/test-key")
        self.assertEqual(error.retry_count, 3)
    
    def test_async_endpoint_error(self):
        """Test AsyncEndpointError initialization."""
        error = AsyncEndpointError(
            message="Endpoint invocation failed",
            endpoint_name="test-endpoint",
            error_code="ValidationException",
            http_status_code=400
        )
        
        self.assertEqual(str(error), "Endpoint invocation failed")
        self.assertEqual(error.endpoint_name, "test-endpoint")
        self.assertEqual(error.error_code, "ValidationException")
        self.assertEqual(error.http_status_code, 400)
    
    def test_worker_pool_error(self):
        """Test WorkerPoolError initialization."""
        error = WorkerPoolError(
            message="Worker failed",
            worker_type="submission",
            worker_id=1
        )
        
        self.assertEqual(str(error), "Worker failed")
        self.assertEqual(error.worker_type, "submission")
        self.assertEqual(error.worker_id, 1)


class TestAsyncErrorHandler(unittest.TestCase):
    """Test cases for AsyncErrorHandler."""
    
    def test_handle_s3_client_error(self):
        """Test handling S3 ClientError."""
        client_error = ClientError(
            error_response={
                'Error': {
                    'Code': 'NoSuchBucket',
                    'Message': 'The specified bucket does not exist'
                },
                'ResponseMetadata': {
                    'HTTPStatusCode': 404
                }
            },
            operation_name='GetObject'
        )
        
        context = {
            's3_uri': 's3://test-bucket/test-key',
            'retry_count': 2
        }
        
        result = AsyncErrorHandler.handle_client_error(client_error, 'S3Upload', context)
        
        self.assertIsInstance(result, S3OperationError)
        self.assertIn('not found', str(result))
        self.assertEqual(result.operation, 'S3Upload')
        self.assertEqual(result.s3_uri, 's3://test-bucket/test-key')
        self.assertEqual(result.retry_count, 2)
    
    def test_handle_sagemaker_client_error(self):
        """Test handling SageMaker ClientError."""
        client_error = ClientError(
            error_response={
                'Error': {
                    'Code': 'ValidationException',
                    'Message': 'Invalid endpoint name'
                },
                'ResponseMetadata': {
                    'HTTPStatusCode': 400
                }
            },
            operation_name='InvokeEndpointAsync'
        )
        
        context = {
            'endpoint_name': 'test-endpoint'
        }
        
        result = AsyncErrorHandler.handle_client_error(client_error, 'SageMakerInvoke', context)
        
        self.assertIsInstance(result, AsyncEndpointError)
        self.assertIn('resource error', str(result))
        self.assertEqual(result.endpoint_name, 'test-endpoint')
        self.assertEqual(result.error_code, 'ValidationException')
        self.assertEqual(result.http_status_code, 400)
    
    def test_handle_generic_client_error(self):
        """Test handling generic ClientError."""
        client_error = ClientError(
            error_response={
                'Error': {
                    'Code': 'InternalError',
                    'Message': 'Internal service error'
                }
            },
            operation_name='SomeOperation'
        )
        
        result = AsyncErrorHandler.handle_client_error(client_error, 'GenericOperation')
        
        self.assertIsInstance(result, AsyncInferenceError)
        self.assertIn('AWS operation failed', str(result))
    
    def test_is_retryable_error_client_error(self):
        """Test retryable error detection for ClientError."""
        # Retryable error
        retryable_error = ClientError(
            error_response={'Error': {'Code': 'ServiceUnavailable'}},
            operation_name='Test'
        )
        self.assertTrue(AsyncErrorHandler.is_retryable_error(retryable_error))
        
        # Non-retryable error
        non_retryable_error = ClientError(
            error_response={'Error': {'Code': 'ValidationException'}},
            operation_name='Test'
        )
        self.assertFalse(AsyncErrorHandler.is_retryable_error(non_retryable_error))
    
    def test_is_retryable_error_s3_operation_error(self):
        """Test retryable error detection for S3OperationError."""
        # Retryable S3 error
        retryable_error = S3OperationError("Service unavailable")
        self.assertTrue(AsyncErrorHandler.is_retryable_error(retryable_error))
        
        # Non-retryable S3 error
        non_retryable_error = S3OperationError("Access denied")
        self.assertFalse(AsyncErrorHandler.is_retryable_error(non_retryable_error))
    
    def test_is_retryable_error_async_endpoint_error(self):
        """Test retryable error detection for AsyncEndpointError."""
        # Retryable endpoint error
        retryable_error = AsyncEndpointError("Service unavailable", error_code="ServiceUnavailable")
        self.assertTrue(AsyncErrorHandler.is_retryable_error(retryable_error))
        
        # Non-retryable endpoint error
        non_retryable_error = AsyncEndpointError("Validation failed", error_code="ValidationException")
        self.assertFalse(AsyncErrorHandler.is_retryable_error(non_retryable_error))
    
    def test_is_retryable_error_timeout_error(self):
        """Test retryable error detection for AsyncInferenceTimeoutError."""
        timeout_error = AsyncInferenceTimeoutError("Inference timed out")
        self.assertFalse(AsyncErrorHandler.is_retryable_error(timeout_error))
    
    def test_calculate_retry_delay(self):
        """Test retry delay calculation."""
        # Test exponential backoff
        delay1 = AsyncErrorHandler.calculate_retry_delay(1, base_delay=1.0, multiplier=2.0)
        delay2 = AsyncErrorHandler.calculate_retry_delay(2, base_delay=1.0, multiplier=2.0)
        delay3 = AsyncErrorHandler.calculate_retry_delay(3, base_delay=1.0, multiplier=2.0)
        
        self.assertEqual(delay1, 1.0)  # 1.0 * 2^0
        self.assertEqual(delay2, 2.0)  # 1.0 * 2^1
        self.assertEqual(delay3, 4.0)  # 1.0 * 2^2
        
        # Test max delay capping
        delay_large = AsyncErrorHandler.calculate_retry_delay(10, base_delay=1.0, max_delay=5.0, multiplier=2.0)
        self.assertEqual(delay_large, 5.0)  # Capped at max_delay
    
    @patch('osml_extensions.errors.async_errors.logger')
    def test_log_error_with_context_timeout_error(self, mock_logger):
        """Test logging AsyncInferenceTimeoutError with context."""
        error = AsyncInferenceTimeoutError(
            "Inference timed out",
            inference_id="test-123",
            elapsed_time=300.5,
            max_wait_time=300.0
        )
        
        context = {"operation_id": "op-456"}
        
        AsyncErrorHandler.log_error_with_context(error, "AsyncInference", context)
        
        # Verify error was logged
        mock_logger.error.assert_called()
        log_message = mock_logger.error.call_args[0][0]
        self.assertIn("Async inference timeout", log_message)
        self.assertIn("elapsed=300.5s", log_message)
        self.assertIn("max=300.0s", log_message)
        self.assertIn("inference_id=test-123", log_message)
    
    @patch('osml_extensions.errors.async_errors.logger')
    def test_log_error_with_context_s3_error(self, mock_logger):
        """Test logging S3OperationError with context."""
        error = S3OperationError(
            "Upload failed",
            operation="upload",
            s3_uri="s3://test-bucket/test-key",
            retry_count=3
        )
        
        AsyncErrorHandler.log_error_with_context(error, "S3Upload")
        
        # Verify error was logged
        mock_logger.error.assert_called()
        log_message = mock_logger.error.call_args[0][0]
        self.assertIn("S3 operation error", log_message)
        self.assertIn("operation=upload", log_message)
        self.assertIn("s3_uri=s3://test-bucket/test-key", log_message)
        self.assertIn("retries=3", log_message)
    
    @patch('osml_extensions.errors.async_errors.logger')
    def test_log_error_with_context_generic_error(self, mock_logger):
        """Test logging generic error with context."""
        error = Exception("Generic error")
        context = {"key1": "value1", "key2": "value2"}
        
        AsyncErrorHandler.log_error_with_context(error, "GenericOperation", context)
        
        # Verify error was logged
        mock_logger.error.assert_called()
        log_message = mock_logger.error.call_args[0][0]
        self.assertIn("Error in GenericOperation", log_message)
        self.assertIn("key1=value1", log_message)
        self.assertIn("key2=value2", log_message)
    
    def test_create_error_summary_empty(self):
        """Test creating error summary with empty list."""
        summary = AsyncErrorHandler.create_error_summary([])
        
        self.assertEqual(summary["total_errors"], 0)
    
    def test_create_error_summary_multiple_errors(self):
        """Test creating error summary with multiple errors."""
        errors = [
            S3OperationError("S3 error 1"),
            S3OperationError("S3 error 2"),
            AsyncEndpointError("Endpoint error"),
            AsyncInferenceTimeoutError("Timeout error"),
            Exception("Generic error")
        ]
        
        summary = AsyncErrorHandler.create_error_summary(errors)
        
        self.assertEqual(summary["total_errors"], 5)
        self.assertEqual(summary["error_types"]["S3OperationError"], 2)
        self.assertEqual(summary["error_types"]["AsyncEndpointError"], 1)
        self.assertEqual(summary["error_types"]["AsyncInferenceTimeoutError"], 1)
        self.assertEqual(summary["error_types"]["Exception"], 1)
        
        # Check retryable vs permanent counts
        self.assertEqual(summary["retryable_errors"], 3)  # 2 S3 + 1 Exception
        self.assertEqual(summary["permanent_errors"], 2)  # 1 Endpoint + 1 Timeout
        
        # Check most common error
        self.assertEqual(summary["most_common_error"]["type"], "S3OperationError")
        self.assertEqual(summary["most_common_error"]["count"], 2)


if __name__ == "__main__":
    unittest.main()