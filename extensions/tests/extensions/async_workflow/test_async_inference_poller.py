#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import time
import unittest
from unittest.mock import Mock, patch

from botocore.exceptions import ClientError

from ..src.osml_extensions.config import AsyncEndpointConfig
from ..src.osml_extensions.errors import ExtensionRuntimeError
from ..src.osml_extensions.polling import AsyncInferencePoller, AsyncInferenceTimeoutError


class TestAsyncInferencePoller(unittest.TestCase):
    """Test cases for AsyncInferencePoller."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.config = AsyncEndpointConfig(
            input_bucket="test-input-bucket",
            output_bucket="test-output-bucket",
            max_wait_time=300,  # 5 minutes for testing
            polling_interval=10,
            max_polling_interval=60,
            exponential_backoff_multiplier=2.0
        )
        
        self.mock_sm_client = Mock()
        self.poller = AsyncInferencePoller(self.mock_sm_client, self.config)
        self.test_inference_id = "test-inference-123"
        self.test_output_location = "s3://test-output-bucket/results/output.json"
    
    def test_successful_polling_immediate_completion(self):
        """Test polling when job completes immediately."""
        # Mock job already completed
        self.mock_sm_client.describe_inference_recommendations_job.return_value = {
            'Status': 'Completed',
            'OutputLocation': self.test_output_location
        }
        
        result = self.poller.poll_until_complete(self.test_inference_id)
        
        self.assertEqual(result, self.test_output_location)
        self.mock_sm_client.describe_inference_recommendations_job.assert_called_once_with(
            JobName=self.test_inference_id
        )
    
    def test_successful_polling_with_wait(self):
        """Test polling when job completes after some attempts."""
        # Mock job in progress, then completed
        self.mock_sm_client.describe_inference_recommendations_job.side_effect = [
            {'Status': 'InProgress'},
            {'Status': 'InProgress'},
            {'Status': 'Completed', 'OutputLocation': self.test_output_location}
        ]
        
        with patch('time.sleep'):  # Mock sleep to speed up test
            result = self.poller.poll_until_complete(self.test_inference_id)
        
        self.assertEqual(result, self.test_output_location)
        self.assertEqual(self.mock_sm_client.describe_inference_recommendations_job.call_count, 3)
    
    def test_polling_with_metrics(self):
        """Test polling with metrics logging."""
        mock_metrics = Mock()
        self.mock_sm_client.describe_inference_recommendations_job.return_value = {
            'Status': 'Completed',
            'OutputLocation': self.test_output_location
        }
        
        result = self.poller.poll_until_complete(self.test_inference_id, mock_metrics)
        
        self.assertEqual(result, self.test_output_location)
        mock_metrics.put_dimensions.assert_called()
        mock_metrics.put_metric.assert_called()
    
    def test_polling_job_failed(self):
        """Test polling when job fails."""
        self.mock_sm_client.describe_inference_recommendations_job.return_value = {
            'Status': 'Failed'
        }
        
        with self.assertRaises(ExtensionRuntimeError) as context:
            self.poller.poll_until_complete(self.test_inference_id)
        
        self.assertIn("failed", str(context.exception))
    
    def test_polling_timeout(self):
        """Test polling timeout behavior."""
        # Mock job always in progress
        self.mock_sm_client.describe_inference_recommendations_job.return_value = {
            'Status': 'InProgress'
        }
        
        # Use very short timeout for testing
        self.config.max_wait_time = 1
        
        with patch('time.sleep'):  # Mock sleep to speed up test
            with self.assertRaises(AsyncInferenceTimeoutError) as context:
                self.poller.poll_until_complete(self.test_inference_id)
        
        self.assertIn("timed out", str(context.exception))
    
    def test_polling_with_client_error_retry(self):
        """Test polling with temporary client errors that should be retried."""
        # Mock temporary error, then success
        self.mock_sm_client.describe_inference_recommendations_job.side_effect = [
            ClientError(
                error_response={'Error': {'Code': 'ServiceUnavailable', 'Message': 'Service unavailable'}},
                operation_name='DescribeInferenceRecommendationsJob'
            ),
            {'Status': 'Completed', 'OutputLocation': self.test_output_location}
        ]
        
        with patch('time.sleep'):  # Mock sleep to speed up test
            result = self.poller.poll_until_complete(self.test_inference_id)
        
        self.assertEqual(result, self.test_output_location)
        self.assertEqual(self.mock_sm_client.describe_inference_recommendations_job.call_count, 2)
    
    def test_polling_with_permanent_client_error(self):
        """Test polling with permanent client errors that should not be retried."""
        self.mock_sm_client.describe_inference_recommendations_job.side_effect = ClientError(
            error_response={'Error': {'Code': 'ValidationException', 'Message': 'Invalid job name'}},
            operation_name='DescribeInferenceRecommendationsJob'
        )
        
        with self.assertRaises(ExtensionRuntimeError) as context:
            self.poller.poll_until_complete(self.test_inference_id)
        
        self.assertIn("Permanent error", str(context.exception))
        self.mock_sm_client.describe_inference_recommendations_job.assert_called_once()
    
    def test_polling_completed_without_output_location(self):
        """Test polling when job completes but no output location is provided."""
        self.mock_sm_client.describe_inference_recommendations_job.return_value = {
            'Status': 'Completed'
            # No OutputLocation
        }
        
        with self.assertRaises(ExtensionRuntimeError) as context:
            self.poller.poll_until_complete(self.test_inference_id)
        
        self.assertIn("no output location", str(context.exception))
    
    def test_polling_unknown_status(self):
        """Test polling with unknown job status."""
        # Mock unknown status, then completed
        self.mock_sm_client.describe_inference_recommendations_job.side_effect = [
            {'Status': 'UnknownStatus'},
            {'Status': 'Completed', 'OutputLocation': self.test_output_location}
        ]
        
        with patch('time.sleep'):  # Mock sleep to speed up test
            result = self.poller.poll_until_complete(self.test_inference_id)
        
        self.assertEqual(result, self.test_output_location)
        self.assertEqual(self.mock_sm_client.describe_inference_recommendations_job.call_count, 2)
    
    def test_get_job_status_success(self):
        """Test successful job status retrieval."""
        expected_response = {
            'Status': 'InProgress',
            'OutputLocation': None
        }
        self.mock_sm_client.describe_inference_recommendations_job.return_value = expected_response
        
        status, output_location = self.poller.get_job_status(self.test_inference_id)
        
        self.assertEqual(status, 'InProgress')
        self.assertIsNone(output_location)
    
    def test_get_job_status_with_output(self):
        """Test job status retrieval with output location."""
        expected_response = {
            'Status': 'Completed',
            'OutputLocation': self.test_output_location
        }
        self.mock_sm_client.describe_inference_recommendations_job.return_value = expected_response
        
        status, output_location = self.poller.get_job_status(self.test_inference_id)
        
        self.assertEqual(status, 'Completed')
        self.assertEqual(output_location, self.test_output_location)
    
    def test_get_job_status_client_error(self):
        """Test job status retrieval with client error."""
        self.mock_sm_client.describe_inference_recommendations_job.side_effect = ClientError(
            error_response={'Error': {'Code': 'AccessDenied', 'Message': 'Access denied'}},
            operation_name='DescribeInferenceRecommendationsJob'
        )
        
        with self.assertRaises(ClientError):
            self.poller.get_job_status(self.test_inference_id)
    
    def test_calculate_backoff_delay(self):
        """Test exponential backoff delay calculation."""
        # Test first few attempts
        delay1 = self.poller._calculate_backoff_delay(1)
        delay2 = self.poller._calculate_backoff_delay(2)
        delay3 = self.poller._calculate_backoff_delay(3)
        
        # Should follow exponential pattern: 10, 20, 40
        self.assertEqual(delay1, 10.0)  # base interval
        self.assertEqual(delay2, 20.0)  # base * 2^1
        self.assertEqual(delay3, 40.0)  # base * 2^2
        
        # Test capping at max interval
        delay_large = self.poller._calculate_backoff_delay(10)
        self.assertEqual(delay_large, self.config.max_polling_interval)
    
    def test_calculate_backoff_delay_with_different_multiplier(self):
        """Test backoff delay calculation with different multiplier."""
        self.config.exponential_backoff_multiplier = 1.5
        
        delay1 = self.poller._calculate_backoff_delay(1)
        delay2 = self.poller._calculate_backoff_delay(2)
        delay3 = self.poller._calculate_backoff_delay(3)
        
        self.assertEqual(delay1, 10.0)  # base interval
        self.assertEqual(delay2, 15.0)  # base * 1.5^1
        self.assertEqual(delay3, 22.5)  # base * 1.5^2
    
    def test_check_job_exists_true(self):
        """Test checking if job exists when it does."""
        self.mock_sm_client.describe_inference_recommendations_job.return_value = {
            'Status': 'InProgress'
        }
        
        exists = self.poller.check_job_exists(self.test_inference_id)
        self.assertTrue(exists)
    
    def test_check_job_exists_false(self):
        """Test checking if job exists when it doesn't."""
        self.mock_sm_client.describe_inference_recommendations_job.side_effect = ClientError(
            error_response={'Error': {'Code': 'ResourceNotFound', 'Message': 'Job not found'}},
            operation_name='DescribeInferenceRecommendationsJob'
        )
        
        exists = self.poller.check_job_exists(self.test_inference_id)
        self.assertFalse(exists)
    
    def test_check_job_exists_with_other_error(self):
        """Test checking if job exists with other errors."""
        self.mock_sm_client.describe_inference_recommendations_job.side_effect = ClientError(
            error_response={'Error': {'Code': 'ServiceUnavailable', 'Message': 'Service unavailable'}},
            operation_name='DescribeInferenceRecommendationsJob'
        )
        
        # Should assume job exists for temporary errors
        exists = self.poller.check_job_exists(self.test_inference_id)
        self.assertTrue(exists)
    
    def test_cancel_job_success(self):
        """Test successful job cancellation."""
        # Mock successful cancellation (placeholder implementation)
        result = self.poller.cancel_job(self.test_inference_id)
        
        # Should return True for successful cancellation
        self.assertTrue(result)
    
    def test_cancel_job_with_error(self):
        """Test job cancellation with error."""
        # Since cancel_job is a placeholder, it will always return True
        # In a real implementation, this would test actual cancellation errors
        result = self.poller.cancel_job(self.test_inference_id)
        self.assertTrue(result)


class TestAsyncInferencePollerIntegration(unittest.TestCase):
    """Integration tests for AsyncInferencePoller."""
    
    def setUp(self):
        """Set up integration test fixtures."""
        self.config = AsyncEndpointConfig(
            input_bucket="test-input-bucket",
            output_bucket="test-output-bucket",
            max_wait_time=10,  # Short timeout for integration tests
            polling_interval=1,
            max_polling_interval=5,
            exponential_backoff_multiplier=1.5
        )
        
        self.mock_sm_client = Mock()
        self.poller = AsyncInferencePoller(self.mock_sm_client, self.config)
    
    def test_realistic_polling_scenario(self):
        """Test a realistic polling scenario with multiple status changes."""
        # Simulate job progression: Pending -> InProgress -> Completed
        self.mock_sm_client.describe_inference_recommendations_job.side_effect = [
            {'Status': 'Pending'},
            {'Status': 'InProgress'},
            {'Status': 'InProgress'},
            {'Status': 'Completed', 'OutputLocation': 's3://test-bucket/output.json'}
        ]
        
        start_time = time.time()
        
        with patch('time.sleep') as mock_sleep:
            result = self.poller.poll_until_complete("test-job-123")
        
        end_time = time.time()
        
        self.assertEqual(result, 's3://test-bucket/output.json')
        self.assertEqual(self.mock_sm_client.describe_inference_recommendations_job.call_count, 4)
        
        # Verify exponential backoff was applied
        expected_delays = [1.0, 1.5, 2.25]  # 1 * 1.5^0, 1 * 1.5^1, 1 * 1.5^2
        actual_delays = [call.args[0] for call in mock_sleep.call_args_list]
        
        for expected, actual in zip(expected_delays, actual_delays):
            self.assertAlmostEqual(expected, actual, places=2)


if __name__ == "__main__":
    unittest.main()