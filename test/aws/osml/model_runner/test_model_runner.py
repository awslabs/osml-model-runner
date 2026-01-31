#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

import unittest
from unittest.mock import MagicMock, patch

from aws.osml.model_runner.model_runner import ModelRunner, RetryableJobException


class TestModelRunner(unittest.TestCase):
    def setUp(self):
        # Create the instance of ModelRunner
        self.runner = ModelRunner()
        # Mock the process methods
        self.runner.image_request_handler = MagicMock()
        self.runner.region_request_handler = MagicMock()

    def test_run_starts_monitoring(self):
        """Test that the `run` method sets up and starts the monitoring loop."""
        # Mock method calls
        self.runner.monitor_work_queues = MagicMock()

        # Call the method
        self.runner.run()

        # Ensure the run method calls monitor_work_queues and sets `self.running`
        self.assertTrue(self.runner.running)
        self.runner.monitor_work_queues.assert_called_once()

    def test_stop_stops_running(self):
        """Test that the `stop` method correctly stops the runner."""
        # Call stop
        self.runner.stop()

        # Check if `self.running` is set to False
        self.assertFalse(self.runner.running)

    @patch("aws.osml.model_runner.model_runner.RegionRequestHandler.process_region_request")
    @patch("aws.osml.model_runner.model_runner.RequestQueue.finish_request")
    @patch("aws.osml.model_runner.model_runner.load_gdal_dataset")
    def test_process_region_requests_success(self, mock_load_gdal, mock_finish_request, mock_process_region):
        """Test processing of region requests successfully."""
        mock_region_request_item = MagicMock()
        mock_image_request_item = MagicMock()
        self.runner._get_or_create_region_request_item = MagicMock(return_value=mock_region_request_item)
        mock_load_gdal.return_value = (MagicMock(), MagicMock())
        mock_process_region.return_value = mock_image_request_item
        self.runner.image_request_table.is_image_request_complete = MagicMock(return_value=True)

        # Simulate queue data
        self.runner.region_requests_iter = iter([("receipt_handle", {"region_id": "region_123"})])

        # Call method
        self.runner._process_region_requests()

        # Ensure region request was processed correctly
        self.runner.image_request_handler.complete_image_request.assert_called_once()
        mock_finish_request.assert_called_once_with("receipt_handle")

    def test_process_image_request_noimage(self):
        """Test path where the scheduler does not return an ImageRequest to process"""
        with patch.object(self.runner, "image_job_scheduler", new_callable=MagicMock) as mock_scheduler:
            mock_scheduler.get_next_scheduled_request.return_value = None
            result = self.runner._process_image_requests()
            self.assertFalse(result)

    def test_process_image_requests_retryable(self):
        """Test that a RetryableJobException resets the request."""

        with patch.object(self.runner, "image_job_scheduler", new_callable=MagicMock) as mock_scheduler, patch.object(
            self.runner, "image_request_handler", new_callable=MagicMock
        ) as mock_handler:
            mock_image_request = MagicMock()
            mock_scheduler.get_next_scheduled_request.return_value = mock_image_request
            mock_handler.process_image_request.side_effect = RetryableJobException()

            result = self.runner._process_image_requests()
            self.assertTrue(result)

            mock_scheduler.finish_request.assert_called_once_with(mock_image_request, should_retry=True)

    @patch("aws.osml.model_runner.model_runner.ImageRequest")
    def test_process_image_requests_general_error(self, mock_image_request):
        """Test that general exceptions mark the image request as failed."""

        with patch.object(self.runner, "image_job_scheduler", new_callable=MagicMock) as mock_scheduler, patch.object(
            self.runner, "image_request_handler", new_callable=MagicMock
        ) as mock_handler:
            mock_image_request = MagicMock()
            mock_scheduler.get_next_scheduled_request.return_value = mock_image_request
            mock_handler.process_image_request.side_effect = Exception("Some error")

            result = self.runner._process_image_requests()
            self.assertTrue(result)

            mock_scheduler.finish_request.assert_called_once_with(mock_image_request)

    @patch("aws.osml.model_runner.model_runner.RegionRequestHandler.process_region_request")
    @patch("aws.osml.model_runner.model_runner.RequestQueue.finish_request")
    @patch("aws.osml.model_runner.model_runner.RequestQueue.reset_request")
    def test_process_region_requests_general_error(self, mock_reset_request, mock_finish_request, mock_process_region):
        """Test that general exceptions log an error and complete the request."""
        # Mock exception
        mock_process_region.side_effect = Exception("Some region processing error")

        # Simulate queue data
        self.runner.region_requests_iter = iter([("receipt_handle", {"region_id": "region_123"})])

        # Call method
        self.runner._process_region_requests()

        # Ensure the request was completed and logged
        mock_finish_request.assert_called_once_with("receipt_handle")
        mock_reset_request.assert_not_called()  # Ensure no reset on general errors

    @patch("aws.osml.model_runner.model_runner.set_gdal_default_configuration")
    def test_monitor_work_queues_handles_unexpected_exception_stops_running(self, mock_set_gdal):
        """Test that unexpected exceptions in monitor_work_queues stop the runner."""
        # Arrange
        self.runner._process_region_requests = MagicMock(side_effect=ValueError("Unexpected error"))
        self.runner.running = True

        # Act
        self.runner.monitor_work_queues()

        # Assert
        self.assertFalse(self.runner.running)
        self.runner._process_region_requests.assert_called_once()

    def test_process_region_requests_with_empty_attributes_returns_false(self):
        """Test that empty region_request_attributes returns False without processing."""
        # Arrange - Test with None
        self.runner.region_requests_iter = iter([("receipt_handle", None)])
        self.runner.region_request_queue.finish_request = MagicMock()

        # Act
        result = self.runner._process_region_requests()

        # Assert
        self.assertFalse(result)
        self.runner.region_request_queue.finish_request.assert_not_called()

    def test_process_region_requests_with_empty_dict_returns_false(self):
        """Test that empty dict region_request_attributes returns False without processing."""
        # Arrange - Test with empty dict
        self.runner.region_requests_iter = iter([("receipt_handle", {})])
        self.runner.region_request_queue.finish_request = MagicMock()

        # Act
        result = self.runner._process_region_requests()

        # Assert
        self.assertFalse(result)
        self.runner.region_request_queue.finish_request.assert_not_called()

    @patch("aws.osml.model_runner.model_runner.load_gdal_dataset")
    @patch("aws.osml.model_runner.model_runner.get_image_path")
    def test_process_region_requests_retryable_exception_resets_request(self, mock_get_path, mock_load_gdal):
        """Test that RetryableJobException resets the request with visibility_timeout=0."""
        # Arrange
        mock_load_gdal.return_value = (MagicMock(), MagicMock())
        mock_get_path.return_value = "/tmp/image.tif"

        mock_region_request_item = MagicMock()
        self.runner._get_or_create_region_request_item = MagicMock(return_value=mock_region_request_item)

        # Mock the region_request_handler to raise RetryableJobException
        self.runner.region_request_handler.process_region_request = MagicMock(side_effect=RetryableJobException("Throttled"))

        self.runner.region_requests_iter = iter([("receipt_handle", {"region_id": "region_123"})])
        self.runner.region_request_queue.reset_request = MagicMock()
        self.runner.region_request_queue.finish_request = MagicMock()

        # Act
        result = self.runner._process_region_requests()

        # Assert
        self.assertTrue(result)
        self.runner.region_request_queue.reset_request.assert_called_once_with("receipt_handle", visibility_timeout=0)
        self.runner.region_request_queue.finish_request.assert_not_called()

    @patch("aws.osml.model_runner.model_runner.ThreadingLocalContextFilter")
    @patch("aws.osml.model_runner.model_runner.load_gdal_dataset")
    @patch("aws.osml.model_runner.model_runner.get_image_path")
    def test_process_region_requests_clears_context_in_finally(self, mock_get_path, mock_load_gdal, mock_context_filter):
        """Test that ThreadingLocalContextFilter.set_context(None) is called in finally block."""
        # Arrange
        mock_load_gdal.side_effect = Exception("Processing failed")
        mock_get_path.return_value = "/tmp/image.tif"
        self.runner.region_requests_iter = iter([("receipt_handle", {"region_id": "region_123"})])

        # Act
        self.runner._process_region_requests()

        # Assert
        # Verify set_context was called twice: once with attributes, once with None
        self.assertEqual(mock_context_filter.set_context.call_count, 2)
        # First call with attributes
        mock_context_filter.set_context.assert_any_call({"region_id": "region_123"})
        # Second call with None in finally block
        mock_context_filter.set_context.assert_any_call(None)

    @patch("aws.osml.model_runner.model_runner.ThreadingLocalContextFilter")
    def test_process_image_requests_clears_context_in_finally(self, mock_context_filter):
        """Test that ThreadingLocalContextFilter.set_context(None) is called in finally block for image requests."""
        # Arrange
        mock_image_request = MagicMock()
        mock_image_request.image_id = "img123"
        mock_image_request.job_id = "job456"

        # Mock the scheduler using patch.object approach from existing tests
        with patch.object(self.runner, "image_job_scheduler", spec_set=True) as mock_scheduler:
            mock_scheduler.get_next_scheduled_request.return_value = mock_image_request
            mock_scheduler.finish_request = MagicMock()

            # Mock the handler to raise exception
            self.runner.image_request_handler.process_image_request.side_effect = Exception("Processing failed")

            # Act
            self.runner._process_image_requests()

            # Assert
            # Verify set_context was called twice: once with request dict, once with None
            self.assertEqual(mock_context_filter.set_context.call_count, 2)
            # First call should have been with the image request's __dict__
            first_call_arg = mock_context_filter.set_context.call_args_list[0][0][0]
            self.assertIn("image_id", first_call_arg)
            # Second call with None in finally block
            mock_context_filter.set_context.assert_any_call(None)

    def test_fail_image_request_with_none_image_request_uses_empty_strings(self):
        """Test that _fail_image_request handles None image_request without AttributeError."""
        # Arrange
        error = Exception("Test error")

        # Act
        self.runner._fail_image_request(None, error)

        # Assert
        # Verify fail_image_request was called
        self.runner.image_request_handler.fail_image_request.assert_called_once()
        # Get the ImageRequestItem that was passed
        call_args = self.runner.image_request_handler.fail_image_request.call_args[0]
        image_item = call_args[0]
        # Verify it has empty strings for image_id and job_id
        self.assertEqual(image_item.image_id, "")
        self.assertEqual(image_item.job_id, "")

    def test_get_or_create_region_request_item_returns_existing_item(self):
        """Test that _get_or_create_region_request_item returns existing item without creating new one."""
        # Arrange
        mock_region_request = MagicMock()
        mock_region_request.region_id = "region_123"
        mock_region_request.image_id = "img_456"

        existing_item = MagicMock()
        self.runner.region_request_table.get_region_request = MagicMock(return_value=existing_item)
        self.runner.region_request_table.start_region_request = MagicMock()

        # Act
        result = self.runner._get_or_create_region_request_item(mock_region_request)

        # Assert
        self.assertEqual(result, existing_item)
        self.runner.region_request_table.get_region_request.assert_called_once_with("region_123", "img_456")
        # Verify start_region_request was NOT called since item already exists
        self.runner.region_request_table.start_region_request.assert_not_called()


if __name__ == "__main__":
    unittest.main()
