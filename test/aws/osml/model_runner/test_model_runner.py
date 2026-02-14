#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

from unittest.mock import MagicMock, patch

import pytest

from aws.osml.model_runner.common import RequestStatus
from aws.osml.model_runner.model_runner import ModelRunner, RetryableJobException


@pytest.fixture
def model_runner_setup():
    """Set up test fixtures for ModelRunner tests."""
    # Create the instance of ModelRunner
    runner = ModelRunner()
    # Mock the process methods
    runner.image_request_handler = MagicMock()
    runner.region_request_handler = MagicMock()

    yield runner


def test_run_starts_monitoring(model_runner_setup):
    """Test that the `run` method sets up and starts the monitoring loop."""
    runner = model_runner_setup

    # Mock method calls
    runner.monitor_work_queues = MagicMock()

    # Call the method
    runner.run()

    # Ensure the run method calls monitor_work_queues and sets `self.running`
    assert runner.running is True
    runner.monitor_work_queues.assert_called_once()


def test_stop_stops_running(model_runner_setup):
    """Test that the `stop` method correctly stops the runner."""
    runner = model_runner_setup

    # Call stop
    runner.stop()

    # Check if `self.running` is set to False
    assert runner.running is False


@patch("aws.osml.model_runner.model_runner.RegionRequestHandler.process_region_request")
@patch("aws.osml.model_runner.model_runner.RequestQueue.finish_request")
@patch("aws.osml.model_runner.model_runner.load_gdal_dataset")
def test_process_region_requests_success(mock_load_gdal, mock_finish_request, mock_process_region, model_runner_setup):
    """Test processing of region requests successfully."""
    runner = model_runner_setup

    mock_region_request_item = MagicMock()
    mock_image_request_item = MagicMock()
    runner._get_or_create_region_request_item = MagicMock(return_value=mock_region_request_item)
    mock_load_gdal.return_value = (MagicMock(), MagicMock())
    mock_process_region.return_value = mock_image_request_item
    runner.image_request_table.is_image_request_complete = MagicMock(return_value=True)

    # Simulate queue data
    runner.region_requests_iter = iter([("receipt_handle", {"region_id": "region_123"})])

    # Call method
    runner._process_region_requests()

    # Ensure region request was processed correctly
    runner.image_request_handler.complete_image_request.assert_called_once()
    mock_finish_request.assert_called_once_with("receipt_handle")


def test_process_image_request_noimage(model_runner_setup):
    """Test path where the scheduler does not return an ImageRequest to process"""
    runner = model_runner_setup

    with patch.object(runner, "image_job_scheduler", new_callable=MagicMock) as mock_scheduler:
        mock_scheduler.get_next_scheduled_request.return_value = None
        result = runner._process_image_requests()
        assert result is False


def test_process_image_requests_retryable(model_runner_setup):
    """Test that a RetryableJobException resets the request."""
    runner = model_runner_setup

    with patch.object(runner, "image_job_scheduler", new_callable=MagicMock) as mock_scheduler, patch.object(
        runner, "image_request_handler", new_callable=MagicMock
    ) as mock_handler:
        mock_image_request = MagicMock()
        mock_scheduler.get_next_scheduled_request.return_value = mock_image_request
        mock_handler.process_image_request.side_effect = RetryableJobException()

        result = runner._process_image_requests()
        assert result is True

        mock_scheduler.finish_request.assert_called_once_with(mock_image_request, should_retry=True)


@patch("aws.osml.model_runner.model_runner.ImageRequest")
def test_process_image_requests_general_error(mock_image_request, model_runner_setup):
    """Test that general exceptions mark the image request as failed."""
    runner = model_runner_setup

    with patch.object(runner, "image_job_scheduler", new_callable=MagicMock) as mock_scheduler, patch.object(
        runner, "image_request_handler", new_callable=MagicMock
    ) as mock_handler:
        mock_image_request = MagicMock()
        mock_scheduler.get_next_scheduled_request.return_value = mock_image_request
        mock_handler.process_image_request.side_effect = Exception("Some error")

        result = runner._process_image_requests()
        assert result is True

        mock_scheduler.finish_request.assert_called_once_with(mock_image_request)


@patch("aws.osml.model_runner.model_runner.RegionRequestHandler.process_region_request")
@patch("aws.osml.model_runner.model_runner.RequestQueue.finish_request")
@patch("aws.osml.model_runner.model_runner.RequestQueue.reset_request")
def test_process_region_requests_general_error(
    mock_reset_request, mock_finish_request, mock_process_region, model_runner_setup
):
    """Test that general exceptions log an error and complete the request."""
    runner = model_runner_setup

    # Mock exception
    mock_process_region.side_effect = Exception("Some region processing error")

    # Simulate queue data
    runner.region_requests_iter = iter([("receipt_handle", {"region_id": "region_123"})])

    # Call method
    runner._process_region_requests()

    # Ensure the request was completed and logged
    mock_finish_request.assert_called_once_with("receipt_handle")
    mock_reset_request.assert_not_called()  # Ensure no reset on general errors


def test_update_requested_jobs_for_region_completion_success(model_runner_setup):
    """Test that successful requested-jobs updates do not trigger failure paths."""
    runner = model_runner_setup
    runner.requested_jobs_table = MagicMock()
    runner.region_request_table = MagicMock()
    runner.image_request_table = MagicMock()
    runner.region_status_monitor = MagicMock()

    image_request_item = MagicMock()
    region_request_item = MagicMock()
    region_request_item.region_id = "region-1"

    runner._update_requested_jobs_for_region_completion(image_request_item, region_request_item, RequestStatus.SUCCESS)

    runner.requested_jobs_table.complete_region.assert_called_once_with(image_request_item, "region-1")
    runner.region_request_table.complete_region_request.assert_not_called()
    runner.image_request_table.complete_region_request.assert_not_called()


def test_update_requested_jobs_for_region_completion_failure_marks_failed(model_runner_setup):
    """Test that requested-jobs update failures downgrade region and image status to failed."""
    runner = model_runner_setup
    runner.requested_jobs_table = MagicMock()
    runner.region_request_table = MagicMock()
    runner.image_request_table = MagicMock()
    runner.region_status_monitor = MagicMock()

    image_request_item = MagicMock()
    region_request_item = MagicMock()
    region_request_item.region_id = "region-1"
    region_request_item.image_id = "image-1"
    region_request_item.job_id = "job-1"
    runner.requested_jobs_table.complete_region.side_effect = Exception("ddb failure")
    runner.region_request_table.complete_region_request.return_value = region_request_item

    runner._update_requested_jobs_for_region_completion(image_request_item, region_request_item, RequestStatus.SUCCESS)

    runner.requested_jobs_table.complete_region.assert_called_once_with(image_request_item, "region-1")
    runner.region_request_table.complete_region_request.assert_called_once_with(region_request_item, RequestStatus.FAILED)
    runner.region_status_monitor.process_event.assert_called_once()
    runner.image_request_table.complete_region_request.assert_called_once_with("image-1", error=True)


@patch("aws.osml.model_runner.model_runner.set_gdal_default_configuration")
def test_monitor_work_queues_handles_unexpected_exception_stops_running(mock_set_gdal, model_runner_setup):
    """Test that unexpected exceptions in monitor_work_queues stop the runner."""
    runner = model_runner_setup

    # Arrange
    runner._process_region_requests = MagicMock(side_effect=ValueError("Unexpected error"))
    runner.running = True

    # Act
    runner.monitor_work_queues()

    # Assert
    assert runner.running is False
    runner._process_region_requests.assert_called_once()


def test_process_region_requests_with_empty_attributes_returns_false(model_runner_setup):
    """Test that empty region_request_attributes returns False without processing."""
    runner = model_runner_setup

    # Arrange - Test with None
    runner.region_requests_iter = iter([("receipt_handle", None)])
    runner.region_request_queue.finish_request = MagicMock()

    # Act
    result = runner._process_region_requests()

    # Assert
    assert result is False
    runner.region_request_queue.finish_request.assert_not_called()


def test_process_region_requests_with_empty_dict_returns_false(model_runner_setup):
    """Test that empty dict region_request_attributes returns False without processing."""
    runner = model_runner_setup

    # Arrange - Test with empty dict
    runner.region_requests_iter = iter([("receipt_handle", {})])
    runner.region_request_queue.finish_request = MagicMock()

    # Act
    result = runner._process_region_requests()

    # Assert
    assert result is False
    runner.region_request_queue.finish_request.assert_not_called()


@patch("aws.osml.model_runner.model_runner.load_gdal_dataset")
@patch("aws.osml.model_runner.model_runner.get_image_path")
def test_process_region_requests_retryable_exception_resets_request(mock_get_path, mock_load_gdal, model_runner_setup):
    """Test that RetryableJobException resets the request with visibility_timeout=0."""
    runner = model_runner_setup

    # Arrange
    mock_load_gdal.return_value = (MagicMock(), MagicMock())
    mock_get_path.return_value = "/tmp/image.tif"

    mock_region_request_item = MagicMock()
    runner._get_or_create_region_request_item = MagicMock(return_value=mock_region_request_item)

    # Mock the region_request_handler to raise RetryableJobException
    runner.region_request_handler.process_region_request = MagicMock(side_effect=RetryableJobException("Throttled"))

    runner.region_requests_iter = iter([("receipt_handle", {"region_id": "region_123"})])
    runner.region_request_queue.reset_request = MagicMock()
    runner.region_request_queue.finish_request = MagicMock()

    # Act
    result = runner._process_region_requests()

    # Assert
    assert result is True
    runner.region_request_queue.reset_request.assert_called_once_with("receipt_handle", visibility_timeout=0)
    runner.region_request_queue.finish_request.assert_not_called()


@patch("aws.osml.model_runner.model_runner.ThreadingLocalContextFilter")
@patch("aws.osml.model_runner.model_runner.load_gdal_dataset")
@patch("aws.osml.model_runner.model_runner.get_image_path")
def test_process_region_requests_clears_context_in_finally(
    mock_get_path, mock_load_gdal, mock_context_filter, model_runner_setup
):
    """Test that ThreadingLocalContextFilter.set_context(None) is called in finally block."""
    runner = model_runner_setup

    # Arrange
    mock_load_gdal.side_effect = Exception("Processing failed")
    mock_get_path.return_value = "/tmp/image.tif"
    runner.region_requests_iter = iter([("receipt_handle", {"region_id": "region_123"})])

    # Act
    runner._process_region_requests()

    # Assert
    # Verify set_context was called twice: once with attributes, once with None
    assert mock_context_filter.set_context.call_count == 2
    # First call with attributes
    mock_context_filter.set_context.assert_any_call({"region_id": "region_123"})
    # Second call with None in finally block
    mock_context_filter.set_context.assert_any_call(None)


@patch("aws.osml.model_runner.model_runner.ThreadingLocalContextFilter")
def test_process_image_requests_clears_context_in_finally(mock_context_filter, model_runner_setup):
    """Test that ThreadingLocalContextFilter.set_context(None) is called in finally block for image requests."""
    runner = model_runner_setup

    # Arrange
    mock_image_request = MagicMock()
    mock_image_request.image_id = "img123"
    mock_image_request.job_id = "job456"

    # Mock the scheduler using patch.object approach from existing tests
    with patch.object(runner, "image_job_scheduler", spec_set=True) as mock_scheduler:
        mock_scheduler.get_next_scheduled_request.return_value = mock_image_request
        mock_scheduler.finish_request = MagicMock()

        # Mock the handler to raise exception
        runner.image_request_handler.process_image_request.side_effect = Exception("Processing failed")

        # Act
        runner._process_image_requests()

        # Assert
        # Verify set_context was called twice: once with request dict, once with None
        assert mock_context_filter.set_context.call_count == 2
        # First call should have been with the image request's __dict__
        first_call_arg = mock_context_filter.set_context.call_args_list[0][0][0]
        assert "image_id" in first_call_arg
        # Second call with None in finally block
        mock_context_filter.set_context.assert_any_call(None)


def test_fail_image_request_with_none_image_request_uses_empty_strings(model_runner_setup):
    """Test that _fail_image_request handles None image_request without AttributeError."""
    runner = model_runner_setup

    # Arrange
    error = Exception("Test error")

    # Act
    runner._fail_image_request(None, error)

    # Assert
    # Verify fail_image_request was called
    runner.image_request_handler.fail_image_request.assert_called_once()
    # Get the ImageRequestItem that was passed
    call_args = runner.image_request_handler.fail_image_request.call_args[0]
    image_item = call_args[0]
    # Verify it has empty strings for image_id and job_id
    assert image_item.image_id == ""
    assert image_item.job_id == ""


def test_get_or_create_region_request_item_returns_existing_item(model_runner_setup):
    """Test that _get_or_create_region_request_item returns existing item without creating new one."""
    runner = model_runner_setup

    # Arrange
    mock_region_request = MagicMock()
    mock_region_request.region_id = "region_123"
    mock_region_request.image_id = "img_456"

    existing_item = MagicMock()
    runner.region_request_table.get_region_request = MagicMock(return_value=existing_item)
    runner.region_request_table.start_region_request = MagicMock()

    # Act
    result = runner._get_or_create_region_request_item(mock_region_request)

    # Assert
    assert result == existing_item
    runner.region_request_table.get_region_request.assert_called_once_with("region_123", "img_456")
    # Verify start_region_request was NOT called since item already exists
    runner.region_request_table.start_region_request.assert_not_called()
