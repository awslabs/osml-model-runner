#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

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
        self.runner.region_request_table.get_or_create_region_request_item = MagicMock(return_value=mock_region_request_item)
        mock_load_gdal.return_value = (MagicMock(), MagicMock())
        mock_process_region.return_value = mock_image_request_item
        self.runner.region_request_table.is_image_request_complete = MagicMock(return_value=(True, None, None))

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
        self.assertEqual(mock_reset_request.call_count, 0)  # Ensure no reset on general errors

    def test_monitor_work_queues_processes_tiles_first(self):
        """Test that monitor_work_queues processes tiles before regions"""
        with patch.object(self.runner, "_process_tile_requests") as mock_tiles:
            with patch.object(self.runner, "_process_region_requests") as mock_regions:
                with patch.object(self.runner, "_process_image_requests") as mock_images:
                    # Set running to False after first iteration
                    def stop_after_first():
                        self.runner.running = False
                        return True

                    mock_tiles.side_effect = stop_after_first

                    self.runner.running = True
                    self.runner.monitor_work_queues()

                    # Should process tiles and not call regions/images
                    mock_tiles.assert_called_once()
                    self.assertEqual(mock_regions.call_count, 0)
                    self.assertEqual(mock_images.call_count, 0)

    def test_monitor_work_queues_processes_regions_when_no_tiles(self):
        """Test that monitor_work_queues processes regions when no tiles"""
        with patch.object(self.runner, "_process_tile_requests", return_value=False):
            with patch.object(self.runner, "_process_region_requests") as mock_regions:
                with patch.object(self.runner, "_process_image_requests") as mock_images:
                    def stop_after_first():
                        self.runner.running = False
                        return True

                    mock_regions.side_effect = stop_after_first

                    self.runner.running = True
                    self.runner.monitor_work_queues()

                    mock_regions.assert_called_once()
                    self.assertEqual(mock_images.call_count, 0)

    def test_monitor_work_queues_processes_images_when_no_tiles_or_regions(self):
        """Test that monitor_work_queues processes images when no tiles or regions"""
        with patch.object(self.runner, "_process_tile_requests", return_value=False):
            with patch.object(self.runner, "_process_region_requests", return_value=False):
                with patch.object(self.runner, "_process_image_requests", return_value=True) as mock_images:
                    def stop_after_first():
                        self.runner.running = False
                        return True

                    mock_images.side_effect = stop_after_first

                    self.runner.running = True
                    self.runner.monitor_work_queues()

                    mock_images.assert_called_once()

    def test_monitor_work_queues_exception_stops_runner(self):
        """Test that exceptions in monitor_work_queues stop the runner"""
        with patch.object(self.runner, "_process_tile_requests", side_effect=Exception("Test error")):
            self.runner.running = True
            self.runner.monitor_work_queues()

            # Should stop running after exception
            self.assertFalse(self.runner.running)

    def test_process_tile_requests_no_tiles(self):
        """Test _process_tile_requests when no tiles available"""
        self.runner.tile_requests_iter = iter([])

        result = self.runner._process_tile_requests()

        self.assertFalse(result)

    @patch("aws.osml.model_runner.model_runner.TileRequest")
    def test_process_tile_requests_success(self, mock_tile_request_class):
        """Test _process_tile_requests successful processing"""
        from aws.osml.model_runner.database import TileRequestItem
        from aws.osml.model_runner.common import RequestStatus

        # Mock tile request item
        mock_tile_item = TileRequestItem(
            tile_id="test-tile-123",
            region_id="test-region-789",
            image_id="test-image-456",
            tile_status=RequestStatus.IN_PROGRESS,
        )

        self.runner.tile_request_table.get_tile_request_by_event = MagicMock(return_value=mock_tile_item)
        self.runner.tile_request_table.complete_tile_request = MagicMock(return_value=mock_tile_item)
        self.runner.tile_request_handler.process_tile_request = MagicMock(return_value=None)

        mock_tile_request = MagicMock()
        mock_tile_request_class.from_tile_request_item.return_value = mock_tile_request

        with patch.object(self.runner, "complete_tile_request"):
            event_message = {"tile_id": "test-tile-123"}
            self.runner.tile_requests_iter = iter([("receipt_handle", event_message)])

            result = self.runner._process_tile_requests()

            self.assertTrue(result)
            self.runner.tile_request_handler.process_tile_request.assert_called_once()

    def test_process_tile_requests_already_completed(self):
        """Test _process_tile_requests when tile already completed"""
        from aws.osml.model_runner.database import TileRequestItem
        from aws.osml.model_runner.common import RequestStatus

        mock_tile_item = TileRequestItem(
            tile_id="test-tile-123",
            region_id="test-region-789",
            tile_status=RequestStatus.SUCCESS,
        )

        self.runner.tile_request_table.get_tile_request_by_event = MagicMock(return_value=mock_tile_item)
        self.runner.tile_request_handler = MagicMock(return_value=True)

        with patch.object(self.runner, "complete_tile_request"):
            event_message = {"tile_id": "test-tile-123"}
            self.runner.tile_requests_iter = iter([("receipt_handle", event_message)])

            result = self.runner._process_tile_requests()

            self.assertTrue(result)
            # Should not process already completed tile
            self.assertEqual(self.runner.tile_request_handler.process_tile_request.call_count, 0)

    def test_process_tile_requests_failed_tile(self):
        """Test _process_tile_requests with failed tile"""
        from aws.osml.model_runner.database import TileRequestItem
        from aws.osml.model_runner.common import RequestStatus

        mock_tile_item = TileRequestItem(
            tile_id="test-tile-123",
            region_id="test-region-789",
            tile_status=RequestStatus.FAILED,
            error_message="Test error",
        )

        self.runner.tile_request_table.get_tile_request_by_event = MagicMock(return_value=mock_tile_item)
        self.runner.tile_request_handler.fail_tile_request = MagicMock(return_value=mock_tile_item)

        with patch.object(self.runner, "complete_tile_request"):
            event_message = {"tile_id": "test-tile-123", "failureReason": "Test error"}
            self.runner.tile_requests_iter = iter([("receipt_handle", event_message)])

            result = self.runner._process_tile_requests()

            self.assertTrue(result)
            self.runner.tile_request_handler.fail_tile_request.assert_called_once()

    def test_process_tile_requests_retryable_exception(self):
        """Test _process_tile_requests with retryable exception"""
        self.runner.tile_request_table.get_tile_request_by_event = MagicMock(return_value=None)
        self.runner.tile_request_queue.reset_request = MagicMock(return_value=None)

        event_message = {"tile_id": "test-tile-123"}
        self.runner.tile_requests_iter = iter([("receipt_handle", event_message)])

        result = self.runner._process_tile_requests()

        self.assertTrue(result)
        # Should reset request for retry
        self.runner.tile_request_queue.reset_request.assert_called_once()

    def test_process_tile_requests_self_throttled(self):
        """Test _process_tile_requests with self-throttling"""
        from aws.osml.model_runner.exceptions import SelfThrottledTileException
        from aws.osml.model_runner.database import TileRequestItem
        from aws.osml.model_runner.common import RequestStatus

        mock_tile_item = TileRequestItem(
            tile_id="test-tile-123",
            region_id="test-region-789",
            tile_status=RequestStatus.IN_PROGRESS,
        )

        self.runner.tile_request_table.get_tile_request_by_event = MagicMock(return_value=mock_tile_item)
        self.runner.tile_request_handler.process_tile_request = MagicMock(side_effect=SelfThrottledTileException())
        self.runner.tile_request_queue.reset_request = MagicMock(return_value=None)

        event_message = {"tile_id": "test-tile-123"}
        self.runner.tile_requests_iter = iter([("receipt_handle", event_message)])

        result = self.runner._process_tile_requests()

        self.assertTrue(result)
        # Should reset with throttling timeout
        self.runner.tile_request_queue.reset_request.assert_called_once()

    def test_process_region_requests_retryable_exception(self):
        """Test _process_region_requests with retryable exception"""
        from aws.osml.model_runner.exceptions import RetryableJobException

        self.runner.region_request_table.get_or_create_region_request_item = MagicMock(
            side_effect=RetryableJobException("Test retry")
        )
        self.runner.region_request_queue.reset_request = MagicMock(return_value=None)

        with patch("aws.osml.model_runner.model_runner.load_gdal_dataset") as mock_load:
            mock_load.return_value = (MagicMock(), MagicMock())

            region_attributes = {"region_id": "test-region-123"}
            self.runner.region_requests_iter = iter([("receipt_handle", region_attributes)])

            result = self.runner._process_region_requests()

            self.assertTrue(result)
            # Should reset request for retry
            self.runner.region_request_queue.reset_request.assert_called_once()

    def test_process_region_requests_self_throttled(self):
        """Test _process_region_requests with self-throttling"""
        from aws.osml.model_runner.exceptions import SelfThrottledRegionException

        self.runner.region_request_handler.process_region_request = MagicMock(
            side_effect=SelfThrottledRegionException()
        )
        self.runner.region_request_queue.reset_request = MagicMock(return_value=None)

        mock_region_request_item = MagicMock()
        self.runner.region_request_table.get_or_create_region_request_item = MagicMock(return_value=mock_region_request_item)

        with patch("aws.osml.model_runner.model_runner.load_gdal_dataset") as mock_load:
            with patch("aws.osml.model_runner.model_runner.get_image_path"):
                mock_load.return_value = (MagicMock(), MagicMock())

                region_attributes = {"region_id": "test-region-123"}
                self.runner.region_requests_iter = iter([("receipt_handle", region_attributes)])

                result = self.runner._process_region_requests()

                self.assertTrue(result)
                # Should reset with throttling timeout
                self.runner.region_request_queue.reset_request.assert_called_once()

    def test_fail_image_request(self):
        """Test _fail_image_request method"""
        from aws.osml.model_runner.api import ImageRequest

        mock_image_request = ImageRequest(
            image_id="test-image-123",
            job_id="test-job-456",
            image_url="s3://bucket/image.tif",
            outputs=[],
        )

        error = Exception("Test error")

        self.runner._fail_image_request(mock_image_request, error)

        # Should call fail_image_request on handler
        self.runner.image_request_handler.fail_image_request.assert_called_once()

    def test_check_if_region_request_complete_not_done(self):
        """Test check_if_region_request_complete when region not complete"""
        from aws.osml.model_runner.database import TileRequestItem, RegionRequestItem

        tile_item = TileRequestItem(
            tile_id="test-tile-123",
            region_id="test-region-789",
            image_id="test-image-456",
        )

        region_item = RegionRequestItem(
            region_id="test-region-789",
            image_id="test-image-456",
            total_tiles=10,  # Expecting 10 tiles
        )

        self.runner.region_request_table.get_region_request = MagicMock(return_value=region_item)
        self.runner.tile_request_table.get_region_request_complete_counts = MagicMock(return_value=(0, 5))  # Only 5 done

        result_region, is_done = self.runner.check_if_region_request_complete(tile_item)

        self.assertFalse(is_done)
        self.assertEqual(result_region, region_item)

    def test_check_if_region_request_complete_done(self):
        """Test check_if_region_request_complete when region is complete"""
        from aws.osml.model_runner.common import RequestStatus
        from aws.osml.model_runner.database import TileRequestItem, RegionRequestItem, ImageRequestItem

        tile_item = TileRequestItem(
            tile_id="test-tile-123",
            region_id="test-region-789",
            image_id="test-image-456",
        )

        region_item = RegionRequestItem(
            job_id="test-job-001",
            region_id="test-region-789",
            image_id="test-image-456",
            total_tiles=10,
            region_status=RequestStatus.IN_PROGRESS,
            processing_duration=1
        )

        image_item = ImageRequestItem(
            image_id="test-image-456",
            job_id="test-job-001",
        )

        self.runner.region_request_table.get_region_request = MagicMock(return_value=region_item)
        self.runner.tile_request_table.get_region_request_complete_counts = MagicMock(return_value=(0, 10))  # All done
        self.runner.region_request_table.complete_region_request = MagicMock(return_value=region_item)
        self.runner.image_request_table.complete_region_request = MagicMock(return_value=image_item)
        self.runner.region_status_monitor.get_status = MagicMock(return_value=RequestStatus.SUCCESS)

        with patch("aws.osml.model_runner.status.SNSHelper.publish_message") as mock_status:
            mock_status.return_value = None
            result_region, is_done = self.runner.check_if_region_request_complete(tile_item)

            self.assertTrue(is_done)
            self.runner.region_request_table.complete_region_request.assert_called_once()

    def test_check_if_region_request_complete_already_marked_done(self):
        """Test check_if_region_request_complete when already marked complete"""
        from aws.osml.model_runner.database import TileRequestItem, RegionRequestItem
        from aws.osml.model_runner.common import RequestStatus

        tile_item = TileRequestItem(
            tile_id="test-tile-123",
            region_id="test-region-789",
            image_id="test-image-456",
        )

        region_item = RegionRequestItem(
            region_id="test-region-789",
            image_id="test-image-456",
            total_tiles=10,
            region_status=RequestStatus.SUCCESS.name,  # Already marked complete
        )

        self.runner.region_request_table.get_region_request = MagicMock(return_value=region_item)
        self.runner.tile_request_table.get_region_request_complete_counts = MagicMock(return_value=(0, 10))
        self.runner.region_request_table.complete_region_request = MagicMock(return_value=region_item)

        result_region, is_done = self.runner.check_if_region_request_complete(tile_item)

        self.assertTrue(is_done)
        # Should not call complete_region_request again
        self.assertEqual(self.runner.region_request_table.complete_region_request.call_count, 0)

    def test_check_if_region_request_complete_no_total_tiles(self):
        """Test check_if_region_request_complete when total_tiles not set"""
        from aws.osml.model_runner.database import TileRequestItem, RegionRequestItem

        tile_item = TileRequestItem(
            tile_id="test-tile-123",
            region_id="test-region-789",
            image_id="test-image-456",
        )

        region_item = RegionRequestItem(
            region_id="test-region-789",
            image_id="test-image-456",
            total_tiles=None,  # Not set yet
        )

        self.runner.region_request_table.get_region_request = MagicMock(return_value=region_item)

        result_region, is_done = self.runner.check_if_region_request_complete(tile_item)

        self.assertFalse(is_done)

    def test_complete_tile_request_region_not_done(self):
        """Test complete_tile_request when region not complete"""
        from aws.osml.model_runner.database import TileRequestItem

        tile_item = TileRequestItem(
            tile_id="test-tile-123",
            region_id="test-region-789",
            image_id="test-image-456",
        )

        self.runner.image_request_table.get_image_request = MagicMock(return_value=tile_item)

        with patch.object(self.runner, "check_if_region_request_complete", return_value=(MagicMock(), False)):
            self.runner.complete_tile_request(tile_item)

            # Should not check if image is done
            self.assertEqual(self.runner.image_request_table.get_image_request.call_count, 0)

    def test_complete_tile_request_image_done_all_failed(self):
        """Test complete_tile_request when image done but all regions failed"""
        from aws.osml.model_runner.database import TileRequestItem, RegionRequestItem, ImageRequestItem

        tile_item = TileRequestItem(
            tile_id="test-tile-123",
            region_id="test-region-789",
            image_id="test-image-456",
            image_url="s3://bucket/image.tif",
            image_read_role="test-role",
        )

        region_item = RegionRequestItem(
            region_id="test-region-789",
            image_id="test-image-456",
        )

        image_item = ImageRequestItem(
            image_id="test-image-456",
            job_id="test-job-001",
        )

        with patch.object(self.runner, "check_if_region_request_complete", return_value=(region_item, True)):
            self.runner.image_request_table.get_image_request = MagicMock(return_value=image_item)
            self.runner.region_request_table.is_image_request_complete = MagicMock(return_value=(True, 0, 2))  # All failed
            self.runner.image_request_table.update_image_request = MagicMock(return_value=image_item)

            with patch.object(self.runner, "_fail_image_request"):
                self.runner.complete_tile_request(tile_item)

                # Should fail the image request
                self.runner._fail_image_request.assert_called_once()

    def test_complete_tile_request_image_done_success(self):
        """Test complete_tile_request when image successfully completed"""
        from aws.osml.model_runner.database import TileRequestItem, RegionRequestItem, ImageRequestItem

        tile_item = TileRequestItem(
            tile_id="test-tile-123",
            region_id="test-region-789",
            image_id="test-image-456",
            image_url="s3://bucket/image.tif",
            image_read_role="test-role",
        )

        region_item = RegionRequestItem(
            region_id="test-region-789",
            image_id="test-image-456",
            tile_size=[512, 512],
            tile_overlap=[64, 64],
        )

        image_item = ImageRequestItem(
            image_id="test-image-456",
            job_id="test-job-001",
        )

        with patch.object(self.runner, "check_if_region_request_complete", return_value=(region_item, True)):
            with patch("aws.osml.model_runner.model_runner.load_gdal_dataset") as mock_load:
                with patch("aws.osml.model_runner.model_runner.get_image_path"):
                    mock_raster = MagicMock()
                    mock_sensor = MagicMock()
                    mock_driver = MagicMock()
                    mock_driver.ShortName = "GTiff"
                    mock_raster.GetDriver.return_value = mock_driver
                    mock_load.return_value = (mock_raster, mock_sensor)

                    self.runner.image_request_table.get_image_request = MagicMock(return_value=image_item)
                    self.runner.region_request_table.is_image_request_complete = MagicMock(
                        return_value=(True, 2, 0)
                    )  # Success
                    self.runner.image_request_table.update_image_request = MagicMock(return_value=image_item)

                    self.runner.complete_tile_request(tile_item)

                    # Should complete the image request
                    self.runner.image_request_handler.complete_image_request.assert_called_once()


if __name__ == "__main__":
    unittest.main()

