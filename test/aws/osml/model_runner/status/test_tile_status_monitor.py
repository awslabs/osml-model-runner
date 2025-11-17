#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

import unittest
from unittest import TestCase
from unittest.mock import Mock, patch

import pytest


class TestTileStatusMonitor(TestCase):
    """Unit tests for TileStatusMonitor class"""

    def test_monitor_initialization(self):
        """Test TileStatusMonitor initialization"""
        from aws.osml.model_runner.status.tile_status_monitor import TileStatusMonitor

        with patch("aws.osml.model_runner.status.sns_helper.boto3"):
            monitor = TileStatusMonitor("test-topic-arn")
            assert monitor.sns_helper.topic_arn == "test-topic-arn"

    def test_monitor_initialization_without_topic(self):
        """Test TileStatusMonitor initialization without topic ARN"""
        from aws.osml.model_runner.status.tile_status_monitor import TileStatusMonitor

        with patch("aws.osml.model_runner.status.sns_helper.boto3"):
            monitor = TileStatusMonitor(None)
            assert monitor.sns_helper.topic_arn is None
            assert monitor.sns_helper.sns_client is None

    def test_get_status_success(self):
        """Test getting status from tile request item with SUCCESS status"""
        from aws.osml.model_runner.status.tile_status_monitor import TileStatusMonitor
        from aws.osml.model_runner.common import RequestStatus
        from aws.osml.model_runner.database import TileRequestItem

        with patch("aws.osml.model_runner.status.sns_helper.boto3"):
            monitor = TileStatusMonitor("test-topic-arn")

            tile_item = TileRequestItem(
                tile_id="test-tile-123",
                region_id="test-region-789",
                tile_status=RequestStatus.SUCCESS,
            )

            status = monitor.get_status(tile_item)
            assert status == RequestStatus.SUCCESS

    def test_get_status_pending_maps_to_in_progress(self):
        """Test that PENDING status maps to IN_PROGRESS"""
        from aws.osml.model_runner.status.tile_status_monitor import TileStatusMonitor
        from aws.osml.model_runner.common import RequestStatus
        from aws.osml.model_runner.database import TileRequestItem

        with patch("aws.osml.model_runner.status.sns_helper.boto3"):
            monitor = TileStatusMonitor("test-topic-arn")

            tile_item = TileRequestItem(
                tile_id="test-tile-123",
                region_id="test-region-789",
                tile_status=RequestStatus.PENDING,
            )

            status = monitor.get_status(tile_item)
            assert status == RequestStatus.IN_PROGRESS

    def test_process_event_success(self):
        """Test processing a successful tile event"""
        from aws.osml.model_runner.status.tile_status_monitor import TileStatusMonitor
        from aws.osml.model_runner.common import RequestStatus
        from aws.osml.model_runner.database import TileRequestItem

        with patch("aws.osml.model_runner.status.sns_helper.boto3") as mock_boto3:
            mock_sns = Mock()
            mock_boto3.client.return_value = mock_sns

            monitor = TileStatusMonitor("test-topic-arn")

            tile_item = TileRequestItem(
                tile_id="test-tile-123",
                region_id="test-region-789",
                job_id="test-job-001",
                image_id="test-image-456",
                tile_status=RequestStatus.SUCCESS,
                processing_duration=1500,
            )

            # Process event should not raise exception
            monitor.process_event(tile_item, RequestStatus.SUCCESS, "Tile completed successfully")

    def test_process_event_failed(self):
        """Test processing a failed tile event"""
        from aws.osml.model_runner.status.tile_status_monitor import TileStatusMonitor
        from aws.osml.model_runner.common import RequestStatus
        from aws.osml.model_runner.database import TileRequestItem

        with patch("aws.osml.model_runner.status.sns_helper.boto3") as mock_boto3:
            mock_sns = Mock()
            mock_boto3.client.return_value = mock_sns

            monitor = TileStatusMonitor("test-topic-arn")

            tile_item = TileRequestItem(
                tile_id="test-tile-123",
                region_id="test-region-789",
                job_id="test-job-001",
                image_id="test-image-456",
                tile_status=RequestStatus.FAILED,
            )

            # Process event should not raise exception
            monitor.process_event(tile_item, RequestStatus.FAILED, "Tile processing failed")

    def test_process_event_handles_exception(self):
        """Test that process_event handles exceptions gracefully"""
        from aws.osml.model_runner.status.tile_status_monitor import TileStatusMonitor
        from aws.osml.model_runner.common import RequestStatus

        with patch("aws.osml.model_runner.status.sns_helper.boto3") as mock_boto3:
            mock_sns = Mock()
            mock_boto3.client.return_value = mock_sns

            monitor = TileStatusMonitor("test-topic-arn")

            # Pass invalid item that will cause exception
            invalid_item = None  # This will cause an error

            # Should not raise exception, just log error
            monitor.process_event(invalid_item, RequestStatus.FAILED, "Test message")

    def test_get_status_failed(self):
        """Test getting FAILED status from tile request item"""
        from aws.osml.model_runner.status.tile_status_monitor import TileStatusMonitor
        from aws.osml.model_runner.common import RequestStatus
        from aws.osml.model_runner.database import TileRequestItem

        with patch("aws.osml.model_runner.status.sns_helper.boto3"):
            monitor = TileStatusMonitor("test-topic-arn")

            tile_item = TileRequestItem(
                tile_id="test-tile-123",
                region_id="test-region-789",
                tile_status=RequestStatus.FAILED,
            )

            status = monitor.get_status(tile_item)
            assert status == RequestStatus.FAILED

    def test_get_status_in_progress(self):
        """Test getting IN_PROGRESS status from tile request item"""
        from aws.osml.model_runner.status.tile_status_monitor import TileStatusMonitor
        from aws.osml.model_runner.common import RequestStatus
        from aws.osml.model_runner.database import TileRequestItem

        with patch("aws.osml.model_runner.status.sns_helper.boto3"):
            monitor = TileStatusMonitor("test-topic-arn")

            tile_item = TileRequestItem(
                tile_id="test-tile-123",
                region_id="test-region-789",
                tile_status=RequestStatus.IN_PROGRESS,
            )

            status = monitor.get_status(tile_item)
            assert status == RequestStatus.IN_PROGRESS

    def test_get_status_unknown_status(self):
        """Test getting status with unknown status value"""
        from aws.osml.model_runner.status.tile_status_monitor import TileStatusMonitor
        from aws.osml.model_runner.common import RequestStatus
        from aws.osml.model_runner.database import TileRequestItem

        with patch("aws.osml.model_runner.status.sns_helper.boto3"):
            monitor = TileStatusMonitor("test-topic-arn")

            tile_item = TileRequestItem(
                tile_id="test-tile-123",
                region_id="test-region-789",
                tile_status="UNKNOWN_STATUS",  # Unknown status
            )

            status = monitor.get_status(tile_item)
            # Should default to IN_PROGRESS
            assert status == RequestStatus.IN_PROGRESS

    def test_get_status_no_tile_status_attribute(self):
        """Test getting status when tile_status attribute is missing"""
        from aws.osml.model_runner.status.tile_status_monitor import TileStatusMonitor
        from aws.osml.model_runner.common import RequestStatus

        with patch("aws.osml.model_runner.status.sns_helper.boto3"):
            monitor = TileStatusMonitor("test-topic-arn")

            # Create object without tile_status attribute
            tile_item = Mock()
            del tile_item.tile_status  # Remove the attribute

            status = monitor.get_status(tile_item)
            # Should default to IN_PROGRESS
            assert status == RequestStatus.IN_PROGRESS

    def test_get_status_none_tile_status(self):
        """Test getting status when tile_status is None"""
        from aws.osml.model_runner.status.tile_status_monitor import TileStatusMonitor
        from aws.osml.model_runner.common import RequestStatus
        from aws.osml.model_runner.database import TileRequestItem

        with patch("aws.osml.model_runner.status.sns_helper.boto3"):
            monitor = TileStatusMonitor("test-topic-arn")

            tile_item = TileRequestItem(
                tile_id="test-tile-123",
                region_id="test-region-789",
                tile_status=None,  # None status
            )

            status = monitor.get_status(tile_item)
            # Should default to IN_PROGRESS
            assert status == RequestStatus.IN_PROGRESS

    # def test_get_status_exception_handling(self):
    #     """Test get_status handles exceptions and returns FAILED"""
    #     from aws.osml.model_runner.status.tile_status_monitor import TileStatusMonitor
    #     from aws.osml.model_runner.common import RequestStatus

    #     with patch("aws.osml.model_runner.status.sns_helper.boto3"):
    #         monitor = TileStatusMonitor("test-topic-arn")

    #         # Create object that will raise exception when accessing tile_status
    #         tile_item = Mock()
    #         tile_item.tile_status = property(lambda self: 1 / 0)  # Will raise ZeroDivisionError

    #         status = monitor.get_status(tile_item)
    #         # Should return FAILED on exception
    #         assert status == RequestStatus.FAILED

    def test_process_event_with_none_processing_duration(self):
        """Test processing event with None processing_duration"""
        from aws.osml.model_runner.status.tile_status_monitor import TileStatusMonitor
        from aws.osml.model_runner.common import RequestStatus
        from aws.osml.model_runner.database import TileRequestItem

        with patch("aws.osml.model_runner.status.sns_helper.boto3") as mock_boto3:
            mock_sns = Mock()
            mock_boto3.client.return_value = mock_sns

            monitor = TileStatusMonitor("test-topic-arn")

            tile_item = TileRequestItem(
                tile_id="test-tile-123",
                region_id="test-region-789",
                job_id="test-job-001",
                image_id="test-image-456",
                tile_status=RequestStatus.SUCCESS,
                processing_duration=None,  # None duration
            )

            # Should not raise exception
            monitor.process_event(tile_item, RequestStatus.SUCCESS, "Tile completed")

    def test_process_event_with_missing_attributes(self):
        """Test processing event with missing attributes"""
        from aws.osml.model_runner.status.tile_status_monitor import TileStatusMonitor
        from aws.osml.model_runner.common import RequestStatus

        with patch("aws.osml.model_runner.status.sns_helper.boto3") as mock_boto3:
            mock_sns = Mock()
            mock_boto3.client.return_value = mock_sns

            monitor = TileStatusMonitor("test-topic-arn")

            # Create minimal mock object
            tile_item = Mock()
            tile_item.job_id = "test-job"
            tile_item.image_id = "test-image"
            tile_item.processing_duration = 0
            tile_item.__dict__ = {"job_id": "test-job", "image_id": "test-image"}

            # Should not raise exception
            monitor.process_event(tile_item, RequestStatus.IN_PROGRESS, "Processing")


if __name__ == "__main__":
    unittest.main()
