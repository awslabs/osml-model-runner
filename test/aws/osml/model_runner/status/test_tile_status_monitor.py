#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

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


if __name__ == "__main__":
    unittest.main()
