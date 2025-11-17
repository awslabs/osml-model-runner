#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import unittest
from unittest import TestCase
from unittest.mock import Mock, MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from aws.osml.model_runner.common import RequestStatus


class TestTileRequestTable(TestCase):
    """Unit tests for TileRequestTable class"""

    def test_table_initialization(self):
        """Test TileRequestTable initialization"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable

        with patch("aws.osml.model_runner.database.ddb_helper.boto3"):
            table = TileRequestTable("test-tile-table")
            assert table.table_name == "test-tile-table"

    def test_start_tile_request(self):
        """Test starting a tile request"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable, TileRequestItem

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            mock_table = Mock()
            mock_boto3.resource.return_value.Table.return_value = mock_table

            table = TileRequestTable("test-tile-table")

            tile_item = TileRequestItem(
                tile_id="test-tile-123",
                region_id="test-region-789",
                job_id="test-job-456",
            )

            result = table.start_tile_request(tile_item)

            # Verify the item was updated with start parameters
            assert result.tile_status == RequestStatus.PENDING
            assert result.retry_count == 0
            assert result.start_time is not None
            assert result.expire_time is not None
            mock_table.put_item.assert_called_once()

    def test_get_tile_request(self):
        """Test getting a tile request from the table"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            mock_table = Mock()
            mock_table.get_item.return_value = {
                "Item": {
                    "tile_id": "test-tile-123",
                    "region_id": "test-region-789",
                    "tile_status": "SUCCESS",
                }
            }
            mock_boto3.resource.return_value.Table.return_value = mock_table

            table = TileRequestTable("test-tile-table")
            result = table.get_tile_request("test-tile-123", "test-region-789")

            assert result.tile_id == "test-tile-123"
            assert result.tile_status == "SUCCESS"

    def test_get_tile_request_not_found(self):
        """Test getting a non-existent tile request"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            mock_table = Mock()
            mock_table.get_item.return_value = {}
            mock_boto3.resource.return_value.Table.return_value = mock_table

            table = TileRequestTable("test-tile-table")
            result = table.get_tile_request("non-existent-tile", "non-existent-region")

            assert result is None

    def test_update_tile_status(self):
        """Test updating tile status"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable

        test_cases = [
            (RequestStatus.IN_PROGRESS, None), 
            (RequestStatus.SUCCESS, None),
            (RequestStatus.FAILED, "an error occurred")]
        for (status, error_message) in test_cases:
            with self.subTest(status=status, error_message=error_message):
                with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
                    mock_table = Mock()
                    mock_table.update_item.return_value = {
                        "Attributes": {
                            "tile_id": "test-tile-123",
                            "region_id": "test-region-789",
                            "tile_status": status,
                        }
                    }
                    mock_table.get_item.return_value = {"Item": {"tile_id": "test-tile-123", "start_time": 1000}}
                    mock_boto3.resource.return_value.Table.return_value = mock_table

                    table = TileRequestTable("test-tile-table")
                    result = table.update_tile_status("test-tile-123", "test-region-789", tile_status=status, error_message=error_message)

                    mock_table.update_item.assert_called_once()
                    assert result.tile_status == status

    def test_complete_tile_request(self):
        """Test completing a tile request"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable, TileRequestItem

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            with patch("aws.osml.model_runner.database.tile_request_table.S3_MANAGER"):
                mock_table = Mock()
                mock_table.update_item.return_value = {
                    "Attributes": {
                        "tile_id": "test-tile-123",
                        "region_id": "test-region-789",
                        "tile_status": RequestStatus.SUCCESS,
                        "end_time": 2000,
                    }
                }
                mock_boto3.resource.return_value.Table.return_value = mock_table

                table = TileRequestTable("test-tile-table")

                tile_item = TileRequestItem(
                    tile_id="test-tile-123",
                    region_id="test-region-789",
                    start_time=1000,
                    output_location="s3://bucket/output",
                )

                # Mock the cleanup_tile_artifacts method
                with patch.object(table, "cleanup_tile_artifacts") as mock_cleanup:
                    result = table.complete_tile_request(tile_item, RequestStatus.SUCCESS)

                    assert result.tile_status == RequestStatus.SUCCESS
                    assert result.end_time is not None
                    mock_table.update_item.assert_called_once()
                    # Verify cleanup was called
                    mock_cleanup.assert_called_once_with(tile_item)

    def test_get_tile_request_by_output_location(self):
        """Test getting a tile request by output location"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            mock_table = Mock()
            # GSI query returns only region_id and tile_id
            mock_table.query.return_value = {
                "Items": [{"region_id": "test-region-789", "tile_id": "test-tile-123"}]
            }
            # Full item retrieval
            mock_table.get_item.return_value = {
                "Item": {
                    "tile_id": "test-tile-123",
                    "region_id": "test-region-789",
                    "output_location": "s3://bucket/output",
                }
            }
            mock_boto3.resource.return_value.Table.return_value = mock_table

            table = TileRequestTable("test-tile-table")
            result = table.get_tile_request_by_output_location("s3://bucket/output")

            assert result is not None
            assert result.tile_id == "test-tile-123"
            mock_table.query.assert_called_once()

    def test_update_tile_inference_info(self):
        """Test updating tile inference information"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            mock_table = Mock()
            mock_table.update_item.return_value = {
                "Attributes": {
                    "tile_id": "test-tile-123",
                    "region_id": "test-region-789",
                    "inference_id": "inference-456",
                    "output_location": "s3://bucket/output",
                }
            }
            mock_boto3.resource.return_value.Table.return_value = mock_table

            table = TileRequestTable("test-tile-table")
            result = table.update_tile_inference_info(
                "test-tile-123", "test-region-789", "inference-456", "s3://bucket/output", "s3://bucket/failure"
            )

            assert result.inference_id == "inference-456"
            mock_table.update_item.assert_called_once()

    def test_get_tile_request_by_event_sagemaker_failed(self):
        """Test getting tile request from SageMaker failure event"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable, TileRequestItem

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            mock_table = Mock()
            mock_boto3.resource.return_value.Table.return_value = mock_table

            table = TileRequestTable("test-tile-table")

            # Mock get_tile_request_by_inference_id
            mock_tile_item = TileRequestItem(
                tile_id="test-tile-123",
                region_id="test-region-789",
                inference_id="test-inference-id",
            )

            with patch.object(table, "get_tile_request_by_inference_id", return_value=mock_tile_item):
                # SageMaker failure event
                event_message = {
                    "inferenceId": "test-inference-id",
                    "invocationStatus": "Failed",
                    "failureReason": "Model invocation failed",
                    "responseParameters": {"endpointName": "test-endpoint"},
                }

                result = table.get_tile_request_by_event(event_message)

                assert result is not None
                assert result.tile_id == "test-tile-123"
                assert result.tile_status == RequestStatus.FAILED
                assert result.error_message == "Model invocation failed"

    def test_get_tile_request_by_event_s3_direct(self):
        """Test getting tile request from direct S3 event"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable, TileRequestItem

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            mock_table = Mock()
            mock_boto3.resource.return_value.Table.return_value = mock_table

            table = TileRequestTable("test-tile-table")

            # Mock get_tile_request_by_output_location
            mock_tile_item = TileRequestItem(
                tile_id="test-tile-456",
                region_id="test-region-789",
                output_location="s3://test-bucket/output/result.out",
            )

            with patch.object(table, "get_tile_request_by_output_location", return_value=mock_tile_item):
                # Direct S3 event
                event_message = {
                    "Records": [
                        {
                            "s3": {
                                "bucket": {"name": "test-bucket"},
                                "object": {"key": "output/result.out"},
                            }
                        }
                    ]
                }

                result = table.get_tile_request_by_event(event_message)

                assert result is not None
                assert result.tile_id == "test-tile-456"
                assert result.output_location == "s3://test-bucket/output/result.out"

    def test_get_tile_request_by_event_poller(self):
        """Test getting tile request from poller event with failure"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable, TileRequestItem

        test_cases = [
            (RequestStatus.SUCCESS, "s3://bucket/output.out",),
            (RequestStatus.FAILED, "s3://bucket/failure.out",)
        ]

        for status, identified_s3_path in test_cases:
            with self.subTest(status=status, identified_s3_path=identified_s3_path):
                with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
                    with patch("aws.osml.model_runner.database.tile_request_table.S3_MANAGER") as mock_s3:
                        mock_table = Mock()
                        mock_boto3.resource.return_value.Table.return_value = mock_table

                        table = TileRequestTable("test-tile-table")

                        # Mock tile item with failure location
                        mock_tile_item = TileRequestItem(
                            tile_id="test-tile-888",
                            region_id="test-region-888",
                            tile_status=RequestStatus.IN_PROGRESS,
                            output_location="s3://bucket/output.out",
                            failure_location="s3://bucket/failure.out",
                        )

                        with patch.object(table, "get_tile_request", return_value=mock_tile_item):
                            # Mock S3 manager to indicate failure file exists
                            mock_s3.does_object_exist.side_effect = lambda uri: uri == identified_s3_path

                            # Poller event
                            event_message = {
                                "PollerInfo": {
                                    "tile_id": "test-tile-888",
                                    "region_id": "test-region-888",
                                }
                            }

                            result = table.get_tile_request_by_event(event_message)

                            assert result is not None
                            assert result.tile_id == "test-tile-888"
                            assert result.tile_status == status

    def test_get_tile_request_by_event_poller_already_complete(self):
        """Test getting tile request from poller event when already complete"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable, TileRequestItem

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            with patch("aws.osml.model_runner.database.tile_request_table.S3_MANAGER"):
                mock_table = Mock()
                mock_boto3.resource.return_value.Table.return_value = mock_table

                table = TileRequestTable("test-tile-table")

                # Mock tile item that's already complete
                mock_tile_item = TileRequestItem(
                    tile_id="test-tile-777",
                    region_id="test-region-777",
                    tile_status=RequestStatus.SUCCESS,
                )

                with patch.object(table, "get_tile_request", return_value=mock_tile_item):
                    # Poller event
                    event_message = {
                        "PollerInfo": {
                            "tile_id": "test-tile-777",
                            "region_id": "test-region-777",
                        }
                    }

                    result = table.get_tile_request_by_event(event_message)

                    assert result is not None
                    assert result.tile_id == "test-tile-777"
                    assert result.tile_status == RequestStatus.SUCCESS

    def test_get_tile_request_by_event_poller_no_files(self):
        """Test getting tile request from poller event when no files found"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable, TileRequestItem

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            with patch("aws.osml.model_runner.database.tile_request_table.S3_MANAGER") as mock_s3:
                mock_table = Mock()
                mock_boto3.resource.return_value.Table.return_value = mock_table

                table = TileRequestTable("test-tile-table")

                # Mock tile item
                mock_tile_item = TileRequestItem(
                    tile_id="test-tile-666",
                    region_id="test-region-666",
                    tile_status=RequestStatus.IN_PROGRESS,
                    output_location="s3://bucket/output.out",
                    failure_location="s3://bucket/failure.out",
                )

                with patch.object(table, "get_tile_request", return_value=mock_tile_item):
                    # Mock S3 manager to indicate no files exist
                    mock_s3.does_object_exist.return_value = False

                    # Poller event
                    event_message = {
                        "PollerInfo": {
                            "tile_id": "test-tile-666",
                            "region_id": "test-region-666",
                        }
                    }

                    result = table.get_tile_request_by_event(event_message)

                    # Should return None to indicate retry needed
                    assert result is None

    def test_get_tile_request_by_event_invalid_format(self):
        """Test getting tile request with invalid event format"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            mock_table = Mock()
            mock_boto3.resource.return_value.Table.return_value = mock_table

            table = TileRequestTable("test-tile-table")

            # Invalid event message
            event_message = {"invalid": "format"}

            result = table.get_tile_request_by_event(event_message)

            # Should return empty string for invalid format
            assert result == ""

    def test_get_tile_request_by_event_sagemaker_success_skipped(self):
        """Test that SageMaker success events are skipped"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable
        from aws.osml.model_runner.exceptions import SkipException

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            mock_table = Mock()
            mock_boto3.resource.return_value.Table.return_value = mock_table

            table = TileRequestTable("test-tile-table")

            # SageMaker success event (should be skipped)
            event_message = {
                "inferenceId": "test-inference-id",
                "invocationStatus": "Completed",
                "responseParameters": {"outputLocation": "s3://bucket/output.out"},
            }

            with pytest.raises(SkipException):
                table.get_tile_request_by_event(event_message)

    def test_start_tile_request_exception(self):
        """Test start_tile_request with exception"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable, TileRequestItem
        from aws.osml.model_runner.database.exceptions import StartRegionException

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            mock_table = Mock()
            mock_table.put_item.side_effect = Exception("DynamoDB error")
            mock_boto3.resource.return_value.Table.return_value = mock_table

            table = TileRequestTable("test-tile-table")

            tile_item = TileRequestItem(
                tile_id="test-tile-123",
                region_id="test-region-789",
            )

            with pytest.raises(StartRegionException):
                table.start_tile_request(tile_item)

    def test_update_tile_status_exception(self):
        """Test update_tile_status with exception"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable
        from aws.osml.model_runner.database.exceptions import UpdateRegionException

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            mock_table = Mock()
            mock_table.update_item.side_effect = Exception("Update failed")
            mock_boto3.resource.return_value.Table.return_value = mock_table

            table = TileRequestTable("test-tile-table")

            with pytest.raises(UpdateRegionException):
                table.update_tile_status("test-tile-123", "test-region-789", RequestStatus.SUCCESS)

    def test_complete_tile_request_exception(self):
        """Test complete_tile_request with exception"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable, TileRequestItem
        from aws.osml.model_runner.database.exceptions import CompleteRegionException

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            with patch("aws.osml.model_runner.database.tile_request_table.S3_MANAGER"):
                mock_table = Mock()
                mock_table.update_item.side_effect = Exception("Complete failed")
                mock_boto3.resource.return_value.Table.return_value = mock_table

                table = TileRequestTable("test-tile-table")

                tile_item = TileRequestItem(
                    tile_id="test-tile-123",
                    region_id="test-region-789",
                    start_time=1000,
                    output_location="s3://bucket/output",
                )

                with pytest.raises(CompleteRegionException):
                    table.complete_tile_request(tile_item, RequestStatus.SUCCESS)

    def test_get_tiles_for_region(self):
        """Test getting all tiles for a region"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            mock_table = Mock()
            mock_table.query.return_value = {
                "Items": [
                    {"tile_id": "tile-1", "region_id": "region-1", "tile_status": "SUCCESS"},
                    {"tile_id": "tile-2", "region_id": "region-1", "tile_status": "FAILED"},
                ]
            }
            mock_boto3.resource.return_value.Table.return_value = mock_table

            table = TileRequestTable("test-tile-table")
            result = table.get_tiles_for_region("region-1")

            assert len(result) == 2
            assert result[0].tile_id == "tile-1"
            assert result[1].tile_id == "tile-2"

    def test_get_tiles_for_region_with_status_filter(self):
        """Test getting tiles for a region with status filter"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            mock_table = Mock()
            mock_table.query.return_value = {
                "Items": [
                    {"tile_id": "tile-1", "region_id": "region-1", "tile_status": "SUCCESS"},
                ]
            }
            mock_boto3.resource.return_value.Table.return_value = mock_table

            table = TileRequestTable("test-tile-table")
            result = table.get_tiles_for_region("region-1", status_filter="SUCCESS")

            assert len(result) == 1
            assert result[0].tile_status == "SUCCESS"

    def test_get_tiles_for_region_exception(self):
        """Test get_tiles_for_region with exception"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            mock_table = Mock()
            mock_table.query.side_effect = Exception("Query failed")
            mock_boto3.resource.return_value.Table.return_value = mock_table

            table = TileRequestTable("test-tile-table")
            result = table.get_tiles_for_region("region-1")

            # Should return empty list on exception
            assert result == []

    def test_increment_retry_count(self):
        """Test incrementing retry count"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            mock_table = Mock()
            mock_table.update_item.return_value = {
                "Attributes": {
                    "tile_id": "test-tile-123",
                    "region_id": "test-region-789",
                    "retry_count": 1,
                }
            }
            mock_boto3.resource.return_value.Table.return_value = mock_table

            table = TileRequestTable("test-tile-table")
            result = table.increment_retry_count("test-tile-123", "test-region-789")

            assert result.retry_count == 1
            mock_table.update_item.assert_called_once()

    def test_increment_retry_count_exception(self):
        """Test increment_retry_count with exception"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable
        from aws.osml.model_runner.database.exceptions import UpdateRegionException

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            mock_table = Mock()
            mock_table.update_item.side_effect = Exception("Update failed")
            mock_boto3.resource.return_value.Table.return_value = mock_table

            table = TileRequestTable("test-tile-table")

            with pytest.raises(UpdateRegionException):
                table.increment_retry_count("test-tile-123", "test-region-789")

    def test_get_tile_request_by_inference_id(self):
        """Test getting tile request by inference ID"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            mock_table = Mock()
            # GSI query returns only region_id and tile_id
            mock_table.query.return_value = {
                "Items": [{"region_id": "test-region-789", "tile_id": "test-tile-123"}]
            }
            # Full item retrieval
            mock_table.get_item.return_value = {
                "Item": {
                    "tile_id": "test-tile-123",
                    "region_id": "test-region-789",
                    "inference_id": "inference-456",
                }
            }
            mock_boto3.resource.return_value.Table.return_value = mock_table

            table = TileRequestTable("test-tile-table")
            result = table.get_tile_request_by_inference_id("inference-456")

            assert result is not None
            assert result.tile_id == "test-tile-123"
            assert result.inference_id == "inference-456"

    def test_get_tile_request_by_inference_id_not_found(self):
        """Test getting tile request by inference ID when not found"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            mock_table = Mock()
            mock_table.query.return_value = {"Items": []}
            mock_boto3.resource.return_value.Table.return_value = mock_table

            table = TileRequestTable("test-tile-table")
            result = table.get_tile_request_by_inference_id("non-existent-inference")

            assert result is None

    def test_get_tile_request_by_inference_id_missing_keys(self):
        """Test getting tile request by inference ID with missing keys in GSI result"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            mock_table = Mock()
            # GSI query returns item without required keys
            mock_table.query.return_value = {"Items": [{"some_field": "value"}]}
            mock_boto3.resource.return_value.Table.return_value = mock_table

            table = TileRequestTable("test-tile-table")
            result = table.get_tile_request_by_inference_id("inference-456")

            assert result is None

    def test_get_tile_request_by_inference_id_exception(self):
        """Test getting tile request by inference ID with exception"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            mock_table = Mock()
            mock_table.query.side_effect = Exception("Query failed")
            mock_boto3.resource.return_value.Table.return_value = mock_table

            table = TileRequestTable("test-tile-table")
            result = table.get_tile_request_by_inference_id("inference-456")

            assert result is None

    def test_get_tile_request_by_output_location_not_found(self):
        """Test getting tile request by output location when not found"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            mock_table = Mock()
            mock_table.query.return_value = {"Items": []}
            mock_boto3.resource.return_value.Table.return_value = mock_table

            table = TileRequestTable("test-tile-table")
            result = table.get_tile_request_by_output_location("s3://bucket/non-existent")

            assert result is None

    def test_get_tile_request_by_output_location_missing_keys(self):
        """Test getting tile request by output location with missing keys"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            mock_table = Mock()
            # GSI query returns item without required keys
            mock_table.query.return_value = {"Items": [{"some_field": "value"}]}
            mock_boto3.resource.return_value.Table.return_value = mock_table

            table = TileRequestTable("test-tile-table")
            result = table.get_tile_request_by_output_location("s3://bucket/output")

            assert result is None

    def test_get_tile_request_by_output_location_exception(self):
        """Test getting tile request by output location with exception"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            mock_table = Mock()
            mock_table.query.side_effect = Exception("Query failed")
            mock_boto3.resource.return_value.Table.return_value = mock_table

            table = TileRequestTable("test-tile-table")
            result = table.get_tile_request_by_output_location("s3://bucket/output")

            assert result is None

    def test_update_tile_inference_info_exception(self):
        """Test update_tile_inference_info with exception"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable
        from aws.osml.model_runner.database.exceptions import UpdateRegionException

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            mock_table = Mock()
            mock_table.update_item.side_effect = Exception("Update failed")
            mock_boto3.resource.return_value.Table.return_value = mock_table

            table = TileRequestTable("test-tile-table")

            with pytest.raises(UpdateRegionException):
                table.update_tile_inference_info(
                    "test-tile-123", "test-region-789", "inference-456", "s3://bucket/output", "s3://bucket/failure"
                )

    def test_get_region_request_complete_counts(self):
        """Test getting complete counts for a region"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable, TileRequestItem

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            mock_table = Mock()
            mock_boto3.resource.return_value.Table.return_value = mock_table

            table = TileRequestTable("test-tile-table")

            # Mock get_tiles_for_region to return mixed status tiles
            mock_tiles = [
                TileRequestItem(tile_id="tile-1", region_id="region-1", tile_status=RequestStatus.SUCCESS),
                TileRequestItem(tile_id="tile-2", region_id="region-1", tile_status=RequestStatus.SUCCESS),
                TileRequestItem(tile_id="tile-3", region_id="region-1", tile_status=RequestStatus.FAILED),
                TileRequestItem(tile_id="tile-4", region_id="region-1", tile_status=RequestStatus.IN_PROGRESS),
            ]

            with patch.object(table, "get_tiles_for_region", return_value=mock_tiles):
                tile_item = TileRequestItem(tile_id="tile-1", region_id="region-1")
                failed_count, complete_count = table.get_region_request_complete_counts(tile_item)

                assert failed_count == 1
                assert complete_count == 2

    def test_get_region_request_complete_counts_exception(self):
        """Test get_region_request_complete_counts with exception"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable, TileRequestItem

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            mock_table = Mock()
            mock_boto3.resource.return_value.Table.return_value = mock_table

            table = TileRequestTable("test-tile-table")

            with patch.object(table, "get_tiles_for_region", side_effect=Exception("Query failed")):
                tile_item = TileRequestItem(tile_id="tile-1", region_id="region-1")
                result = table.get_region_request_complete_counts(tile_item)

                # Should return safe defaults (False, 0, 0, None, None)
                assert result == (False, 0, 0, None, None)

    def test_get_or_create_tile_request_item_existing(self):
        """Test get_or_create_tile_request_item with existing item"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable, TileRequestItem
        from aws.osml.model_runner.api import TileRequest

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            mock_table = Mock()
            mock_boto3.resource.return_value.Table.return_value = mock_table

            table = TileRequestTable("test-tile-table")

            # Mock existing tile item
            existing_item = TileRequestItem(
                tile_id="test-tile-123",
                region_id="test-region-789",
                tile_status=RequestStatus.IN_PROGRESS,
            )

            with patch.object(table, "get_tile_request", return_value=existing_item):
                tile_request = TileRequest(
                    tile_id="test-tile-123",
                    region_id="test-region-789",
                    job_id="test-job-456",
                    image_id="test-image-789",
                    image_url="s3://bucket/image.tif",
                    image_path="/tmp/image.tif",
                    tile_bounds=[[0, 0], [512, 512]],
                    model_name="test-model",
                    model_invocation_role="test-role",
                )

                result = table.get_or_create_tile_request_item(tile_request)

                assert result.tile_id == "test-tile-123"
                assert result.tile_status == RequestStatus.IN_PROGRESS

    def test_get_or_create_tile_request_item_new(self):
        """Test get_or_create_tile_request_item creating new item"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable
        from aws.osml.model_runner.api import TileRequest

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            mock_table = Mock()
            mock_boto3.resource.return_value.Table.return_value = mock_table

            table = TileRequestTable("test-tile-table")

            with patch.object(table, "get_tile_request", return_value=None):
                with patch.object(table, "start_tile_request") as mock_start:
                    tile_request = TileRequest(
                        tile_id="test-tile-new",
                        region_id="test-region-new",
                        job_id="test-job-456",
                        image_id="test-image-789",
                        image_url="s3://bucket/image.tif",
                        image_path="/tmp/image.tif",
                        tile_bounds=[[0, 0], [512, 512]],
                        model_name="test-model",
                        model_invocation_role="test-role",
                    )

                    table.get_or_create_tile_request_item(tile_request)

                    # Verify start_tile_request was called
                    mock_start.assert_called_once()

    def test_get_tile_request_by_event_s3_url_encoded(self):
        """Test getting tile request from S3 event with URL-encoded key"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable, TileRequestItem

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            mock_table = Mock()
            mock_boto3.resource.return_value.Table.return_value = mock_table

            table = TileRequestTable("test-tile-table")

            # Mock get_tile_request_by_output_location
            mock_tile_item = TileRequestItem(
                tile_id="test-tile-456",
                region_id="test-region-789",
                output_location="s3://test-bucket/output/result with spaces.out",
            )

            with patch.object(table, "get_tile_request_by_output_location", return_value=mock_tile_item):
                # S3 event with URL-encoded key
                event_message = {
                    "Records": [
                        {
                            "s3": {
                                "bucket": {"name": "test-bucket"},
                                "object": {"key": "output/result+with+spaces.out"},
                            }
                        }
                    ]
                }

                result = table.get_tile_request_by_event(event_message)

                assert result is not None
                assert result.tile_id == "test-tile-456"

    def test_get_tile_request_by_event_exception(self):
        """Test get_tile_request_by_event with exception during processing"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            mock_table = Mock()
            mock_boto3.resource.return_value.Table.return_value = mock_table

            table = TileRequestTable("test-tile-table")

            with patch.object(table, "get_tile_request_by_output_location", side_effect=Exception("Lookup failed")):
                # S3 event
                event_message = {
                    "Records": [
                        {
                            "s3": {
                                "bucket": {"name": "test-bucket"},
                                "object": {"key": "output/result.out"},
                            }
                        }
                    ]
                }

                result = table.get_tile_request_by_event(event_message)

                # Should return empty string on exception
                assert result == ""

    def test_tile_request_item_from_tile_request(self):
        """Test TileRequestItem.from_tile_request factory method"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestItem
        from aws.osml.model_runner.api import TileRequest

        tile_request = TileRequest(
            tile_id="test-tile-123",
            region_id="test-region-789",
            job_id="test-job-456",
            image_id="test-image-789",
            image_url="s3://bucket/image.tif",
            image_path="/tmp/image.tif",
            tile_bounds=[[0, 0], [512, 512]],
            inference_id="inference-123",
            output_location="s3://bucket/output.json",
            failure_location="s3://bucket/failure.json",
            model_name="test-model",
            model_invocation_role="test-role",
            tile_size=[512, 512],
            tile_overlap=[64, 64],
            model_invoke_mode="ASYNC",
            image_read_role="read-role",
        )

        result = TileRequestItem.from_tile_request(tile_request)

        assert result.tile_id == "test-tile-123"
        assert result.region_id == "test-region-789"
        assert result.job_id == "test-job-456"
        assert result.inference_id == "inference-123"
        assert result.output_location == "s3://bucket/output.json"
        assert result.model_name == "test-model"
        assert result.tile_size == [512, 512]
        assert result.tile_overlap == [64, 64]

    def test_tile_request_item_post_init(self):
        """Test TileRequestItem __post_init__ method"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestItem

        tile_item = TileRequestItem(
            tile_id="test-tile-123",
            region_id="test-region-789",
            tile_bounds=[[0, 0], [512, 512]],
        )

        # Verify tile_bounds is converted to tuple of tuples
        assert isinstance(tile_item.tile_bounds, tuple)
        assert isinstance(tile_item.tile_bounds[0], tuple)
        assert tile_item.region == tile_item.tile_bounds

        # Verify ddb_key is set correctly
        assert tile_item.ddb_key.hash_key == "region_id"
        assert tile_item.ddb_key.hash_value == "test-region-789"
        assert tile_item.ddb_key.range_key == "tile_id"
        assert tile_item.ddb_key.range_value == "test-tile-123"

    def test_complete_tile_request_with_error_message(self):
        """Test complete_tile_request with error message"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable, TileRequestItem

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            with patch("aws.osml.model_runner.database.tile_request_table.S3_MANAGER"):
                mock_table = Mock()
                mock_table.update_item.return_value = {
                    "Attributes": {
                        "tile_id": "test-tile-123",
                        "region_id": "test-region-789",
                        "tile_status": RequestStatus.FAILED,
                        "error_message": "Processing failed",
                    }
                }
                mock_boto3.resource.return_value.Table.return_value = mock_table

                table = TileRequestTable("test-tile-table")

                tile_item = TileRequestItem(
                    tile_id="test-tile-123",
                    region_id="test-region-789",
                    start_time=1000,
                    output_location="s3://bucket/output",
                )

                with patch.object(table, "cleanup_tile_artifacts"):
                    result = table.complete_tile_request(tile_item, RequestStatus.FAILED, "Processing failed")

                    assert result.tile_status == RequestStatus.FAILED
                    assert result.error_message == "Processing failed"

    def test_update_tile_status_with_processing_duration(self):
        """Test update_tile_status calculates processing duration correctly"""
        from aws.osml.model_runner.database.tile_request_table import TileRequestTable, TileRequestItem

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            mock_table = Mock()
            # Mock existing item with start_time
            mock_table.get_item.return_value = {
                "Item": {
                    "tile_id": "test-tile-123",
                    "region_id": "test-region-789",
                    "start_time": 1000,
                }
            }
            mock_table.update_item.return_value = {
                "Attributes": {
                    "tile_id": "test-tile-123",
                    "region_id": "test-region-789",
                    "tile_status": RequestStatus.SUCCESS,
                    "processing_duration": 5000,
                }
            }
            mock_boto3.resource.return_value.Table.return_value = mock_table

            table = TileRequestTable("test-tile-table")
            result = table.update_tile_status("test-tile-123", "test-region-789", RequestStatus.SUCCESS)

            # Verify processing_duration was calculated
            assert result.processing_duration == 5000


if __name__ == "__main__":
    unittest.main()
