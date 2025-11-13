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

        with patch("aws.osml.model_runner.database.ddb_helper.boto3") as mock_boto3:
            mock_table = Mock()
            mock_table.update_item.return_value = {
                "Attributes": {
                    "tile_id": "test-tile-123",
                    "region_id": "test-region-789",
                    "tile_status": RequestStatus.IN_PROGRESS,
                }
            }
            mock_table.get_item.return_value = {"Item": {"tile_id": "test-tile-123", "start_time": 1000}}
            mock_boto3.resource.return_value.Table.return_value = mock_table

            table = TileRequestTable("test-tile-table")
            result = table.update_tile_status("test-tile-123", "test-region-789", RequestStatus.IN_PROGRESS)

            mock_table.update_item.assert_called_once()
            assert result.tile_status == RequestStatus.IN_PROGRESS

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


if __name__ == "__main__":
    unittest.main()
