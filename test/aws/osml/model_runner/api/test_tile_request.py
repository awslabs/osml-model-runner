#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import unittest
from unittest import TestCase
from unittest.mock import Mock

import pytest


class TestTileRequest(TestCase):
    """Unit tests for TileRequest API class"""

    def setUp(self):
        """Set up sample data for each test"""
        self.valid_tile_data = {
            "tile_id": "test-tile-123",
            "region_id": "test-region-789",
            "image_id": "test-image-456",
            "job_id": "test-job-abc",
            "image_path": "/tmp/tile_0_0.tif",
            "image_url": "s3://bucket/image.tif",
            "tile_bounds": [[0, 0], [1024, 1024]],
        }

    def test_tile_request_creation(self):
        """Test basic TileRequest creation with required fields"""
        from aws.osml.model_runner.api.tile_request import TileRequest

        tile_request = TileRequest(
            tile_id="test-tile-123",
            region_id="test-region-789",
            image_id="test-image-456",
            job_id="test-job-abc",
            image_path="/tmp/tile.tif",
            image_url="s3://bucket/image.tif",
            tile_bounds=[[0, 0], [1024, 1024]],
        )

        assert tile_request.tile_id == "test-tile-123"
        assert tile_request.region_id == "test-region-789"
        assert tile_request.image_id == "test-image-456"
        assert tile_request.job_id == "test-job-abc"
        assert tile_request.image_path == "/tmp/tile.tif"
        assert tile_request.image_url == "s3://bucket/image.tif"
        assert tile_request.tile_bounds == [[0, 0], [1024, 1024]]

    def test_is_valid_with_valid_data(self):
        """Test is_valid() returns True for valid tile request"""
        from aws.osml.model_runner.api.tile_request import TileRequest

        tile_request = TileRequest(**self.valid_tile_data)
        assert tile_request.is_valid() is True

    def test_is_valid_with_missing_tile_id(self):
        """Test is_valid() returns False when tile_id is missing"""
        from aws.osml.model_runner.api.tile_request import TileRequest

        data = self.valid_tile_data.copy()
        data["tile_id"] = ""

        tile_request = TileRequest(**data)
        assert tile_request.is_valid() is False

    def test_is_valid_with_invalid_tile_bounds_length(self):
        """Test is_valid() returns False when tile_bounds doesn't have 2 elements"""
        from aws.osml.model_runner.api.tile_request import TileRequest

        data = self.valid_tile_data.copy()
        data["tile_bounds"] = [[0, 0]]  # Only 1 coordinate pair

        tile_request = TileRequest(**data)
        assert tile_request.is_valid() is False

    def test_from_tile_request_dict(self):
        """Test creating TileRequest from dictionary"""
        from aws.osml.model_runner.api.tile_request import TileRequest

        tile_dict = {
            "tile_id": "test-tile-123",
            "region_id": "test-region-789",
            "image_id": "test-image-456",
            "job_id": "test-job-abc",
            "image_path": "/tmp/tile.tif",
            "image_url": "s3://bucket/image.tif",
            "tile_bounds": [[0, 0], [1024, 1024]],
            "inference_id": "inference-123",
            "output_location": "s3://bucket/output/",
            "model_name": "test-model",
        }

        tile_request = TileRequest.from_tile_request_dict(tile_dict)

        assert tile_request.tile_id == "test-tile-123"
        assert tile_request.region_id == "test-region-789"
        assert tile_request.image_id == "test-image-456"
        assert tile_request.job_id == "test-job-abc"
        assert tile_request.inference_id == "inference-123"
        assert tile_request.output_location == "s3://bucket/output/"
        assert tile_request.model_name == "test-model"

    def test_from_tile_request_item(self):
        """Test creating TileRequest from TileRequestItem"""
        from aws.osml.model_runner.api.tile_request import TileRequest

        # Mock TileRequestItem
        mock_item = Mock()
        mock_item.tile_id = "test-tile-123"
        mock_item.region_id = "test-region-789"
        mock_item.image_id = "test-image-456"
        mock_item.job_id = "test-job-abc"
        mock_item.image_path = "/tmp/tile.tif"
        mock_item.image_url = "s3://bucket/image.tif"
        mock_item.tile_bounds = [[0, 0], [1024, 1024]]
        mock_item.inference_id = "inference-123"
        mock_item.output_location = "s3://bucket/output/"
        mock_item.failure_location = ""
        mock_item.model_invocation_role = "arn:aws:iam::123456789:role/test"
        mock_item.tile_size = [1024, 1024]
        mock_item.tile_overlap = [50, 50]
        mock_item.model_invoke_mode = "SM_ENDPOINT"
        mock_item.model_name = "test-model"

        tile_request = TileRequest.from_tile_request_item(mock_item)

        assert tile_request.tile_id == "test-tile-123"
        assert tile_request.region_id == "test-region-789"
        assert tile_request.image_id == "test-image-456"
        assert tile_request.job_id == "test-job-abc"
        assert tile_request.inference_id == "inference-123"
        assert tile_request.model_name == "test-model"
        assert tile_request.tile_size == [1024, 1024]
        assert tile_request.tile_overlap == [50, 50]


if __name__ == "__main__":
    unittest.main()
