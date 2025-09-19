#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import os
import tempfile
import threading
import time
import unittest
from threading import Thread
from unittest.mock import MagicMock, Mock, patch

import boto3
import pytest
from botocore.exceptions import ClientError

try:
    from moto import mock_s3
except ImportError:
    # Mock the decorator if moto is not available
    def mock_s3(func):
        return func


from ..src.osml_extensions.config import AsyncEndpointConfig
from ..src.osml_extensions.utils import CleanupPolicy, ManagedResource, ResourceManager, ResourceType


class TestManagedResource(unittest.TestCase):
    """Test cases for ManagedResource class."""

    def test_managed_resource_initialization(self):
        """Test ManagedResource initialization."""
        resource_data = {"test_key": "test_value"}
        resource = ManagedResource(
            resource_id="test_resource", resource_type=ResourceType.S3_OBJECT, resource_data=resource_data
        )

        self.assertEqual(resource.resource_id, "test_resource")
        self.assertEqual(resource.resource_type, ResourceType.S3_OBJECT)
        self.assertEqual(resource.resource_data, resource_data)
        self.assertFalse(resource.cleanup_attempted)
        self.assertFalse(resource.cleanup_successful)
        self.assertIsNone(resource.cleanup_error)
        self.assertIsNone(resource.cleanup_time)
        self.assertIsNotNone(resource.created_time)

    def test_managed_resource_with_custom_time(self):
        """Test ManagedResource with custom creation time."""
        custom_time = 1234567890.0
        resource = ManagedResource(
            resource_id="test_resource", resource_type=ResourceType.TEMP_FILE, resource_data={}, created_time=custom_time
        )

        self.assertEqual(resource.created_time, custom_time)


class TestResourceManager(unittest.TestCase):
    """Test cases for ResourceManager class."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = AsyncEndpointConfig(
            input_bucket="test-input-bucket",
            output_bucket="test-output-bucket",
            cleanup_enabled=True,
            cleanup_policy="immediate",
        )
        self.mock_s3_client = Mock()
        self.resource_manager = ResourceManager(self.config, self.mock_s3_client)

    def tearDown(self):
        """Clean up after tests."""
        if hasattr(self, "resource_manager"):
            self.resource_manager.stop_cleanup_worker()

    def test_resource_manager_initialization(self):
        """Test ResourceManager initialization."""
        self.assertEqual(self.resource_manager.config, self.config)
        self.assertEqual(self.resource_manager.s3_client, self.mock_s3_client)
        self.assertIsInstance(self.resource_manager.managed_resources, dict)
        self.assertIsInstance(self.resource_manager.cleanup_policies, dict)
        self.assertEqual(len(self.resource_manager.cleanup_policies), 4)

    def test_cleanup_worker_lifecycle(self):
        """Test cleanup worker start and stop."""
        # Worker should not be running initially
        self.assertIsNone(self.resource_manager.cleanup_worker)

        # Start worker
        self.resource_manager.start_cleanup_worker()
        self.assertIsNotNone(self.resource_manager.cleanup_worker)
        self.assertTrue(self.resource_manager.cleanup_worker.is_alive())
        self.assertTrue(self.resource_manager.cleanup_worker_running)

        # Stop worker
        self.resource_manager.stop_cleanup_worker()
        self.assertFalse(self.resource_manager.cleanup_worker_running)

    def test_register_s3_object(self):
        """Test S3 object registration."""
        s3_uri = "s3://test-bucket/test-key"

        resource_id = self.resource_manager.register_s3_object(s3_uri)

        self.assertIn(resource_id, self.resource_manager.managed_resources)
        resource = self.resource_manager.managed_resources[resource_id]
        self.assertEqual(resource.resource_type, ResourceType.S3_OBJECT)
        self.assertEqual(resource.resource_data["s3_uri"], s3_uri)
        self.assertEqual(resource.resource_data["bucket"], "test-bucket")
        self.assertEqual(resource.resource_data["key"], "test-key")

    def test_register_temp_file(self):
        """Test temporary file registration."""
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name

        try:
            resource_id = self.resource_manager.register_temp_file(temp_path)

            self.assertIn(resource_id, self.resource_manager.managed_resources)
            resource = self.resource_manager.managed_resources[resource_id]
            self.assertEqual(resource.resource_type, ResourceType.TEMP_FILE)
            self.assertEqual(resource.resource_data["file_path"], temp_path)
        finally:
            # Clean up temp file
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def test_register_inference_job(self):
        """Test inference job registration."""
        inference_id = "test-inference-123"
        job_data = {
            "input_s3_uri": "s3://input-bucket/input-key",
            "output_s3_uri": "s3://output-bucket/output-key",
            "temp_files": ["/tmp/test-file"],
        }

        resource_id = self.resource_manager.register_inference_job(inference_id, job_data)

        self.assertIn(resource_id, self.resource_manager.managed_resources)
        resource = self.resource_manager.managed_resources[resource_id]
        self.assertEqual(resource.resource_type, ResourceType.INFERENCE_JOB)
        self.assertEqual(resource.resource_data["inference_id"], inference_id)
        self.assertEqual(resource.resource_data["input_s3_uri"], job_data["input_s3_uri"])

    def test_register_worker_thread(self):
        """Test worker thread registration."""
        mock_thread = Mock(spec=Thread)
        mock_thread.name = "TestWorker"

        resource_id = self.resource_manager.register_worker_thread(mock_thread)

        self.assertIn(resource_id, self.resource_manager.managed_resources)
        resource = self.resource_manager.managed_resources[resource_id]
        self.assertEqual(resource.resource_type, ResourceType.WORKER_THREAD)
        self.assertEqual(resource.resource_data["thread"], mock_thread)
        self.assertEqual(resource.resource_data["thread_name"], "TestWorker")

    @mock_s3
    def test_cleanup_s3_object_success(self):
        """Test successful S3 object cleanup."""
        # Create S3 bucket and object
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket="test-bucket")
        s3_client.put_object(Bucket="test-bucket", Key="test-key", Body=b"test data")

        # Create resource manager with real S3 client
        resource_manager = ResourceManager(self.config, s3_client)

        try:
            # Register S3 object
            s3_uri = "s3://test-bucket/test-key"
            resource_id = resource_manager.register_s3_object(s3_uri, CleanupPolicy.DISABLED)

            # Manually trigger cleanup
            success = resource_manager.cleanup_resource(resource_id, force=True)

            self.assertTrue(success)

            # Verify object was deleted
            with self.assertRaises(ClientError):
                s3_client.get_object(Bucket="test-bucket", Key="test-key")
        finally:
            resource_manager.stop_cleanup_worker()

    def test_cleanup_temp_file_success(self):
        """Test successful temporary file cleanup."""
        # Create temporary file
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name
            temp_file.write(b"test data")

        try:
            # Register temp file
            resource_id = self.resource_manager.register_temp_file(temp_path, CleanupPolicy.DISABLED)

            # Verify file exists
            self.assertTrue(os.path.exists(temp_path))

            # Manually trigger cleanup
            success = self.resource_manager.cleanup_resource(resource_id, force=True)

            self.assertTrue(success)
            self.assertFalse(os.path.exists(temp_path))
        finally:
            # Clean up if cleanup failed
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    @mock_s3
    def test_cleanup_inference_job_success(self):
        """Test successful inference job cleanup."""
        # Create S3 bucket and objects
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket="test-bucket")
        s3_client.put_object(Bucket="test-bucket", Key="input-key", Body=b"input data")
        s3_client.put_object(Bucket="test-bucket", Key="output-key", Body=b"output data")

        # Create temporary file
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name
            temp_file.write(b"temp data")

        # Create resource manager with real S3 client
        resource_manager = ResourceManager(self.config, s3_client)

        try:
            # Register inference job
            inference_id = "test-inference-123"
            job_data = {
                "input_s3_uri": "s3://test-bucket/input-key",
                "output_s3_uri": "s3://test-bucket/output-key",
                "temp_files": [temp_path],
            }
            resource_id = resource_manager.register_inference_job(inference_id, job_data, CleanupPolicy.DISABLED)

            # Manually trigger cleanup
            success = resource_manager.cleanup_resource(resource_id, force=True)

            self.assertTrue(success)

            # Verify S3 objects were deleted
            with self.assertRaises(ClientError):
                s3_client.get_object(Bucket="test-bucket", Key="input-key")
            with self.assertRaises(ClientError):
                s3_client.get_object(Bucket="test-bucket", Key="output-key")

            # Verify temp file was deleted
            self.assertFalse(os.path.exists(temp_path))
        finally:
            resource_manager.stop_cleanup_worker()
            # Clean up if cleanup failed
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def test_cleanup_worker_thread_success(self):
        """Test successful worker thread cleanup."""
        # Create a mock thread that can be stopped
        mock_thread = Mock(spec=Thread)
        mock_thread.name = "TestWorker"
        mock_thread.is_alive.return_value = True
        mock_thread.stop = Mock()
        mock_thread.join = Mock()

        # After join is called, thread should not be alive
        def mock_join(timeout=None):
            mock_thread.is_alive.return_value = False

        mock_thread.join.side_effect = mock_join

        # Register worker thread
        resource_id = self.resource_manager.register_worker_thread(mock_thread, CleanupPolicy.DISABLED)

        # Manually trigger cleanup
        success = self.resource_manager.cleanup_resource(resource_id, force=True)

        self.assertTrue(success)
        mock_thread.stop.assert_called_once()
        mock_thread.join.assert_called_once_with(timeout=10.0)

    def test_cleanup_failed_job_resources(self):
        """Test cleanup of failed job resources."""
        inference_id = "failed-inference-123"
        job_data = {
            "input_s3_uri": "s3://test-bucket/input-key",
            "output_s3_uri": "s3://test-bucket/output-key",
            "temp_files": [],
        }

        # Register inference job
        self.resource_manager.register_inference_job(inference_id, job_data, CleanupPolicy.DISABLED)

        # Mock successful cleanup
        with patch.object(self.resource_manager, "_perform_cleanup", return_value=True) as mock_cleanup:
            success = self.resource_manager.cleanup_failed_job_resources(inference_id)

            self.assertTrue(success)
            mock_cleanup.assert_called()

    def test_get_resource_stats(self):
        """Test resource statistics collection."""
        # Register some resources
        self.resource_manager.register_s3_object("s3://test-bucket/key1")
        self.resource_manager.register_s3_object("s3://test-bucket/key2")
        self.resource_manager.register_temp_file("/tmp/test-file")

        stats = self.resource_manager.get_resource_stats()

        self.assertEqual(stats["total_resources"], 3)
        self.assertIn("s3_object", stats["by_type"])
        self.assertIn("temp_file", stats["by_type"])
        self.assertEqual(stats["by_type"]["s3_object"]["total"], 2)
        self.assertEqual(stats["by_type"]["temp_file"]["total"], 1)

    def test_cleanup_all_resources(self):
        """Test cleanup of all resources."""
        # Register some resources
        self.resource_manager.register_s3_object("s3://test-bucket/key1", CleanupPolicy.DISABLED)
        self.resource_manager.register_temp_file("/tmp/test-file", CleanupPolicy.DISABLED)

        # Mock successful cleanup
        with patch.object(self.resource_manager, "_perform_cleanup", return_value=True) as mock_cleanup:
            count = self.resource_manager.cleanup_all_resources(force=True)

            self.assertEqual(count, 2)
            self.assertEqual(mock_cleanup.call_count, 2)

    def test_cleanup_all_resources_by_type(self):
        """Test cleanup of resources by type."""
        # Register resources of different types
        self.resource_manager.register_s3_object("s3://test-bucket/key1", CleanupPolicy.DISABLED)
        self.resource_manager.register_s3_object("s3://test-bucket/key2", CleanupPolicy.DISABLED)
        self.resource_manager.register_temp_file("/tmp/test-file", CleanupPolicy.DISABLED)

        # Mock successful cleanup
        with patch.object(self.resource_manager, "_perform_cleanup", return_value=True) as mock_cleanup:
            count = self.resource_manager.cleanup_all_resources(ResourceType.S3_OBJECT, force=True)

            self.assertEqual(count, 2)  # Only S3 objects should be cleaned up
            self.assertEqual(mock_cleanup.call_count, 2)

    def test_context_manager(self):
        """Test ResourceManager as context manager."""
        config = AsyncEndpointConfig(
            input_bucket="test-input-bucket", output_bucket="test-output-bucket", cleanup_enabled=True
        )

        with patch.object(ResourceManager, "cleanup_all_resources") as mock_cleanup:
            with ResourceManager(config) as rm:
                self.assertIsInstance(rm, ResourceManager)
                self.assertTrue(rm.cleanup_worker_running)

            mock_cleanup.assert_called_once_with(force=True)

    def test_cleanup_policy_disabled(self):
        """Test that disabled cleanup policy prevents cleanup."""
        s3_uri = "s3://test-bucket/test-key"
        resource_id = self.resource_manager.register_s3_object(s3_uri, CleanupPolicy.DISABLED)

        # Cleanup should not be performed without force
        success = self.resource_manager.cleanup_resource(resource_id, force=False)

        resource = self.resource_manager.managed_resources[resource_id]
        self.assertFalse(resource.cleanup_attempted)

    def test_cleanup_policy_immediate(self):
        """Test immediate cleanup policy."""
        # Stop the cleanup worker to prevent automatic cleanup
        self.resource_manager.stop_cleanup_worker()

        with patch.object(self.resource_manager, "_perform_cleanup", return_value=True) as mock_cleanup:
            s3_uri = "s3://test-bucket/test-key"
            self.resource_manager.register_s3_object(s3_uri, CleanupPolicy.IMMEDIATE)

            # Give some time for cleanup to be scheduled
            time.sleep(0.1)

            # Cleanup should have been scheduled
            self.assertFalse(self.resource_manager.cleanup_queue.empty())

    def test_cleanup_policy_delayed(self):
        """Test delayed cleanup policy."""
        # Stop the cleanup worker to prevent automatic cleanup
        self.resource_manager.stop_cleanup_worker()

        s3_uri = "s3://test-bucket/test-key"
        self.resource_manager.register_s3_object(s3_uri, CleanupPolicy.DELAYED)

        # Give some time for cleanup to be scheduled
        time.sleep(0.1)

        # Cleanup should have been scheduled with delay
        self.assertFalse(self.resource_manager.cleanup_queue.empty())

        # Get the scheduled item
        cleanup_item = self.resource_manager.cleanup_queue.get_nowait()
        self.assertIsInstance(cleanup_item, dict)
        self.assertIn("cleanup_time", cleanup_item)
        self.assertIn("resource", cleanup_item)

    def test_error_handling_in_cleanup(self):
        """Test error handling during cleanup operations."""
        # Mock S3 client to raise an exception
        self.mock_s3_client.delete_object.side_effect = ClientError(
            error_response={"Error": {"Code": "AccessDenied", "Message": "Access denied"}}, operation_name="DeleteObject"
        )

        s3_uri = "s3://test-bucket/test-key"
        resource_id = self.resource_manager.register_s3_object(s3_uri, CleanupPolicy.DISABLED)

        # Cleanup should fail gracefully
        success = self.resource_manager.cleanup_resource(resource_id, force=True)

        self.assertFalse(success)
        resource = self.resource_manager.managed_resources[resource_id]
        self.assertTrue(resource.cleanup_attempted)
        self.assertFalse(resource.cleanup_successful)
        self.assertIsNotNone(resource.cleanup_error)


class TestCleanupPolicies(unittest.TestCase):
    """Test cases for cleanup policies."""

    def test_cleanup_policy_enum_values(self):
        """Test CleanupPolicy enum values."""
        self.assertEqual(CleanupPolicy.IMMEDIATE.value, "immediate")
        self.assertEqual(CleanupPolicy.DELAYED.value, "delayed")
        self.assertEqual(CleanupPolicy.DISABLED.value, "disabled")

    def test_resource_type_enum_values(self):
        """Test ResourceType enum values."""
        self.assertEqual(ResourceType.S3_OBJECT.value, "s3_object")
        self.assertEqual(ResourceType.TEMP_FILE.value, "temp_file")
        self.assertEqual(ResourceType.INFERENCE_JOB.value, "inference_job")
        self.assertEqual(ResourceType.WORKER_THREAD.value, "worker_thread")


if __name__ == "__main__":
    unittest.main()
