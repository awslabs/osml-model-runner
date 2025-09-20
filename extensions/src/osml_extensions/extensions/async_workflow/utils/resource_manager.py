#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
import os
import threading
import time
from enum import Enum
from queue import Empty, Queue
from threading import Event, Thread
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

from aws.osml.model_runner.app_config import BotoConfig

from ..config import AsyncEndpointConfig

logger = logging.getLogger(__name__)


class CleanupPolicy(Enum):
    """Cleanup policy options for resource management."""

    IMMEDIATE = "immediate"  # Clean up resources immediately after use
    DELAYED = "delayed"  # Clean up resources after a delay
    DISABLED = "disabled"  # Disable cleanup (resources remain)


class ResourceType(Enum):
    """Types of resources that can be managed."""

    S3_OBJECT = "s3_object"
    TEMP_FILE = "temp_file"
    INFERENCE_JOB = "inference_job"
    WORKER_THREAD = "worker_thread"


class ManagedResource:
    """
    Represents a managed resource with cleanup metadata.
    """

    def __init__(
        self,
        resource_id: str,
        resource_type: ResourceType,
        resource_data: Dict[str, Any],
        created_time: Optional[float] = None,
    ):
        """
        Initialize ManagedResource.

        :param resource_id: Unique identifier for the resource
        :param resource_type: Type of resource
        :param resource_data: Resource-specific data needed for cleanup
        :param created_time: When the resource was created (defaults to now)
        """
        self.resource_id = resource_id
        self.resource_type = resource_type
        self.resource_data = resource_data
        self.created_time = created_time or time.time()
        self.cleanup_attempted = False
        self.cleanup_successful = False
        self.cleanup_error: Optional[str] = None
        self.cleanup_time: Optional[float] = None


class ResourceManager:
    """
    Comprehensive resource manager for async endpoint operations.

    This class handles cleanup of S3 objects, temporary files, inference jobs,
    and worker threads with configurable policies and graceful error handling.
    """

    def __init__(self, config: AsyncEndpointConfig):
        """
        Initialize ResourceManager.

        :param config: AsyncEndpointConfig with cleanup settings
        :param s3_client: Optional S3 client (will create if not provided)
        """
        self.config = config
        self.s3_client = boto3.client("s3", config=BotoConfig.default)

        # Resource tracking
        self.managed_resources: Dict[str, ManagedResource] = {}
        self.cleanup_queue = Queue()
        self.resource_lock = threading.RLock()

        # Cleanup worker
        self.cleanup_worker: Optional[Thread] = None
        self.cleanup_worker_running = False
        self.shutdown_event = Event()

        # Cleanup policies
        self.cleanup_policies: Dict[ResourceType, CleanupPolicy] = {
            ResourceType.S3_OBJECT: CleanupPolicy(config.cleanup_enabled and "immediate" or "disabled"),
            ResourceType.TEMP_FILE: CleanupPolicy.IMMEDIATE,
            ResourceType.INFERENCE_JOB: CleanupPolicy(config.cleanup_enabled and "immediate" or "disabled"),
            ResourceType.WORKER_THREAD: CleanupPolicy.IMMEDIATE,
        }

        # Delayed cleanup settings
        self.cleanup_delay_seconds = getattr(config, "cleanup_delay_seconds", 300)  # 5 minutes default

        logger.debug(f"ResourceManager initialized with cleanup_enabled={config.cleanup_enabled}")

    def start_cleanup_worker(self) -> None:
        """Start the background cleanup worker thread."""
        if self.cleanup_worker is None or not self.cleanup_worker.is_alive():
            self.cleanup_worker_running = True
            self.shutdown_event.clear()
            self.cleanup_worker = Thread(target=self._cleanup_worker_loop, name="ResourceCleanupWorker")
            self.cleanup_worker.daemon = True
            self.cleanup_worker.start()
            logger.debug("Resource cleanup worker started")

    def stop_cleanup_worker(self, timeout: float = 10.0) -> None:
        """
        Stop the background cleanup worker thread.

        :param timeout: Maximum time to wait for worker to stop
        """
        if self.cleanup_worker and self.cleanup_worker.is_alive():
            logger.debug("Stopping resource cleanup worker")
            self.cleanup_worker_running = False
            self.shutdown_event.set()

            self.cleanup_worker.join(timeout=timeout)
            if self.cleanup_worker.is_alive():
                logger.warning("Resource cleanup worker did not stop gracefully")
            else:
                logger.debug("Resource cleanup worker stopped")

    def register_s3_object(self, s3_uri: str, cleanup_policy: Optional[CleanupPolicy] = None) -> str:
        """
        Register an S3 object for managed cleanup.

        :param s3_uri: S3 URI of the object
        :param cleanup_policy: Optional override for cleanup policy
        :return: Resource ID for tracking
        """
        resource_id = f"s3_{hash(s3_uri)}"

        resource_data = {"s3_uri": s3_uri, "bucket": urlparse(s3_uri).netloc, "key": urlparse(s3_uri).path.lstrip("/")}

        resource = ManagedResource(
            resource_id=resource_id, resource_type=ResourceType.S3_OBJECT, resource_data=resource_data
        )

        with self.resource_lock:
            self.managed_resources[resource_id] = resource

        # Schedule cleanup based on policy
        policy = cleanup_policy or self.cleanup_policies[ResourceType.S3_OBJECT]
        self._schedule_cleanup(resource, policy)

        logger.debug(f"Registered S3 object for cleanup: {s3_uri} (policy: {policy.value})")
        return resource_id

    def register_temp_file(self, file_path: str, cleanup_policy: Optional[CleanupPolicy] = None) -> str:
        """
        Register a temporary file for managed cleanup.

        :param file_path: Path to the temporary file
        :param cleanup_policy: Optional override for cleanup policy
        :return: Resource ID for tracking
        """
        resource_id = f"temp_{hash(file_path)}"

        resource_data = {"file_path": file_path}

        resource = ManagedResource(
            resource_id=resource_id, resource_type=ResourceType.TEMP_FILE, resource_data=resource_data
        )

        with self.resource_lock:
            self.managed_resources[resource_id] = resource

        # Schedule cleanup based on policy
        policy = cleanup_policy or self.cleanup_policies[ResourceType.TEMP_FILE]
        self._schedule_cleanup(resource, policy)

        logger.debug(f"Registered temp file for cleanup: {file_path} (policy: {policy.value})")
        return resource_id

    def register_inference_job(
        self, inference_id: str, job_data: Dict[str, Any], cleanup_policy: Optional[CleanupPolicy] = None
    ) -> str:
        """
        Register an inference job for managed cleanup.

        :param inference_id: SageMaker inference job ID
        :param job_data: Job-specific data (S3 URIs, etc.)
        :param cleanup_policy: Optional override for cleanup policy
        :return: Resource ID for tracking
        """
        resource_id = f"job_{inference_id}"

        resource_data = {
            "inference_id": inference_id,
            "input_s3_uri": job_data.get("input_s3_uri"),
            "output_s3_uri": job_data.get("output_s3_uri"),
            "temp_files": job_data.get("temp_files", []),
        }

        resource = ManagedResource(
            resource_id=resource_id, resource_type=ResourceType.INFERENCE_JOB, resource_data=resource_data
        )

        with self.resource_lock:
            self.managed_resources[resource_id] = resource

        # Schedule cleanup based on policy
        policy = cleanup_policy or self.cleanup_policies[ResourceType.INFERENCE_JOB]
        self._schedule_cleanup(resource, policy)

        logger.debug(f"Registered inference job for cleanup: {inference_id} (policy: {policy.value})")
        return resource_id

    def register_worker_thread(self, thread: Thread, cleanup_policy: Optional[CleanupPolicy] = None) -> str:
        """
        Register a worker thread for managed cleanup.

        :param thread: Thread instance to manage
        :param cleanup_policy: Optional override for cleanup policy
        :return: Resource ID for tracking
        """
        resource_id = f"thread_{thread.name}_{id(thread)}"

        resource_data = {"thread": thread, "thread_name": thread.name}

        resource = ManagedResource(
            resource_id=resource_id, resource_type=ResourceType.WORKER_THREAD, resource_data=resource_data
        )

        with self.resource_lock:
            self.managed_resources[resource_id] = resource

        # Schedule cleanup based on policy
        policy = cleanup_policy or self.cleanup_policies[ResourceType.WORKER_THREAD]
        self._schedule_cleanup(resource, policy)

        logger.debug(f"Registered worker thread for cleanup: {thread.name} (policy: {policy.value})")
        return resource_id

    def cleanup_resource(self, resource_id: str, force: bool = False) -> bool:
        """
        Clean up a specific resource immediately.

        :param resource_id: ID of the resource to clean up
        :param force: Force cleanup even if policy is disabled
        :return: True if cleanup was successful
        """
        with self.resource_lock:
            resource = self.managed_resources.get(resource_id)
            if not resource:
                logger.warning(f"Resource not found for cleanup: {resource_id}")
                return False

        return self._perform_cleanup(resource, force=force)

    def cleanup_all_resources(self, resource_type: Optional[ResourceType] = None, force: bool = False) -> int:
        """
        Clean up all resources of a specific type or all resources.

        :param resource_type: Optional resource type filter
        :param force: Force cleanup even if policy is disabled
        :return: Number of resources successfully cleaned up
        """
        logger.info("Cleaning up all resources" + (f" of type {resource_type.value}" if resource_type else ""))

        resources_to_cleanup = []
        with self.resource_lock:
            for resource in self.managed_resources.values():
                if resource_type is None or resource.resource_type == resource_type:
                    if not resource.cleanup_attempted or force:
                        resources_to_cleanup.append(resource)

        successful_cleanups = 0
        for resource in resources_to_cleanup:
            if self._perform_cleanup(resource, force=force):
                successful_cleanups += 1

        logger.info(f"Cleaned up {successful_cleanups}/{len(resources_to_cleanup)} resources")
        return successful_cleanups

    def cleanup_failed_job_resources(self, inference_id: str) -> bool:
        """
        Clean up all resources associated with a failed inference job.

        :param inference_id: ID of the failed inference job
        :return: True if all resources were cleaned up successfully
        """
        logger.debug(f"Cleaning up resources for failed job: {inference_id}")

        job_resource_id = f"job_{inference_id}"
        with self.resource_lock:
            job_resource = self.managed_resources.get(job_resource_id)
            if not job_resource:
                logger.warning(f"No job resource found for failed job: {inference_id}")
                return False

        # Clean up the job resource (which includes S3 objects and temp files)
        success = self._perform_cleanup(job_resource, force=True)

        # Also clean up any individual S3 objects that might be registered separately
        s3_uris = [job_resource.resource_data.get("input_s3_uri"), job_resource.resource_data.get("output_s3_uri")]

        for s3_uri in s3_uris:
            if s3_uri:
                s3_resource_id = f"s3_{hash(s3_uri)}"
                with self.resource_lock:
                    s3_resource = self.managed_resources.get(s3_resource_id)
                    if s3_resource:
                        self._perform_cleanup(s3_resource, force=True)

        return success

    def get_resource_stats(self) -> Dict[str, Any]:
        """
        Get statistics about managed resources.

        :return: Dictionary of resource statistics
        """
        with self.resource_lock:
            stats = {
                "total_resources": len(self.managed_resources),
                "by_type": {},
                "cleanup_stats": {"attempted": 0, "successful": 0, "failed": 0},
            }

            for resource in self.managed_resources.values():
                resource_type = resource.resource_type.value
                if resource_type not in stats["by_type"]:
                    stats["by_type"][resource_type] = {
                        "total": 0,
                        "cleanup_attempted": 0,
                        "cleanup_successful": 0,
                        "cleanup_failed": 0,
                    }

                stats["by_type"][resource_type]["total"] += 1

                if resource.cleanup_attempted:
                    stats["by_type"][resource_type]["cleanup_attempted"] += 1
                    stats["cleanup_stats"]["attempted"] += 1

                    if resource.cleanup_successful:
                        stats["by_type"][resource_type]["cleanup_successful"] += 1
                        stats["cleanup_stats"]["successful"] += 1
                    else:
                        stats["by_type"][resource_type]["cleanup_failed"] += 1
                        stats["cleanup_stats"]["failed"] += 1

        return stats

    def _schedule_cleanup(self, resource: ManagedResource, policy: CleanupPolicy) -> None:
        """
        Schedule cleanup for a resource based on policy.

        :param resource: Resource to schedule cleanup for
        :param policy: Cleanup policy to apply
        """
        if policy == CleanupPolicy.DISABLED:
            logger.debug(f"Cleanup disabled for resource: {resource.resource_id}")
            return

        if policy == CleanupPolicy.IMMEDIATE:
            # Add to cleanup queue for immediate processing
            self.cleanup_queue.put(resource)
        elif policy == CleanupPolicy.DELAYED:
            # Add to cleanup queue with delay marker
            delayed_resource = {"resource": resource, "cleanup_time": time.time() + self.cleanup_delay_seconds}
            self.cleanup_queue.put(delayed_resource)

        # Ensure cleanup worker is running
        self.start_cleanup_worker()

    def _cleanup_worker_loop(self) -> None:
        """Main loop for the cleanup worker thread."""
        logger.debug("Resource cleanup worker started")

        try:
            while self.cleanup_worker_running and not self.shutdown_event.is_set():
                try:
                    # Get next cleanup task with timeout
                    cleanup_item = self.cleanup_queue.get(timeout=5.0)

                    if isinstance(cleanup_item, dict) and "cleanup_time" in cleanup_item:
                        # Delayed cleanup item
                        resource = cleanup_item["resource"]
                        cleanup_time = cleanup_item["cleanup_time"]

                        if time.time() >= cleanup_time:
                            self._perform_cleanup(resource)
                        else:
                            # Put back in queue for later
                            self.cleanup_queue.put(cleanup_item)
                            time.sleep(1.0)  # Wait before checking again
                    else:
                        # Immediate cleanup item
                        resource = cleanup_item
                        self._perform_cleanup(resource)

                except Empty:
                    # No cleanup tasks, continue loop
                    continue
                except Exception as e:
                    logger.error(f"Error in cleanup worker loop: {e}")

        finally:
            logger.debug("Resource cleanup worker finished")

    def _perform_cleanup(self, resource: ManagedResource, force: bool = False) -> bool:
        """
        Perform cleanup for a specific resource.

        :param resource: Resource to clean up
        :param force: Force cleanup even if policy is disabled
        :return: True if cleanup was successful
        """
        if resource.cleanup_attempted and not force:
            logger.debug(f"Cleanup already attempted for resource: {resource.resource_id}")
            return resource.cleanup_successful

        logger.debug(f"Performing cleanup for resource: {resource.resource_id} (type: {resource.resource_type.value})")

        resource.cleanup_attempted = True
        resource.cleanup_time = time.time()

        try:
            if resource.resource_type == ResourceType.S3_OBJECT:
                success = self._cleanup_s3_object(resource)
            elif resource.resource_type == ResourceType.TEMP_FILE:
                success = self._cleanup_temp_file(resource)
            elif resource.resource_type == ResourceType.INFERENCE_JOB:
                success = self._cleanup_inference_job(resource)
            elif resource.resource_type == ResourceType.WORKER_THREAD:
                success = self._cleanup_worker_thread(resource)
            else:
                logger.warning(f"Unknown resource type for cleanup: {resource.resource_type}")
                success = False

            resource.cleanup_successful = success

            if success:
                logger.debug(f"Successfully cleaned up resource: {resource.resource_id}")
            else:
                logger.warning(f"Failed to clean up resource: {resource.resource_id}")

            return success

        except Exception as e:
            error_msg = f"Error cleaning up resource {resource.resource_id}: {e}"
            logger.error(error_msg)
            resource.cleanup_error = error_msg
            resource.cleanup_successful = False
            return False

    def _cleanup_s3_object(self, resource: ManagedResource) -> bool:
        """
        Clean up an S3 object.

        :param resource: S3 object resource
        :return: True if cleanup was successful
        """
        try:
            bucket = resource.resource_data["bucket"]
            key = resource.resource_data["key"]

            self.s3_client.delete_object(Bucket=bucket, Key=key)
            logger.debug(f"Deleted S3 object: s3://{bucket}/{key}")
            return True

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            if error_code == "NoSuchKey":
                # Object doesn't exist, consider cleanup successful
                logger.debug(f"S3 object already deleted: {resource.resource_data['s3_uri']}")
                return True
            else:
                logger.warning(f"Failed to delete S3 object: {error_code} - {e}")
                return False

        except Exception as e:
            logger.warning(f"Unexpected error deleting S3 object: {e}")
            return False

    def _cleanup_temp_file(self, resource: ManagedResource) -> bool:
        """
        Clean up a temporary file.

        :param resource: Temp file resource
        :return: True if cleanup was successful
        """
        try:
            file_path = resource.resource_data["file_path"]

            if os.path.exists(file_path):
                os.remove(file_path)
                logger.debug(f"Deleted temp file: {file_path}")
            else:
                logger.debug(f"Temp file already deleted: {file_path}")

            return True

        except Exception as e:
            logger.warning(f"Failed to delete temp file: {e}")
            return False

    def _cleanup_inference_job(self, resource: ManagedResource) -> bool:
        """
        Clean up resources associated with an inference job.

        :param resource: Inference job resource
        :return: True if cleanup was successful
        """
        success = True

        # Clean up S3 objects
        for s3_uri_key in ["input_s3_uri", "output_s3_uri"]:
            s3_uri = resource.resource_data.get(s3_uri_key)
            if s3_uri:
                try:
                    parsed_uri = urlparse(s3_uri)
                    bucket = parsed_uri.netloc
                    key = parsed_uri.path.lstrip("/")

                    self.s3_client.delete_object(Bucket=bucket, Key=key)
                    logger.debug(f"Deleted S3 object for job: {s3_uri}")
                except Exception as e:
                    logger.warning(f"Failed to delete S3 object {s3_uri}: {e}")
                    success = False

        # Clean up temp files
        temp_files = resource.resource_data.get("temp_files", [])
        for file_path in temp_files:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    logger.debug(f"Deleted temp file for job: {file_path}")
            except Exception as e:
                logger.warning(f"Failed to delete temp file {file_path}: {e}")
                success = False

        return success

    def _cleanup_worker_thread(self, resource: ManagedResource) -> bool:
        """
        Clean up a worker thread.

        :param resource: Worker thread resource
        :return: True if cleanup was successful
        """
        try:
            thread = resource.resource_data["thread"]
            thread_name = resource.resource_data["thread_name"]

            if thread.is_alive():
                # Signal thread to stop (implementation depends on thread type)
                if hasattr(thread, "stop"):
                    thread.stop()

                # Wait for thread to finish
                thread.join(timeout=10.0)

                if thread.is_alive():
                    logger.warning(f"Worker thread did not stop gracefully: {thread_name}")
                    return False
                else:
                    logger.debug(f"Worker thread stopped: {thread_name}")
                    return True
            else:
                logger.debug(f"Worker thread already stopped: {thread_name}")
                return True

        except Exception as e:
            logger.warning(f"Failed to cleanup worker thread: {e}")
            return False

    def __enter__(self):
        """Context manager entry."""
        self.start_cleanup_worker()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        try:
            # Clean up all remaining resources
            self.cleanup_all_resources(force=True)
        finally:
            # Stop cleanup worker
            self.stop_cleanup_worker()
