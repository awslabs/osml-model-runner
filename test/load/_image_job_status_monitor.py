# Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

"""
Background thread for monitoring image processing job status updates.

This monitor polls an SQS queue for job status updates and maintains a cache of
recent job statuses accessible to all Locust users.

Aligned with the more robust pattern in `test/load`:
- Long polling + batching
- Visibility timeout
- Parsing both direct SQS messages and SNS-wrapped messages
"""

import json
import logging
from collections import OrderedDict
from threading import Lock, Thread
from typing import Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class ImageJobStatusMonitor(Thread):
    """
    Monitor job status by polling an SQS queue.
    """

    def __init__(self, status_queue_name: str, status_queue_account: str, max_size: int = 1000):
        super().__init__(daemon=True, name="ImageJobStatusMonitor")
        self.status_queue_name = status_queue_name
        self.status_queue_account = status_queue_account
        self.max_size = max_size
        self.running = False
        self.job_status_cache: OrderedDict[str, Tuple[str, Optional[int]]] = OrderedDict()
        self.job_status_cache_lock = Lock()
        self.unparseable_message_count = 0

    def run(self) -> None:
        """
        Poll SQS for status messages and update the in-memory status cache.

        :returns: None
        """
        try:
            sqs_resource = boto3.resource("sqs")
            queue = sqs_resource.get_queue_by_name(
                QueueName=self.status_queue_name,
                QueueOwnerAWSAccountId=self.status_queue_account,
            )

            logger.info("Started monitoring %s for image status messages", self.status_queue_name)
            self.running = True

            while self.running:
                try:
                    messages = queue.receive_messages(
                        MaxNumberOfMessages=10,
                        WaitTimeSeconds=5,
                        VisibilityTimeout=30,
                    )
                    for message in messages:
                        try:
                            parsed = self._parse_status_message(message.body)
                            if parsed:
                                key, status, duration = parsed
                                self._update_cache(key, status, duration)
                                message.delete()
                                continue

                            # If we can't parse it at all, delete to avoid infinite re-processing loops.
                            self.unparseable_message_count += 1
                            if self.unparseable_message_count in (1, 10, 100) or (
                                self.unparseable_message_count % 1000 == 0
                            ):
                                logger.warning(
                                    "Dropping unparseable status message (count=%s) from queue=%s",
                                    self.unparseable_message_count,
                                    self.status_queue_name,
                                )
                            message.delete()
                        except Exception as e:
                            logger.debug("Error processing status message: %s", e, exc_info=True)
                except ClientError as e:
                    logger.warning("ClientError in status monitor: %s", e)
                except Exception as e:
                    logger.warning("Unexpected error in status monitor: %s", e, exc_info=True)

        except ClientError as e:
            logger.error("Error accessing SQS queue %s: %s", self.status_queue_name, e)
        except Exception as e:
            logger.error("Exception in ImageJobStatusMonitor.run() - Stopping: %s", e, exc_info=True)
        finally:
            self.running = False
            logger.info("Status monitor stopped")

    def stop(self) -> None:
        """
        Request the monitor thread to stop.

        :returns: None
        """
        self.running = False

    def check_job_status(self, job_id: str) -> Tuple[Optional[str], Optional[int]]:
        """
        Retrieve the latest known status for a job.

        :param job_id: Job identifier.
        :returns: Tuple of `(status, processing_duration)`; values may be `None` if unknown.
        """
        with self.job_status_cache_lock:
            return self.job_status_cache.get(job_id, (None, None))

    def _parse_status_message(self, message_body: str) -> Optional[Tuple[str, str, Optional[int]]]:
        """
        Parse a status message from SQS.

        Supports:
        - Direct attribute wrapper: {"MessageAttributes": {...}}
        - SNS-wrapped message: {"MessageAttributes": {...}, "Message": "{...json...}"}

        :param message_body: Raw message body from SQS.
        :returns: Tuple of `(key, status, duration)` or `None` if the message cannot be parsed.
        """
        try:
            wrapper = json.loads(message_body)
        except json.JSONDecodeError:
            return None

        if not isinstance(wrapper, dict):
            return None

        message_attributes = wrapper.get("MessageAttributes", {}) or {}

        key = (
            message_attributes.get("job_id", {}).get("Value")
            or message_attributes.get("jobId", {}).get("Value")
            or message_attributes.get("image_id", {}).get("Value")
        )
        status = (
            message_attributes.get("status", {}).get("Value")
            or message_attributes.get("image_status", {}).get("Value")
            or message_attributes.get("imageStatus", {}).get("Value")
        )
        processing_duration = message_attributes.get("processing_duration", {}).get("Value")

        # If missing, try parsing inner Message field
        if (not key or not status) and wrapper.get("Message"):
            inner_message_str = wrapper.get("Message", "")
            try:
                inner = json.loads(inner_message_str) if isinstance(inner_message_str, str) else inner_message_str
            except (json.JSONDecodeError, TypeError):
                inner = None

            if isinstance(inner, dict):
                status = status or inner.get("status")
                key = (
                    key
                    or inner.get("job_id")
                    or inner.get("jobId")
                    or inner.get("image_id")
                    or inner.get("request", {}).get("job_id")
                    or inner.get("request", {}).get("jobId")
                    or inner.get("request", {}).get("image_id")
                )
                if processing_duration is None:
                    processing_duration = (
                        inner.get("processing_duration")
                        or inner.get("processingDuration")
                        or inner.get("request", {}).get("processing_duration")
                        or inner.get("request", {}).get("processingDuration")
                    )

        if not key or not status:
            return None

        duration_int: Optional[int] = None
        if processing_duration is not None:
            try:
                duration_float = float(processing_duration)
                if duration_float > 0:
                    duration_int = int(duration_float)
            except (TypeError, ValueError):
                duration_int = None

        # If the key is "<job_id>:<image_url>", also cache by the job_id prefix so our wait-by-job_id works.
        if ":" in key:
            maybe_job_id = key.split(":", 1)[0]
            if maybe_job_id:
                self._update_cache(maybe_job_id, status, duration_int)

        return key, status, duration_int

    def _update_cache(self, key: str, status: str, processing_duration: Optional[int]) -> None:
        """
        Update the in-memory cache with the newest status.

        This maintains a bounded LRU-style cache (oldest evicted first).

        :param key: Cache key for the status (often job id; may include other identifiers).
        :param status: Job status string.
        :param processing_duration: Optional processing duration for the job.
        :returns: None
        """
        with self.job_status_cache_lock:
            current_status, _ = self.job_status_cache.get(key, (None, None))

            should_update = (
                current_status is None
                or current_status not in ["SUCCESS", "FAILED"]
                or (current_status == "PARTIAL" and status in ["SUCCESS", "FAILED"])
            )
            if not should_update:
                return

            if key in self.job_status_cache:
                self.job_status_cache.pop(key)

            if len(self.job_status_cache) >= self.max_size:
                self.job_status_cache.popitem(last=False)

            self.job_status_cache[key] = (status, processing_duration)
