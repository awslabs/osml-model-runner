#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
Status monitor for tracking Model Runner job completion.

This module provides a singleton status monitor that runs in a background thread
to track job status updates from the SQS status queue and update the job tracker.
"""

import json
import logging
from test.load.locust_job_tracker import get_job_tracker
from threading import Thread
from typing import Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class ImageJobStatusMonitor(Thread):
    """
    Monitor the status of image processing jobs by polling an SQS queue.

    This is a singleton thread that updates the job tracker with status updates
    from the SQS status queue.

    :param status_queue_name: Name of the SQS queue to monitor
    :param status_queue_account: AWS account ID containing the SQS queue
    """

    def __init__(self, status_queue_name: str, status_queue_account: str):
        super().__init__(daemon=True, name="ImageJobStatusMonitor")
        self.status_queue_name = status_queue_name
        self.status_queue_account = status_queue_account
        self.running = False
        self.sqs_resource = None
        self.job_tracker = get_job_tracker()

    def _parse_status_message(self, message_body: str) -> Optional[Tuple[str, str, Optional[float]]]:
        """
        Parse an SNS-wrapped status message from the SQS queue.

        Extracts image_id, status, and processing_duration from either MessageAttributes
        or the inner Message body.

        :param message_body: JSON string of the SNS message wrapper
        :return: Tuple of (image_id, status, processing_duration) or None if parsing fails
        """
        try:
            sns_wrapper = json.loads(message_body)
            message_attributes = sns_wrapper.get("MessageAttributes", {})

            # Extract from MessageAttributes first
            image_id = message_attributes.get("image_id", {}).get("Value")
            status = message_attributes.get("status", {}).get("Value") or message_attributes.get("image_status", {}).get(
                "Value"
            )
            processing_duration = message_attributes.get("processing_duration", {}).get("Value")

            # If missing, try parsing inner Message field
            if not image_id or not status:
                inner_message_str = sns_wrapper.get("Message", "")
                if inner_message_str:
                    try:
                        inner_message = (
                            json.loads(inner_message_str) if isinstance(inner_message_str, str) else inner_message_str
                        )
                        if isinstance(inner_message, dict):
                            if not status:
                                status = inner_message.get("status")
                            if not image_id:
                                image_id = inner_message.get("image_id") or inner_message.get("request", {}).get("image_id")
                            if not processing_duration:
                                processing_duration = inner_message.get("request", {}).get("processing_duration")
                    except (json.JSONDecodeError, TypeError):
                        pass

            if image_id and status:
                # Parse and validate processing_duration
                duration = None
                if processing_duration:
                    try:
                        duration = float(processing_duration)
                        if duration <= 0:
                            duration = None
                    except (ValueError, TypeError):
                        pass
                return (image_id, status, duration)

        except (json.JSONDecodeError, KeyError, TypeError):
            pass

        return None

    def run(self) -> None:
        """
        Main thread loop that continuously polls the SQS queue for job status updates.

        Updates the job tracker with statuses received from the queue.
        """
        try:
            self.sqs_resource = boto3.resource("sqs")
            queue = self.sqs_resource.get_queue_by_name(
                QueueName=self.status_queue_name,
                QueueOwnerAWSAccountId=self.status_queue_account,
            )

            logger.info(f"Started monitoring {self.status_queue_name} for image status messages")
            self.running = True

            while self.running:
                try:
                    messages = queue.receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=5, VisibilityTimeout=30)

                    for message in messages:
                        try:
                            parsed = self._parse_status_message(message.body)
                            if parsed:
                                image_id, status, duration = parsed
                                self.job_tracker.update_job_status(image_id, status, duration)
                            message.delete()
                        except Exception as e:
                            logger.debug(f"Error processing status message: {e}")

                except ClientError as error:
                    logger.warning(f"ClientError in status monitor: {error}")
                except Exception as error:
                    logger.warning(f"Unexpected error in status monitor: {error}")

        except Exception as e:
            logger.error(f"Exception in ImageJobStatusMonitor.run() - Stopping: {e}")
            self.running = False

    def stop(self) -> None:
        """Stop the monitoring thread."""
        self.running = False
