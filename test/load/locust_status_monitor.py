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
                            message_body = json.loads(message.body)
                            message_attributes = message_body.get("MessageAttributes", {})

                            message_image_id = message_attributes.get("image_id", {}).get("Value")
                            message_image_status = message_attributes.get("image_status", {}).get(
                                "Value"
                            ) or message_attributes.get("status", {}).get("Value")
                            processing_duration = message_attributes.get("processing_duration", {}).get("Value")

                            if message_image_id and message_image_status:
                                # Update the job tracker
                                duration = float(processing_duration) if processing_duration else None
                                self.job_tracker.update_job_status(message_image_id, message_image_status, duration)

                            message.delete()

                        except (json.JSONDecodeError, KeyError) as e:
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
