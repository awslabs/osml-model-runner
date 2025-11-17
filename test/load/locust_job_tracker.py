#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
Job tracking and statistics for load tests.

This module provides a shared job tracker that maintains detailed information
about all submitted jobs and calculates comprehensive statistics.
"""

import json
import logging
import os
from datetime import datetime
from test.types import ImageRequestStatus, JobStatus, LoadTestResults
from threading import Lock
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# Global job tracker (singleton pattern)
_job_tracker: Optional["JobTracker"] = None
_tracker_lock = Lock()


class JobTracker:
    """
    Tracks all submitted jobs and their detailed status information.

    This is a singleton that maintains a shared dictionary of job statuses
    accessible by all Locust users and the status monitor.
    """

    def __init__(self):
        """Initialize the job tracker."""
        self.job_status_dict: Dict[str, JobStatus] = {}
        self.lock = Lock()

    def register_job(
        self,
        image_id: str,
        job_id: str,
        image_url: str,
        message_id: str,
        size: int,
        pixels: int,
        start_time: str,
    ) -> None:
        """
        Register a new job for tracking.

        :param image_id: Unique image ID (job_id:image_url)
        :param job_id: Job ID
        :param image_url: Image URL
        :param message_id: SQS message ID
        :param size: Image size in bytes
        :param pixels: Total pixels in image
        :param start_time: Start time as formatted string
        """
        with self.lock:
            self.job_status_dict[image_id] = JobStatus(
                job_id=job_id,
                image_url=image_url,
                message_id=message_id,
                status=ImageRequestStatus.STARTED,
                completed=False,
                size=size,
                pixels=pixels,
                start_time=start_time,
                processing_duration=None,
            )

    def update_job_status(self, image_id: str, status: str, processing_duration: Optional[float] = None) -> None:
        """
        Update the status of a tracked job.

        :param image_id: Image ID to update
        :param status: New status value
        :param processing_duration: Processing duration if available
        """
        with self.lock:
            if image_id not in self.job_status_dict:
                logger.debug(f"Job {image_id} not found in tracker")
                return

            job_status = self.job_status_dict[image_id]

            # Only update if not already terminal, or moving from PARTIAL to terminal
            if job_status.status not in [
                ImageRequestStatus.SUCCESS,
                ImageRequestStatus.FAILED,
            ] or (
                job_status.status == ImageRequestStatus.PARTIAL
                and status in [ImageRequestStatus.SUCCESS, ImageRequestStatus.FAILED]
            ):
                try:
                    job_status.status = ImageRequestStatus(status)
                except ValueError:
                    logger.warning(f"Unknown status: {status}")
                    return

                # Mark as completed if terminal status
                if status in [
                    ImageRequestStatus.SUCCESS,
                    ImageRequestStatus.FAILED,
                    ImageRequestStatus.PARTIAL,
                ]:
                    job_status.completed = True
                    if processing_duration is not None:
                        job_status.processing_duration = float(processing_duration)
                    else:
                        # Calculate duration from start_time if not provided
                        try:
                            start_dt = datetime.strptime(job_status.start_time, "%m/%d/%Y/%H:%M:%S")
                            job_status.processing_duration = (datetime.now() - start_dt).total_seconds()
                        except ValueError:
                            logger.warning(f"Could not parse start_time: {job_status.start_time}")

    def is_complete(self) -> bool:
        """
        Check if all jobs have completed.

        :return: True if all jobs are complete, False otherwise
        """
        with self.lock:
            return all(job.completed for job in self.job_status_dict.values())

    def calculate_statistics(self) -> LoadTestResults:
        """
        Calculate comprehensive statistics from tracked jobs.

        :return: LoadTestResults object with summary statistics
        """
        with self.lock:
            results = LoadTestResults()

            results.total_image_sent = len(self.job_status_dict)
            total_size_processed = 0

            for job_status in self.job_status_dict.values():
                if job_status.completed:
                    results.total_image_processed += 1

                    if job_status.status == ImageRequestStatus.SUCCESS:
                        results.total_image_succeeded += 1
                    elif job_status.status in [
                        ImageRequestStatus.FAILED,
                        ImageRequestStatus.PARTIAL,
                    ]:
                        results.total_image_failed += 1

                    total_size_processed += job_status.size
                    results.total_pixels_processed += job_status.pixels
                else:
                    results.total_image_in_progress += 1

            # Convert size to GB
            results.total_gb_processed = total_size_processed / 1024 / 1024 / 1024

            return results

    def to_dict(self) -> Dict:
        """
        Convert all job statuses to dictionary format.

        :return: Dictionary mapping image_id to job status dictionary
        """
        with self.lock:
            return {image_id: job.to_dict() for image_id, job in self.job_status_dict.items()}


def get_job_tracker() -> JobTracker:
    """
    Get the shared job tracker instance (singleton).

    :return: Shared JobTracker instance
    """
    global _job_tracker

    with _tracker_lock:
        if _job_tracker is None:
            _job_tracker = JobTracker()
        return _job_tracker


def write_job_status_file(job_tracker: JobTracker, output_dir: str = "logs") -> str:
    """
    Write job status to JSON file.

    :param job_tracker: JobTracker instance
    :param output_dir: Directory to write files to
    :return: Path to the written file
    """
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, "job_status.json")

    with open(file_path, "w") as f:
        json.dump(job_tracker.to_dict(), f, indent=4)

    logger.info(f"Wrote job status to {file_path}")
    return file_path


def write_job_summary_file(
    results: LoadTestResults,
    output_dir: str = "logs",
    start_time: Optional[datetime] = None,
    stop_time: Optional[datetime] = None,
) -> str:
    """
    Write job summary statistics to JSON file.

    :param results: LoadTestResults object
    :param output_dir: Directory to write files to
    :param start_time: Start time of the load test (optional)
    :param stop_time: Stop time of the load test (optional)
    :return: Path to the written file
    """
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, "job_summary.json")

    # Set start_time and stop_time if provided
    if start_time is not None:
        results.start_time = start_time.isoformat()
    if stop_time is not None:
        results.stop_time = stop_time.isoformat()

    with open(file_path, "w") as f:
        json.dump(results.to_dict(), f, indent=4)

    logger.info(f"Wrote job summary to {file_path}")
    return file_path


def display_statistics(results: LoadTestResults) -> None:
    """
    Display comprehensive statistics to the logger.

    :param results: LoadTestResults object
    """
    logger.info(
        f"""
            Total Images Sent: {results.total_image_sent}
            Total Images In-Progress: {results.total_image_in_progress}
            Total Images Processed: {results.total_image_processed}
            Total Images Succeeded: {results.total_image_succeeded}
            Total Images Failed: {results.total_image_failed}
            Total GB Processed: {results.total_gb_processed:.2f}
            Total Pixels Processed: {results.total_pixels_processed}
            """
    )
