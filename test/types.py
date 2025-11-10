#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
Shared types for integration and load tests.

This module provides local definitions of types used in both integration and load tests
to avoid dependencies on the OSML model runner package.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional, Union


class ModelInvokeMode(str, Enum):
    """
    Enumeration defining the hosting options for CV models.

    This is a local copy for integration tests to avoid dependencies
    on the OSML model runner package.
    """

    NONE = "NONE"
    SM_ENDPOINT = "SM_ENDPOINT"
    HTTP_ENDPOINT = "HTTP_ENDPOINT"


class ImageRequestStatus(str, Enum):
    """
    Enumeration defining status for image processing requests.

    Used by load tests to track job status.
    """

    STARTED = "STARTED"
    PARTIAL = "PARTIAL"
    IN_PROGRESS = "IN_PROGRESS"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


@dataclass
class ImageRequest:
    """
    Request for the Model Runner to process an image.

    This is a lightweight dataclass for integration tests to avoid dependencies
    on the OSML model runner package. It contains only the fields needed by
    the integration test framework.

    Attributes:
        job_id: The unique identifier for the image processing job.
        image_id: A combined identifier for the image, usually composed of the job ID and image URL.
        image_url: The URL location of the image to be processed.
        model_name: The name of the model to use for image processing.
        model_invoke_mode: The mode in which the model is invoked, such as synchronous or asynchronous.
        model_endpoint_parameters: Optional parameters for the model endpoint.
        tile_size: Size of image tiles for processing (scalar value, assumes square tiles).
        tile_overlap: Overlap between tiles as a scalar value.
        tile_format: The format of the tiles (e.g., NITF, GeoTIFF).
        tile_compression: Compression type to use for the tiles (e.g., None, JPEG).
        kinesis_stream_name: Optional full Kinesis stream name for results (e.g., "mr-stream-sink-123456789").
        s3_bucket_name: Optional full S3 bucket name for results (e.g., "mr-bucket-sink-123456789").
        region_of_interest: Optional region of interest specification for processing.
    """

    job_id: str = ""
    image_id: str = ""
    image_url: str = ""
    model_name: str = ""
    model_invoke_mode: ModelInvokeMode = ModelInvokeMode.NONE
    model_endpoint_parameters: Optional[Dict[str, Union[str, int, float, bool]]] = None
    tile_size: int = 512
    tile_overlap: int = 128
    tile_format: str = "GTIFF"
    tile_compression: str = "NONE"
    # Result destination names (per-test configuration, full resolved names)
    kinesis_stream_name: Optional[str] = None
    s3_bucket_name: Optional[str] = None
    # Test-specific parameters
    region_of_interest: Optional[str] = None


@dataclass
class JobStatus:
    """
    Status tracking for a single image processing job.

    Used by load tests to track the status of individual jobs.

    Attributes:
        job_id: The unique identifier for the image processing job.
        image_url: The URL location of the image being processed.
        message_id: The SQS message ID for the queued request.
        status: Current status of the job.
        completed: Whether the job has completed (successfully or failed).
        size: Size of the image in bytes.
        pixels: Total number of pixels in the image.
        start_time: Timestamp when the job was started.
        processing_duration: Duration of processing in seconds (None if not completed).
    """

    job_id: str
    image_url: str
    message_id: str
    status: ImageRequestStatus = ImageRequestStatus.STARTED
    completed: bool = False
    size: int = 0
    pixels: int = 0
    start_time: str = ""
    processing_duration: Optional[float] = None

    def to_dict(self) -> Dict:
        """
        Convert JobStatus to dictionary format.

        :returns: Dictionary representation of the job status.
        """
        return {
            "job_id": self.job_id,
            "image_url": self.image_url,
            "message_id": self.message_id,
            "status": self.status.value,
            "completed": self.completed,
            "size": self.size,
            "pixels": self.pixels,
            "start_time": self.start_time,
            "processing_duration": self.processing_duration,
        }


@dataclass
class LoadTestResults:
    """
    Summary results from a load test run.

    Attributes:
        total_image_sent: Total number of images sent for processing.
        total_image_in_progress: Number of images still in progress.
        total_image_processed: Number of images that completed processing.
        total_image_succeeded: Number of images that succeeded.
        total_image_failed: Number of images that failed.
        total_gb_processed: Total gigabytes of image data processed.
        total_pixels_processed: Total number of pixels processed.
    """

    total_image_sent: int = 0
    total_image_in_progress: int = 0
    total_image_processed: int = 0
    total_image_succeeded: int = 0
    total_image_failed: int = 0
    total_gb_processed: float = 0.0
    total_pixels_processed: int = 0

    def to_dict(self) -> Dict:
        """
        Convert LoadTestResults to dictionary format.

        :returns: Dictionary representation of the results.
        """
        return {
            "total_image_sent": self.total_image_sent,
            "total_image_in_progress": self.total_image_in_progress,
            "total_image_processed": self.total_image_processed,
            "total_image_succeeded": self.total_image_succeeded,
            "total_image_failed": self.total_image_failed,
            "total_gb_processed": self.total_gb_processed,
            "total_pixels_processed": self.total_pixels_processed,
        }
