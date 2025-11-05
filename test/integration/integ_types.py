#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
Standalone types for integration tests.

This module provides local definitions of types used in integration tests
to avoid dependencies on the OSML model runner package.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional, Tuple, Union


class ModelInvokeMode(str, Enum):
    """
    Enumeration defining the hosting options for CV models.

    This is a local copy for integration tests to avoid dependencies
    on the OSML model runner package.
    """

    NONE = "NONE"
    SM_ENDPOINT = "SM_ENDPOINT"
    HTTP_ENDPOINT = "HTTP_ENDPOINT"


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
        tile_size: Dimensions of the tiles into which the image is split for processing.
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
    tile_size: Tuple[int, int] = (512, 512)
    tile_overlap: int = 128
    tile_format: str = "GTIFF"
    tile_compression: str = "NONE"
    # Result destination names (per-test configuration, full resolved names)
    kinesis_stream_name: Optional[str] = None
    s3_bucket_name: Optional[str] = None
    # Test-specific parameters
    region_of_interest: Optional[str] = None

    @property
    def tile_size_scalar(self) -> int:
        """
        Get tile size as a scalar value.

        :returns: The first element of tile_size tuple, or tile_size itself if not a tuple.
        """
        return self.tile_size[0] if isinstance(self.tile_size, tuple) else self.tile_size
