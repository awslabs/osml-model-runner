#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
Standalone types for integration tests.

This module provides local definitions of types used in integration tests
to avoid dependencies on the OSML model runner package.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional


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
        tile_overlap: Overlap between tiles, defined in dimensions.
        tile_format: The format of the tiles (e.g., NITF, GeoTIFF).
        tile_compression: Compression type to use for the tiles (e.g., None, JPEG).
    """

    job_id: str = ""
    image_id: str = ""
    image_url: str = ""
    model_name: str = ""
    model_invoke_mode: ModelInvokeMode = ModelInvokeMode.NONE
    model_endpoint_parameters: Optional[Dict[str, Any]] = None
    tile_size: tuple = (1024, 1024)
    tile_overlap: tuple = (50, 50)
    tile_format: str = "GTIFF"
    tile_compression: str = "NONE"

    @property
    def tile_size_scalar(self) -> int:
        """
        Get tile size as a scalar value.

        Returns:
            The first element of tile_size tuple if it's a tuple, otherwise tile_size itself
        """
        return self.tile_size[0] if isinstance(self.tile_size, tuple) else self.tile_size

    @property
    def tile_overlap_scalar(self) -> int:
        """
        Get tile overlap as a scalar value.

        Returns:
            The first element of tile_overlap tuple if it's a tuple, otherwise tile_overlap itself
        """
        return self.tile_overlap[0] if isinstance(self.tile_overlap, tuple) else self.tile_overlap
