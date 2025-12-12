#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

from abc import ABC, abstractmethod
from typing import List, Optional

import shapely.geometry.base

from aws.osml.model_runner.common import ImageDimensions, ImageRegion


class RegionCalculator(ABC):
    """
    Abstract interface for calculating the regions an image will be divided into.

    Implementations of this interface encapsulate the complexity of:
    - Reading image headers
    - Handling credentials for image access
    - Calculating processing bounds with ROI
    - Computing regions based on tiling strategy

    This interface allows different implementations while keeping consumers decoupled
    from implementation details.
    """

    @abstractmethod
    def calculate_regions(
        self,
        image_url: str,
        tile_size: ImageDimensions,
        tile_overlap: ImageDimensions,
        roi: Optional[shapely.geometry.base.BaseGeometry] = None,
        image_read_role: Optional[str] = None,
    ) -> List[ImageRegion]:
        """
        Calculate the regions for an image.

        This is the main public method that hides all implementation complexity.
        Callers only need to provide the image URL and tiling parameters.

        :param image_url: URL or path to the image
        :param tile_size: Size of tiles in pixels
        :param tile_overlap: Overlap between tiles in pixels
        :param roi: Optional region of interest to restrict processing
        :param image_read_role: Optional IAM role ARN for accessing the image
        :return: List of regions (each region is a tuple of ((row, col), (width, height)))
        :raises LoadImageException: If image cannot be read or processed
        """
        pass
