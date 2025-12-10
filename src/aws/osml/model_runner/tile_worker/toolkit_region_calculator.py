#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

import logging
from typing import List, Optional

import shapely.geometry.base

from aws.osml.gdal import GDALConfigEnv, load_gdal_dataset
from aws.osml.model_runner.api import get_image_path
from aws.osml.model_runner.common import ImageDimensions, ImageRegion, get_credentials_for_assumed_role
from aws.osml.model_runner.exceptions import LoadImageException
from aws.osml.model_runner.inference import calculate_processing_bounds

from .region_calculator import RegionCalculator
from .tiling_strategy import TilingStrategy

logger = logging.getLogger(__name__)


class ToolkitRegionCalculator(RegionCalculator):
    """
    GDAL-based implementation of RegionCalculator.

    This implementation:
    - Uses GDAL to read image headers
    - Handles AWS credentials for image access
    - Loads sensor models when available
    - Calculates processing bounds with ROI
    - Uses TilingStrategy to compute regions
    """

    def __init__(self, tiling_strategy: TilingStrategy, region_size: ImageDimensions):
        """
        Initialize the GDAL-based region calculator.

        :param tiling_strategy: Strategy for dividing images into regions
        :param region_size: Default size of regions in pixels (e.g., (10240, 10240))
        """
        self.tiling_strategy = tiling_strategy
        self.region_size = region_size

    def calculate_regions(
        self,
        image_url: str,
        tile_size: ImageDimensions,
        tile_overlap: ImageDimensions,
        roi: Optional[shapely.geometry.base.BaseGeometry] = None,
        image_read_role: Optional[str] = None,
    ) -> List[ImageRegion]:
        """
        Calculate the regions for an image using GDAL.

        Implementation details:
        1. Assume IAM role if provided
        2. Load GDAL dataset and sensor model
        3. Calculate processing bounds (with ROI if provided)
        4. Use TilingStrategy to compute regions
        5. Return list of regions

        :param image_url: URL or path to the image
        :param tile_size: Size of tiles in pixels
        :param tile_overlap: Overlap between tiles in pixels
        :param roi: Optional region of interest to restrict processing
        :param image_read_role: Optional IAM role ARN for accessing the image
        :return: List of regions (each region is a tuple of ((row, col), (width, height)))
        :raises LoadImageException: If image cannot be read or processed
        """
        try:
            # Load image and calculate processing bounds
            processing_bounds = self._load_image_and_calculate_bounds(image_url, roi, image_read_role)

            # Compute regions using the tiling strategy
            regions = self._compute_regions(processing_bounds, tile_size, tile_overlap)

            return regions

        except Exception as err:
            logger.error(f"Failed to calculate regions for image {image_url}: {err}")
            raise LoadImageException(f"Failed to calculate regions for image: {err}") from err

    def _load_image_and_calculate_bounds(
        self,
        image_url: str,
        roi: Optional[shapely.geometry.base.BaseGeometry],
        image_read_role: Optional[str],
    ) -> ImageRegion:
        """
        Load image and calculate processing bounds.

        This internal method handles:
        - Assuming IAM role if needed
        - Loading GDAL dataset
        - Loading sensor model
        - Calculating processing bounds with ROI

        The GDAL dataset is loaded, used to calculate bounds, and then allowed to be
        garbage collected immediately to free resources.

        :param image_url: URL or path to the image
        :param roi: Optional region of interest to restrict processing
        :param image_read_role: Optional IAM role ARN for accessing the image
        :return: Processing bounds as ImageRegion ((row, col), (width, height))
        :raises LoadImageException: If image cannot be loaded or bounds cannot be calculated
        """
        # If this request contains an execution role retrieve credentials that will be used to access data
        assumed_credentials = None
        if image_read_role:
            assumed_credentials = get_credentials_for_assumed_role(image_read_role)

        # This will update the GDAL configuration options to use the security credentials for this
        # request. Any GDAL managed AWS calls (i.e. incrementally fetching pixels from a dataset
        # stored in S3) within this "with" statement will be made using customer credentials. At
        # the end of the "with" scope the credentials will be removed.
        with GDALConfigEnv().with_aws_credentials(assumed_credentials):
            # Extract the virtual image path from the request
            image_path = get_image_path(image_url, image_read_role)

            # Use gdal to load the image url we were given
            raster_dataset, sensor_model = load_gdal_dataset(image_path)

            # Determine how much of this image should be processed.
            # Bounds are: UL corner (row, column), dimensions (w, h)
            processing_bounds = calculate_processing_bounds(raster_dataset, roi, sensor_model)

            if not processing_bounds:
                logger.warning(f"Requested ROI does not intersect image {image_url}. Nothing to do")
                raise LoadImageException("Failed to create processing bounds for image!")

            # Return only the processing bounds; dataset and sensor_model will be garbage collected
            # when this method exits, freeing GDAL resources
            return processing_bounds

    def _compute_regions(
        self, processing_bounds: ImageRegion, tile_size: ImageDimensions, tile_overlap: ImageDimensions
    ) -> List[ImageRegion]:
        """
        Use TilingStrategy to compute the regions.

        :param processing_bounds: The bounds of the area to process
        :param tile_size: Size of tiles in pixels
        :param tile_overlap: Overlap between tiles in pixels
        :return: List of regions
        """
        regions = self.tiling_strategy.compute_regions(processing_bounds, self.region_size, tile_size, tile_overlap)
        return regions
