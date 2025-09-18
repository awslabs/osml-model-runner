#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
from typing import List

from geojson import Feature
from osgeo import gdal

from aws.osml.model_runner.database import JobItem
from aws.osml.model_runner.image_request_handler import ImageRequestHandler
from aws.osml.photogrammetry import SensorModel

logger = logging.getLogger(__name__)


class EnhancedImageRequestHandler(ImageRequestHandler):
    """
    Example enhanced ImageRequestHandler showing extension patterns.
    
    This minimal example demonstrates how to extend the base ImageRequestHandler
    with additional functionality while maintaining compatibility.
    """

    def deduplicate(
        self,
        job_item: JobItem,
        raster_dataset: gdal.Dataset,
        sensor_model: SensorModel,
        metrics=None,
    ) -> List[Feature]:
        """
        Example enhanced deduplication with additional processing.
        """
        # Call parent implementation
        features = super().deduplicate(job_item, raster_dataset, sensor_model, metrics)
        
        # Add example enhancement
        logger.debug(f"Enhanced deduplication processed {len(features)} features")
        
        return features