#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
from typing import List

from aws_embedded_metrics import MetricsLogger
from geojson import Feature
from osgeo import gdal
from osml_extensions.registry import HandlerType, register_handler

from aws.osml.model_runner.database import JobItem
from aws.osml.model_runner.image_request_handler import ImageRequestHandler
from aws.osml.photogrammetry import SensorModel

logger = logging.getLogger(__name__)


@register_handler(
    request_type="async_sm_endpoint",
    handler_type=HandlerType.IMAGE_REQUEST_HANDLER,
    name="enhanced_image_request_handler",
    description="Enhanced image request handler with async processing capabilities",
)
class EnhancedImageRequestHandler(ImageRequestHandler):
    """
    Example enhanced ImageRequestHandler showing extension patterns.

    This minimal example demonstrates how to extend the base ImageRequestHandler
    with additional functionality while maintaining compatibility.
    """

    def deduplicate(
        self,
        job_item: JobItem,
        features: List[Feature],
        raster_dataset: gdal.Dataset,
        sensor_model: SensorModel,
        metrics: MetricsLogger = None,
    ) -> List[Feature]:
        """
        Example enhanced deduplication with additional processing.
        """
        # Call parent implementation
        features = super().deduplicate(job_item, features, raster_dataset, sensor_model, metrics)

        # Add example enhancement
        logger.debug(f"Enhanced deduplication processed {len(features)} features")

        return features
