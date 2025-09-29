#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
from typing import List, Optional

from geojson import Feature
from osgeo import gdal
from osgeo.gdal import Dataset

from osml_extensions.registry import HandlerType, register_handler

from aws.osml.model_runner.database import JobItem
from aws.osml.model_runner.image_request_handler import ImageRequestHandler
from aws.osml.photogrammetry import SensorModel
from aws.osml.model_runner.common import ImageRegion
from aws.osml.model_runner.api import ImageRequest

from .api import ExtendedModelInvokeMode


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

    def queue_region_request(
        self,
        all_regions: List[ImageRegion],
        image_request: ImageRequest,
        raster_dataset: Dataset,
        sensor_model: Optional[SensorModel],
        image_extension: Optional[str],
    ) -> None:

        # logger.info("in enhanced image handler queue region reqeust")

        image_request.model_invoke_mode = ExtendedModelInvokeMode["SM_ENDPOINT_ASYNC"]

        # logger.info(f"image request model invoke: {image_request.model_invoke_mode}")
        # logger.info(f"from detector: {AsyncSMDetector.model_invoke_mode}")

        logger.info(f"image_request: {image_request}")

        super().queue_region_request(
            all_regions=all_regions,
            image_request=image_request,
            raster_dataset=raster_dataset,
            sensor_model=sensor_model,
            image_extension=image_extension,
        )

    def deduplicate(
        self,
        job_item: JobItem,
        features: List[Feature],
        raster_dataset: gdal.Dataset,
        sensor_model: SensorModel,
    ) -> List[Feature]:
        """
        Example enhanced deduplication with additional processing.
        """
        # Call parent implementation
        features = super().deduplicate(job_item, features, raster_dataset, sensor_model)

        # Add example enhancement
        logger.debug(f"Enhanced deduplication processed {len(features)} features")

        return features
