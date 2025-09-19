#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
import traceback
from typing import Optional, Type

from aws.osml.model_runner import ModelRunner
from aws.osml.model_runner.inference import FeatureDetectorFactory
from aws.osml.model_runner.tile_worker import TilingStrategy, VariableOverlapTilingStrategy

from .enhanced_app_config import EnhancedServiceConfig
from .enhanced_image_handler import EnhancedImageRequestHandler
from .enhanced_region_handler import EnhancedRegionRequestHandler

logger = logging.getLogger(__name__)


class EnhancedModelRunner(ModelRunner):
    """
    Enhanced ModelRunner with dependency injection support for extensions.

    This class demonstrates how the ModelRunner could be modified to support
    dependency injection while maintaining backward compatibility.
    """

    def __init__(
        self,
        tiling_strategy: TilingStrategy = VariableOverlapTilingStrategy(),
        factory_class: Optional[Type[FeatureDetectorFactory]] = None,
    ) -> None:
        """
        Initialize an enhanced model runner with dependency injection support.

        :param tiling_strategy: Defines how a larger image will be broken into chunks for processing
        :param factory_class: Optional custom factory class for creating detectors

        :return: None
        """
        # Store injected classes before calling parent constructor
        self.factory_class = factory_class

        # Call parent constructor to set up base functionality
        super().__init__(tiling_strategy)

        # Use EnhancedServiceConfig instead of base ServiceConfig
        self.config = EnhancedServiceConfig()

        # Override handlers with enhanced versions if extensions are enabled
        self._setup_enhanced_components()

        logger.info(f"EnhancedModelRunner initialized with factory: {type(self.region_request_handler).__name__}")

    def _setup_enhanced_components(self) -> None:
        """
        Set up enhanced components based on configuration and dependency injection.

        :return: None
        """
        try:
            # Determine if we should use enhanced components
            if not self.config.use_extensions:
                logger.info("Extensions disabled, using base components")
                return

            # Create enhanced region request handler if configured
            self.region_request_handler = EnhancedRegionRequestHandler(
                region_request_table=self.region_request_table,
                job_table=self.job_table,
                region_status_monitor=self.region_status_monitor,
                endpoint_statistics_table=self.endpoint_statistics_table,
                tiling_strategy=self.tiling_strategy,
                endpoint_utils=self.endpoint_utils,
                config=self.config,
            )

            self.image_request_handler = EnhancedImageRequestHandler(
                job_table=self.job_table,
                image_status_monitor=self.image_status_monitor,
                endpoint_statistics_table=self.endpoint_statistics_table,
                tiling_strategy=self.tiling_strategy,
                region_request_queue=self.region_request_queue,
                region_request_table=self.region_request_table,
                endpoint_utils=self.endpoint_utils,
                config=self.config,
                region_request_handler=self.region_request_handler,
            )
            logger.info("Enhanced components configured successfully")

        except Exception as e:
            logger.warning(f"Failed to set up enhanced components: {e}, using base components")
            logger.debug(f"Traceback: {traceback.format_exc()}")
