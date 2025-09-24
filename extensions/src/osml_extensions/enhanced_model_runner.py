#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
import traceback

from aws.osml.model_runner import ModelRunner
from aws.osml.model_runner.tile_worker import TilingStrategy, VariableOverlapTilingStrategy

from .enhanced_app_config import EnhancedServiceConfig
from .registry import DependencyInjectionError, HandlerSelectionError, HandlerSelector

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
    ) -> None:
        """
        Initialize an enhanced model runner with dependency injection support.

        :param tiling_strategy: Defines how a larger image will be broken into chunks for processing

        :return: None
        """
        # Call parent constructor to set up base functionality
        super().__init__(tiling_strategy)

        # Use EnhancedServiceConfig instead of base ServiceConfig
        self.config = EnhancedServiceConfig()

        # Override handlers with enhanced versions if extensions are enabled
        self._setup_enhanced_components()

        logger.debug(f"EnhancedModelRunner initialized with factory: {type(self.region_request_handler).__name__}")

    def _setup_enhanced_components(self) -> None:
        """
        Set up enhanced components based on configuration and dependency injection.

        :return: None
        """
        try:
            # Initialize handler selector and dependency injector
            handler_selector = HandlerSelector()

            # Determine request type from environment or configuration
            request_type = EnhancedServiceConfig.request_type  # ['sm_endpoint', 'async_sm_endpoint']

            logger.debug(f"Setting up components for request_type='{request_type}'")

            region_handler_metadata, image_handler_metadata = handler_selector.select_handlers(request_type)

            # Create image request handler
            # TODO: For now this assumes all image handlers and all region handlers
            # have the same class signature, update this so this is configurable.
            image_handler_args = []
            image_handler_kwargs = dict(
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

            region_handler_args = []
            region_handler_kwargs = dict(
                region_request_table=self.region_request_table,
                job_table=self.job_table,
                region_status_monitor=self.region_status_monitor,
                endpoint_statistics_table=self.endpoint_statistics_table,
                tiling_strategy=self.tiling_strategy,
                endpoint_utils=self.endpoint_utils,
                config=self.config,
            )

            self.image_request_handler = image_handler_metadata.handler_class(*image_handler_args, **image_handler_kwargs)
            self.region_request_handler = region_handler_metadata.handler_class(
                *region_handler_args, **region_handler_kwargs
            )

            logger.debug(
                f"Successfully configured handlers: region='{region_handler_metadata.name}', "
                f"image='{image_handler_metadata.name}'"
            )

        except (HandlerSelectionError, DependencyInjectionError) as e:
            logger.error(f"Failed to set up enhanced components: {e}")
            if hasattr(self.config, "extension_fallback_enabled") and not self.config.extension_fallback_enabled:
                raise

        except Exception as e:
            logger.error(f"Unexpected error setting up enhanced components: {e}")
            logger.debug(f"Traceback: {traceback.format_exc()}")
            if hasattr(self.config, "extension_fallback_enabled") and not self.config.extension_fallback_enabled:
                raise
