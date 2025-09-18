#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
import traceback
from typing import Dict, Optional, Union

from osml_extensions import EnhancedServiceConfig
from osml_extensions.api import ExtendedModelInvokeMode
from osml_extensions.detectors import AsyncSMDetectorBuilder
from osml_extensions.errors import ExtensionRuntimeError

from aws.osml.model_runner.api import ModelInvokeMode
from aws.osml.model_runner.inference.detector import Detector
from aws.osml.model_runner.inference.endpoint_factory import FeatureDetectorFactory

logger = logging.getLogger(__name__)


class EnhancedFeatureDetectorFactory(FeatureDetectorFactory):
    """
    Enhanced factory that supports both base and extended detector classes
    with graceful fallback capabilities.

    This factory maintains full compatibility with the base factory while adding
    support for extension classes.
    """

    def __init__(
        self,
        endpoint: str,
        endpoint_mode: Union[ModelInvokeMode, ExtendedModelInvokeMode],
        assumed_credentials: Optional[Dict[str, str]] = None,
        use_extensions: Optional[bool] = None,
        config: Optional[EnhancedServiceConfig] = None,
    ):
        """
        Initialize the enhanced factory with extension support.

        :param endpoint: URL of the inference model endpoint
        :param endpoint_mode: the type of endpoint (HTTP, SageMaker, or extended modes)
        :param assumed_credentials: optional credentials to use with the model
        :param use_extensions: override for extension usage (defaults to config)
        :param config: EnhancedServiceConfig instance (defaults to new instance)
        """
        # Store the original mode for our enhanced logic
        self.original_endpoint_mode = endpoint_mode
        self.use_extensions = use_extensions
        self.config = config or EnhancedServiceConfig()

        # Convert to base mode for parent class if needed
        if isinstance(endpoint_mode, ExtendedModelInvokeMode):
            base_mode = self._get_compatible_base_mode(endpoint_mode)
            logger.debug(f"Converting extended mode {endpoint_mode} to base mode {base_mode}")
            super().__init__(endpoint, base_mode, assumed_credentials)
        else:
            # Standard base mode, pass through directly
            super().__init__(endpoint, endpoint_mode, assumed_credentials)

        logger.info(f"EnhancedFeatureDetectorFactory initialized - endpoint: {endpoint}, mode: {endpoint_mode}")

    def _should_use_extensions(self) -> bool:
        """
        Determine if extensions should be used based on configuration.

        :return: True if extensions should be used
        """
        if self.use_extensions is not None:
            return self.use_extensions

        return self.config.use_extensions

    def _get_compatible_base_mode(self, extended_mode: ExtendedModelInvokeMode) -> ModelInvokeMode:
        """
        Map extended modes to compatible base modes for parent class.

        :param extended_mode: The extended mode to map
        :return: Compatible base mode
        """
        mapping = {
            ExtendedModelInvokeMode.SM_ENDPOINT_ASYNC: ModelInvokeMode.SM_ENDPOINT,
            # Future mappings would go here
        }
        return mapping.get(extended_mode, ModelInvokeMode.SM_ENDPOINT)

    def _build_enhanced_detector(self) -> Optional[Detector]:
        """
        Build an enhanced detector for extended modes.

        :return: Enhanced detector instance or None if not available
        :raises: ExtensionRuntimeError if enhanced detector creation fails
        """
        try:
            if isinstance(self.original_endpoint_mode, ExtendedModelInvokeMode):
                if self.original_endpoint_mode == ExtendedModelInvokeMode.SM_ENDPOINT_ASYNC:
                    # Import here to avoid circular imports
                    builder = AsyncSMDetectorBuilder(endpoint=self.endpoint, assumed_credentials=self.assumed_credentials)
                    return builder.build()

            return None

        except ImportError as e:
            logger.warning(f"Enhanced detector components not available: {e}")
            logger.debug(f"Traceback: {traceback.format_exc()}")
            raise ExtensionRuntimeError(f"Enhanced detector creation failed: {e}") from e
        except Exception as e:
            logger.error(f"Failed to build enhanced detector: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise ExtensionRuntimeError(f"Enhanced detector creation failed: {e}") from e

    def build(self) -> Optional[Detector]:
        """
        Build a detector instance, preferring extensions when enabled and available.

        This method attempts to create extension-based detectors first, then falls
        back to base implementations if extensions are disabled or fail to load.

        :return: Optional[Detector] = A detector instance (extended or base)
        """
        logger.debug("Building detector with enhanced factory")

        # Check if we should use extensions
        if not self._should_use_extensions():
            logger.debug("Extensions disabled, using base factory")
            return super().build()

        # Try to build enhanced detector for extended modes
        if isinstance(self.original_endpoint_mode, ExtendedModelInvokeMode):
            try:
                enhanced_detector = self._build_enhanced_detector()
                if enhanced_detector is not None:
                    logger.info(f"Successfully created enhanced detector for mode {self.original_endpoint_mode}")
                    return enhanced_detector
                else:
                    logger.debug("Enhanced detector returned None, falling back to base")
            except ExtensionRuntimeError as e:
                logger.warning(f"Enhanced detector creation failed: {e}, falling back to base")
                logger.debug(f"Traceback: {traceback.format_exc()}")
                # Check if fallback is enabled
                if not self.config.extension_fallback_enabled:
                    raise e

        # Fall back to parent implementation for base modes or when enhanced fails
        logger.debug("Using base factory implementation")
        return super().build()
