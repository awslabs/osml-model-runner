#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
from typing import Dict, Optional, Union

from osml_extensions.api import ExtendedModelInvokeMode
from osml_extensions.builders import AsyncSMDetectorBuilder

from aws.osml.model_runner.api import ModelInvokeMode
from aws.osml.model_runner.inference.detector import Detector

logger = logging.getLogger(__name__)


class EnhancedFeatureDetectorFactory:
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
    ):
        """
        Initialize the enhanced factory with extension support.

        :param endpoint: URL of the inference model endpoint
        :param endpoint_mode: the type of endpoint (HTTP, SageMaker)
        :param assumed_credentials: optional credentials to use with the model
        """
        self.endpoint = endpoint
        self.endpoint_mode = endpoint_mode
        self.assumed_credentials = assumed_credentials

        logger.info(f"EnhancedFeatureDetectorFactory initialized - " f"endpoint: {endpoint}, mode: {endpoint_mode}, ")

    def _build_with_original_factory(self) -> Optional[Detector]:
        """
        Build a detector using the original factory as fallback.

        :return: Optional[Detector] = A detector instance from the original factory
        """
        try:
            from osml_extensions.entry_point import get_original_feature_detector_factory

            original_factory_class = get_original_feature_detector_factory()
            original_factory = original_factory_class(self.endpoint, self.endpoint_mode, self.assumed_credentials)  # type: ignore
            return original_factory.build()
        except Exception as fallback_error:
            logger.error(f"Fallback to original factory also failed: {fallback_error}")
            return None

    def build(self) -> Optional[Detector]:
        """
        Build a detector instance, preferring extensions when enabled and available.

        This method attempts to create extension-based detectors first, then falls
        back to base implementations if extensions are disabled or fail to load.

        :return: Optional[Detector] = A detector instance (extended or base)
        """
        try:
            logger.debug("Attempting to build detector with extensions")

            if self.endpoint_mode == ExtendedModelInvokeMode.SM_ENDPOINT_ASYNC:
                return AsyncSMDetectorBuilder(endpoint=self.endpoint, assumed_credentials=self.assumed_credentials).build()
            else:
                # Use the original factory for base functionality
                logger.debug("Using original factory for base detector types")
                return self._build_with_original_factory()

        except ImportError as e:
            logger.warning(f"Extension classes not available: {e}")
            return self._build_with_original_factory()
        except Exception as e:
            logger.error(f"Failed to build detector with extensions: {e}")
            return self._build_with_original_factory()
