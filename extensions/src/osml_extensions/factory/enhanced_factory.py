#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
import os
from typing import Dict, Optional

from osml_extensions.builders.async_sm_builder import AsyncSMDetectorBuilder

from aws.osml.model_runner.api import ModelInvokeMode
from aws.osml.model_runner.inference.detector import Detector
from aws.osml.model_runner.inference.endpoint_factory import FeatureDetectorFactory

logger = logging.getLogger(__name__)


class EnhancedFeatureDetectorFactory(FeatureDetectorFactory):
    """
    Enhanced factory that extends FeatureDetectorFactory to support both base
    and extended detector classes with graceful fallback capabilities.

    This factory maintains full compatibility with the base factory while adding
    support for extension classes.
    """

    def __init__(
        self,
        endpoint: str,
        endpoint_mode: ModelInvokeMode,
        use_extensions: Optional[bool] = None,
        assumed_credentials: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize the enhanced factory with extension support.

        :param endpoint: URL of the inference model endpoint
        :param endpoint_mode: the type of endpoint (HTTP, SageMaker)
        :param use_extensions: whether to use extensions (defaults to environment variable)
        :param assumed_credentials: optional credentials to use with the model
        """
        super().__init__(endpoint, endpoint_mode, assumed_credentials)

        # Use provided value or fall back to environment variable
        if use_extensions is not None:
            self.use_extensions = use_extensions
        else:
            env_value = os.getenv("USE_EXTENSIONS", "true").lower()
            self.use_extensions = env_value in ("true", "1", "yes", "on", "enabled")

        logger.info(
            f"EnhancedFeatureDetectorFactory initialized - "
            f"endpoint: {endpoint}, mode: {endpoint_mode}, "
            f"extensions_enabled: {self.use_extensions}"
        )

    def build(self) -> Optional[Detector]:
        """
        Build a detector instance, preferring extensions when enabled and available.

        This method attempts to create extension-based detectors first, then falls
        back to base implementations if extensions are disabled or fail to load.

        :return: Optional[Detector] = A detector instance (extended or base)
        """
        detector = None

        # Try to build using extensions first
        if self.use_extensions:
            detector = self._build_with_extensions()

        # Fall back to base implementation if extensions failed or are disabled
        if detector is None:
            logger.info("Building detector using base implementation")
            detector = self._build_base_detector()

        if detector is None:
            logger.error(f"Failed to build any detector for endpoint: {self.endpoint}")
        else:
            logger.info(f"Successfully built detector: {type(detector).__name__}")

        return detector

    def _build_with_extensions(self) -> Optional[Detector]:
        """
        Attempt to build a detector using extension classes.

        :return: Optional[Detector] = Extended detector instance or None if failed
        """
        try:
            logger.debug("Attempting to build detector with extensions")

            if self.endpoint_mode == ModelInvokeMode.SM_ENDPOINT:
                return self._build_async_sm_detector()
            else:
                logger.debug(f"No extension available for endpoint mode: {self.endpoint_mode}")
                return None

        except ImportError as e:
            logger.warning(f"Extension classes not available: {e}")
            return None
        except Exception as e:
            logger.error(f"Failed to build detector with extensions: {e}")
            return None

    def _build_async_sm_detector(self) -> Optional[Detector]:
        """
        Build an AsyncSMDetector instance.

        :return: Optional[Detector] = AsyncSMDetector instance or None if failed
        """
        try:
            logger.debug("Building AsyncSMDetector")

            if not self.assumed_credentials:
                raise ValueError("Assumed credentials cann't be None")

            builder = AsyncSMDetectorBuilder(endpoint=self.endpoint, assumed_credentials=self.assumed_credentials)

            detector = builder.build()

            if detector is not None:
                logger.info(f"Successfully built AsyncSMDetector for endpoint: {self.endpoint}")

            return detector

        except Exception as e:
            logger.error(f"Failed to build AsyncSMDetector: {e}")
            return None

    def _build_base_detector(self) -> Optional[Detector]:
        """
        Build a detector using the base factory implementation.

        :return: Optional[Detector] = Base detector instance or None if failed
        """
        try:
            return super().build()
        except Exception as e:
            logger.error(f"Failed to build base detector: {e}")
            return None

    def get_factory_info(self) -> Dict[str, Optional[str]]:
        """
        Get information about the factory configuration for debugging.

        :return: Dict[str, Optional[str]] = Factory configuration information
        """
        return {
            "endpoint": self.endpoint,
            "endpoint_mode": self.endpoint_mode.value if self.endpoint_mode else None,
            "use_extensions": str(self.use_extensions),
            "has_credentials": str(self.assumed_credentials is not None),
        }

    def is_extension_available(self, extension_name: str) -> bool:
        """
        Check if a specific extension is available.

        :param extension_name: Name of the extension to check
        :return: bool = True if extension is available, False otherwise
        """
        try:
            if extension_name == "AsyncSMDetector":
                # Try to import to check availability
                import osml_extensions.detectors.async_sm_detector  # noqa: F401

                return True
            return False
        except ImportError:
            return False
