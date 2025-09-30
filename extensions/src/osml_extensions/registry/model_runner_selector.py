#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
ModelRunner selection logic for the extension registry system.
"""

import logging
from typing import Optional

from .errors import ComponentSelectionError
from .extension_registry import get_registry
from .component_metadata import ComponentMetadata, ComponentType

logger = logging.getLogger(__name__)


class ModelRunnerSelector:
    """Selects appropriate ModelRunner based on configuration."""

    def __init__(self):
        """Initialize the ModelRunner selector."""
        self.registry = get_registry()

    def select_model_runner(self, request_type: str) -> ComponentMetadata:
        """
        Select appropriate ModelRunner based on configuration.

        :param request_type: Request type to determine which ModelRunner to use
        :return: ComponentMetadata for the selected ModelRunner
        :raises ComponentSelectionError: If no suitable ModelRunner can be found
        """
        try:
            logger.debug(f"Selecting ModelRunner for request_type='{request_type}'")

            # Get ModelRunner for the request type
            model_runner_metadata = self.registry.get_component(request_type, ComponentType.MODEL_RUNNER)

            if not model_runner_metadata:
                # Try fallback to base ModelRunner if available
                logger.warning(f"No ModelRunner found for request_type='{request_type}', trying fallback")
                model_runner_metadata = self._get_fallback_model_runner()

            if not model_runner_metadata:
                raise ComponentSelectionError(
                    f"No ModelRunner found for request_type='{request_type}' and no fallback available"
                )

            logger.debug(f"Selected ModelRunner for request_type='{request_type}': '{model_runner_metadata.name}'")
            return model_runner_metadata

        except Exception as e:
            error_msg = f"Failed to select ModelRunner: {e}"
            logger.error(error_msg)

            if not isinstance(e, ComponentSelectionError):
                raise ComponentSelectionError(error_msg) from e
            raise

    def _get_fallback_model_runner(self) -> Optional[ComponentMetadata]:
        """
        Get fallback ModelRunner (base ModelRunner).

        :return: ComponentMetadata for fallback ModelRunner, or None if not available
        """
        # Try common fallback request types
        fallback_types = ["http", "sm_endpoint", "base"]

        for fallback_type in fallback_types:
            model_runner_metadata = self.registry.get_component(fallback_type, ComponentType.MODEL_RUNNER)
            if model_runner_metadata:
                logger.debug(f"Using fallback ModelRunner from request_type='{fallback_type}'")
                return model_runner_metadata

        return None

    def get_available_model_runners(self) -> dict:
        """
        Get all available ModelRunners by request type.

        :return: Dictionary mapping request types to ModelRunner metadata
        """
        available = {}
        for request_type in self.registry.get_supported_request_types():
            model_runner_metadata = self.registry.get_component(request_type, ComponentType.MODEL_RUNNER)
            if model_runner_metadata:
                available[request_type] = model_runner_metadata
        return available
