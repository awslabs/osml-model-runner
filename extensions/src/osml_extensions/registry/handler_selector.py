#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
Handler selection logic for the extension registry system.
"""

import logging
from typing import Tuple

from .errors import HandlerSelectionError
from .extension_registry import get_registry
from .handler_metadata import HandlerMetadata, HandlerType

logger = logging.getLogger(__name__)


class HandlerSelector:
    """Selects appropriate handlers based on configuration."""

    def __init__(self):
        """Initialize the handler selector."""
        self.registry = get_registry()

    def select_handlers(
        self,
        request_type: str,
    ) -> Tuple[HandlerMetadata, HandlerMetadata]:
        """
        Select appropriate handler pair based on configuration.

        :param request_type: Explicit request type to use
        :return: Tuple of (region_handler, image_handler)
        :raises HandlerSelectionError: If no suitable handlers can be found
        """
        try:
            logger.info(f"Selecting handlers for request_type='{request_type}'")

            # Validate request type is supported
            if not self._validate_request_type_support(request_type):
                logger.warning(f"Request type '{request_type}' not supported, falling back to 'http'")
                request_type = "http"

                # If http is also not supported, this is a critical error
                if not self._validate_request_type_support(request_type):
                    raise HandlerSelectionError("No fallback handlers available - 'http' request type not registered")

            # Get handlers for the determined request type
            handlers = self.registry.get_handlers_for_request_type(request_type)

            # Extract region and image handlers
            region_handler = handlers.get(HandlerType.REGION_REQUEST_HANDLER)
            image_handler = handlers.get(HandlerType.IMAGE_REQUEST_HANDLER)

            # Validate both handlers are available
            if not region_handler:
                raise HandlerSelectionError(f"No region request handler found for request_type='{request_type}'")

            if not image_handler:
                raise HandlerSelectionError(f"No image request handler found for request_type='{request_type}'")

            logger.info(
                f"Selected handlers for request_type='{request_type}': "
                f"region='{region_handler.name}', image='{image_handler.name}'"
            )

            return region_handler, image_handler

        except Exception as e:
            error_msg = f"Failed to select handlers: {e}"
            logger.error(error_msg)

            if not isinstance(e, HandlerSelectionError):
                raise HandlerSelectionError(error_msg) from e
            raise

    def _validate_request_type_support(self, request_type: str) -> bool:
        """
        Validate that the request type is supported by the registry.

        :param request_type: Request type to validate
        :return: True if supported, False otherwise
        """
        supported_types = self.registry.get_supported_request_types()
        return request_type in supported_types
