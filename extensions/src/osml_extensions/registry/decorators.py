#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
Decorators for registering handlers with the extension registry.
"""

import logging
from typing import Type

from .errors import HandlerRegistrationError
from .extension_registry import get_registry
from .handler_metadata import HandlerMetadata, HandlerType

logger = logging.getLogger(__name__)


def register_handler(request_type: str, handler_type: HandlerType, name: str, description: str = ""):
    """
    Decorator for registering handlers with the extension registry.

    :param request_type: The request type this handler supports (e.g., "http", "sm_endpoint", "async_sm_endpoint")
    :param handler_type: The type of handler (HandlerType enum)
    :param name: Unique name for the handler
    :param description: Human-readable description of the handler
    :return: Decorator function
    """

    def decorator(handler_class: Type) -> Type:
        try:
            # Validate required parameters
            if not request_type:
                raise HandlerRegistrationError("request_type cannot be empty")

            if not isinstance(handler_type, HandlerType):
                raise HandlerRegistrationError(f"handler_type must be a HandlerType enum, got {type(handler_type)}")

            if not name:
                raise HandlerRegistrationError("name cannot be empty")

            if not handler_class:
                raise HandlerRegistrationError("handler_class cannot be None")

            # Create metadata
            metadata = HandlerMetadata(
                name=name, handler_class=handler_class, handler_type=handler_type, description=description
            )

            # Register with the global registry
            registry = get_registry()
            registry.register_handler(request_type, handler_type, metadata)

            logger.info(f"Successfully registered handler '{name}' for request_type='{request_type}'")

        except Exception as e:
            error_msg = f"Failed to register handler '{name}' for request_type='{request_type}': {e}"
            logger.error(error_msg)

            # Re-raise as HandlerRegistrationError if it's not already one
            if not isinstance(e, HandlerRegistrationError):
                raise HandlerRegistrationError(error_msg) from e
            raise

        return handler_class

    return decorator
