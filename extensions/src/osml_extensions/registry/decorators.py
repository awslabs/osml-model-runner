#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
Decorators for registering components with the extension registry.
"""

import logging
from typing import Type

from .errors import ComponentRegistrationError
from .extension_registry import get_registry
from .component_metadata import ComponentMetadata, ComponentType

logger = logging.getLogger(__name__)


def register_component(request_type: str, component_type: ComponentType, name: str, description: str = ""):
    """
    Decorator for registering components with the extension registry.

    :param request_type: The request type this component supports (e.g., "http", "sm_endpoint", "async_sm_endpoint")
    :param component_type: The type of component (ComponentType enum)
    :param name: Unique name for the component
    :param description: Human-readable description of the component
    :return: Decorator function
    """

    def decorator(component_class: Type) -> Type:
        try:
            # Validate required parameters
            if not request_type:
                raise ComponentRegistrationError("request_type cannot be empty")

            if not isinstance(component_type, ComponentType):
                raise ComponentRegistrationError(f"component_type must be a ComponentType enum, got {type(component_type)}")

            if not name:
                raise ComponentRegistrationError("name cannot be empty")

            if not component_class:
                raise ComponentRegistrationError("component_class cannot be None")

            # Create metadata
            metadata = ComponentMetadata(
                name=name, component_class=component_class, component_type=component_type, description=description
            )

            # Register with the global registry
            registry = get_registry()
            registry.register_component(request_type, component_type, metadata)

            logger.info(f"Successfully registered component '{name}' for request_type='{request_type}'")

        except Exception as e:
            error_msg = f"Failed to register component '{name}' for request_type='{request_type}': {e}"
            logger.error(error_msg)

            # Re-raise as ComponentRegistrationError if it's not already one
            if not isinstance(e, ComponentRegistrationError):
                raise ComponentRegistrationError(error_msg) from e
            raise

        return component_class

    return decorator


# Backward compatibility alias
def register_handler(request_type: str, handler_type, name: str, description: str = ""):
    """
    Backward compatibility decorator for register_component.

    :deprecated: Use register_component instead
    """
    logger.warning("register_handler decorator is deprecated, use register_component instead")
    return register_component(request_type, handler_type, name, description)
