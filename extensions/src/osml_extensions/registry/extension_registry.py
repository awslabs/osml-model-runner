#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
Central registry for all component extensions.
"""

import logging
import threading
from typing import Dict, List, Optional

from .errors import ComponentRegistrationError
from .component_metadata import ComponentMetadata, ComponentType

logger = logging.getLogger(__name__)


class ExtensionRegistry:
    """Central registry for all component extensions."""

    def __init__(self):
        """Initialize the extension registry."""
        self._components: Dict[str, Dict[ComponentType, ComponentMetadata]] = {}
        self._lock = threading.RLock()
        logger.debug("ExtensionRegistry initialized")

    def register_component(self, request_type: str, component_type: ComponentType, metadata: ComponentMetadata) -> None:
        """
        Register a component with the registry.

        :param request_type: The request type (e.g., "http", "sm_endpoint", "async_sm_endpoint")
        :param component_type: The type of component being registered
        :param metadata: Metadata describing the component
        :raises ComponentRegistrationError: If registration fails
        """
        if not request_type:
            raise ComponentRegistrationError("Request type cannot be empty")

        if not isinstance(component_type, ComponentType):
            raise ComponentRegistrationError(f"component_type must be a ComponentType enum, got {type(component_type)}")

        if not isinstance(metadata, ComponentMetadata):
            raise ComponentRegistrationError(f"metadata must be a ComponentMetadata instance, got {type(metadata)}")

        with self._lock:
            if request_type not in self._components:
                self._components[request_type] = {}

            # Check if component already exists for this request type and component type
            if component_type in self._components[request_type]:
                existing = self._components[request_type][component_type]
                logger.warning(
                    f"Overriding existing component for request_type='{request_type}', "
                    f"component_type='{component_type.value}': {existing.name} -> {metadata.name}"
                )

            self._components[request_type][component_type] = metadata
            logger.debug(
                f"Registered component '{metadata.name}' for request_type='{request_type}', "
                f"component_type='{component_type.value}'"
            )

    def get_component(self, request_type: str, component_type: ComponentType) -> Optional[ComponentMetadata]:
        """
        Get a specific component by request type and component type.

        :param request_type: The request type to look up
        :param component_type: The component type to look up
        :return: ComponentMetadata if found, None otherwise
        """
        with self._lock:
            if request_type in self._components:
                return self._components[request_type].get(component_type)
            return None

    def get_components_for_request_type(self, request_type: str) -> Dict[ComponentType, ComponentMetadata]:
        """
        Get all components for a specific request type.

        :param request_type: The request type to look up
        :return: Dictionary mapping component types to metadata
        """
        with self._lock:
            return self._components.get(request_type, {}).copy()

    def get_supported_request_types(self) -> List[str]:
        """
        Get all supported request types.

        :return: List of supported request types
        """
        with self._lock:
            return list(self._components.keys())

    def is_registered(self, request_type: str, component_type: ComponentType) -> bool:
        """
        Check if a component is registered for the given request type and component type.

        :param request_type: The request type to check
        :param component_type: The component type to check
        :return: True if registered, False otherwise
        """
        with self._lock:
            return request_type in self._components and component_type in self._components[request_type]

    def clear_registry(self) -> None:
        """
        Clear all registered components. Used primarily for testing.
        """
        with self._lock:
            self._components.clear()
            logger.debug("Registry cleared")

    def get_registry_stats(self) -> Dict[str, int]:
        """
        Get statistics about the registry.

        :return: Dictionary with registry statistics
        """
        with self._lock:
            stats = {
                "total_request_types": len(self._components),
                "total_components": sum(len(components) for components in self._components.values()),
            }
            for request_type, components in self._components.items():
                stats[f"components_for_{request_type}"] = len(components)
            return stats

    # Backward compatibility methods (deprecated)
    def register_handler(self, request_type: str, handler_type, metadata) -> None:
        """
        Backward compatibility method for register_component.

        :deprecated: Use register_component instead
        """
        logger.warning("register_handler is deprecated, use register_component instead")
        self.register_component(request_type, handler_type, metadata)

    def get_handler(self, request_type: str, handler_type):
        """
        Backward compatibility method for get_component.

        :deprecated: Use get_component instead
        """
        logger.warning("get_handler is deprecated, use get_component instead")
        return self.get_component(request_type, handler_type)

    def get_handlers_for_request_type(self, request_type: str):
        """
        Backward compatibility method for get_components_for_request_type.

        :deprecated: Use get_components_for_request_type instead
        """
        logger.warning("get_handlers_for_request_type is deprecated, use get_components_for_request_type instead")
        return self.get_components_for_request_type(request_type)


# Global registry instance
_registry_instance: Optional[ExtensionRegistry] = None
_registry_lock = threading.Lock()


def get_registry() -> ExtensionRegistry:
    """
    Get the global registry instance (singleton pattern).

    :return: The global ExtensionRegistry instance
    """
    global _registry_instance

    if _registry_instance is None:
        with _registry_lock:
            if _registry_instance is None:
                _registry_instance = ExtensionRegistry()

    return _registry_instance


def reset_registry() -> None:
    """
    Reset the global registry instance. Used primarily for testing.
    """
    global _registry_instance

    with _registry_lock:
        _registry_instance = None
