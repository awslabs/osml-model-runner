#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
Central registry for all handler extensions.
"""

import logging
import threading
from typing import Dict, List, Optional

from .handler_metadata import HandlerMetadata, HandlerType
from .errors import HandlerRegistrationError

logger = logging.getLogger(__name__)


class ExtensionRegistry:
    """Central registry for all handler extensions."""
    
    def __init__(self):
        """Initialize the extension registry."""
        self._handlers: Dict[str, Dict[HandlerType, HandlerMetadata]] = {}
        self._lock = threading.RLock()
        logger.debug("ExtensionRegistry initialized")
    
    def register_handler(self, request_type: str, handler_type: HandlerType, metadata: HandlerMetadata) -> None:
        """
        Register a handler with the registry.
        
        :param request_type: The request type (e.g., "http", "sm_endpoint", "async_sm_endpoint")
        :param handler_type: The type of handler being registered
        :param metadata: Metadata describing the handler
        :raises HandlerRegistrationError: If registration fails
        """
        if not request_type:
            raise HandlerRegistrationError("Request type cannot be empty")
        
        if not isinstance(handler_type, HandlerType):
            raise HandlerRegistrationError(f"handler_type must be a HandlerType enum, got {type(handler_type)}")
        
        if not isinstance(metadata, HandlerMetadata):
            raise HandlerRegistrationError(f"metadata must be a HandlerMetadata instance, got {type(metadata)}")
        
        with self._lock:
            if request_type not in self._handlers:
                self._handlers[request_type] = {}
            
            # Check if handler already exists for this request type and handler type
            if handler_type in self._handlers[request_type]:
                existing = self._handlers[request_type][handler_type]
                logger.warning(
                    f"Overriding existing handler for request_type='{request_type}', "
                    f"handler_type='{handler_type.value}': {existing.name} -> {metadata.name}"
                )
            
            self._handlers[request_type][handler_type] = metadata
            logger.info(
                f"Registered handler '{metadata.name}' for request_type='{request_type}', "
                f"handler_type='{handler_type.value}'"
            )
    
    def get_handler(self, request_type: str, handler_type: HandlerType) -> Optional[HandlerMetadata]:
        """
        Get a specific handler by request type and handler type.
        
        :param request_type: The request type to look up
        :param handler_type: The handler type to look up
        :return: HandlerMetadata if found, None otherwise
        """
        with self._lock:
            if request_type in self._handlers:
                return self._handlers[request_type].get(handler_type)
            return None
    
    def get_handlers_for_request_type(self, request_type: str) -> Dict[HandlerType, HandlerMetadata]:
        """
        Get all handlers for a specific request type.
        
        :param request_type: The request type to look up
        :return: Dictionary mapping handler types to metadata
        """
        with self._lock:
            return self._handlers.get(request_type, {}).copy()
    
    def get_supported_request_types(self) -> List[str]:
        """
        Get all supported request types.
        
        :return: List of supported request types
        """
        with self._lock:
            return list(self._handlers.keys())
    
    def is_registered(self, request_type: str, handler_type: HandlerType) -> bool:
        """
        Check if a handler is registered for the given request type and handler type.
        
        :param request_type: The request type to check
        :param handler_type: The handler type to check
        :return: True if registered, False otherwise
        """
        with self._lock:
            return (request_type in self._handlers and 
                    handler_type in self._handlers[request_type])
    
    def clear_registry(self) -> None:
        """
        Clear all registered handlers. Used primarily for testing.
        """
        with self._lock:
            self._handlers.clear()
            logger.debug("Registry cleared")
    
    def get_registry_stats(self) -> Dict[str, int]:
        """
        Get statistics about the registry.
        
        :return: Dictionary with registry statistics
        """
        with self._lock:
            stats = {
                "total_request_types": len(self._handlers),
                "total_handlers": sum(len(handlers) for handlers in self._handlers.values())
            }
            for request_type, handlers in self._handlers.items():
                stats[f"handlers_for_{request_type}"] = len(handlers)
            return stats


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