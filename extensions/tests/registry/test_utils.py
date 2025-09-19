#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
Test utilities for the extension registry system.
"""

from unittest.mock import Mock
from typing import Type

from osml_extensions.registry import (
    register_handler,
    HandlerType,
    HandlerMetadata,
    get_registry,
    reset_registry
)


class RegistryTestUtils:
    """Utilities for testing the extension registry."""
    
    @staticmethod
    def create_mock_handler(
        handler_type: HandlerType,
        name: str = None,
        request_type: str = "test",
        supported_endpoints: list = None,
        dependencies: list = None,
        **kwargs
    ) -> Type:
        """
        Create a mock handler class for testing.
        
        :param handler_type: Type of handler to create
        :param name: Name for the handler (auto-generated if None)
        :param request_type: Request type for registration
        :param supported_endpoints: Supported endpoints list
        :param dependencies: Dependencies list
        :param kwargs: Additional metadata parameters
        :return: Mock handler class
        """
        if name is None:
            name = f"mock_{handler_type.value}"
        
        if supported_endpoints is None:
            supported_endpoints = ["test"]
        
        if dependencies is None:
            dependencies = []
        
        # Create mock class
        mock_class = type(f"Mock{name.title().replace('_', '')}", (), {
            "__init__": lambda self, **deps: setattr(self, "_deps", deps)
        })
        
        # Register the mock handler
        @register_handler(
            request_type=request_type,
            handler_type=handler_type,
            name=name,
            supported_endpoints=supported_endpoints,
            dependencies=dependencies,
            version=kwargs.get("version", "1.0.0"),
            description=kwargs.get("description", f"Mock {name}")
        )
        class MockHandler(mock_class):
            pass
        
        return MockHandler
    
    @staticmethod
    def register_test_handlers(request_type: str = "test") -> tuple:
        """
        Register a complete set of test handlers for a request type.
        
        :param request_type: Request type to register handlers for
        :return: Tuple of (region_handler_class, image_handler_class)
        """
        region_handler = RegistryTestUtils.create_mock_handler(
            handler_type=HandlerType.REGION_REQUEST_HANDLER,
            name=f"{request_type}_region_handler",
            request_type=request_type,
            dependencies=["config", "table"]
        )
        
        image_handler = RegistryTestUtils.create_mock_handler(
            handler_type=HandlerType.IMAGE_REQUEST_HANDLER,
            name=f"{request_type}_image_handler",
            request_type=request_type,
            dependencies=["config", "queue", "region_request_handler"]
        )
        
        return region_handler, image_handler
    
    @staticmethod
    def clear_test_registry() -> None:
        """Clear the test registry."""
        reset_registry()
    
    @staticmethod
    def create_mock_dependencies() -> dict:
        """
        Create a standard set of mock dependencies for testing.
        
        :return: Dictionary of mock dependencies
        """
        return {
            "config": Mock(name="mock_config"),
            "table": Mock(name="mock_table"),
            "queue": Mock(name="mock_queue"),
            "monitor": Mock(name="mock_monitor"),
            "strategy": Mock(name="mock_strategy"),
            "utils": Mock(name="mock_utils"),
            "statistics": Mock(name="mock_statistics")
        }
    
    @staticmethod
    def create_test_metadata(
        name: str,
        handler_type: HandlerType,
        handler_class: Type = None,
        **kwargs
    ) -> HandlerMetadata:
        """
        Create test handler metadata.
        
        :param name: Handler name
        :param handler_type: Handler type
        :param handler_class: Handler class (Mock if None)
        :param kwargs: Additional metadata parameters
        :return: HandlerMetadata instance
        """
        if handler_class is None:
            handler_class = Mock()
        
        return HandlerMetadata(
            name=name,
            handler_class=handler_class,
            handler_type=handler_type,
            supported_endpoints=kwargs.get("supported_endpoints", ["test"]),
            dependencies=kwargs.get("dependencies", []),
            version=kwargs.get("version", "1.0.0"),
            description=kwargs.get("description", f"Test {name}")
        )
    
    @staticmethod
    def assert_handler_registered(
        request_type: str,
        handler_type: HandlerType,
        expected_name: str = None
    ) -> None:
        """
        Assert that a handler is registered with expected properties.
        
        :param request_type: Request type to check
        :param handler_type: Handler type to check
        :param expected_name: Expected handler name
        """
        registry = get_registry()
        
        # Check if registered
        assert registry.is_registered(request_type, handler_type), \
            f"Handler not registered for request_type='{request_type}', handler_type='{handler_type.value}'"
        
        # Check name if provided
        if expected_name:
            metadata = registry.get_handler(request_type, handler_type)
            assert metadata.name == expected_name, \
                f"Expected handler name '{expected_name}', got '{metadata.name}'"
    
    @staticmethod
    def get_registry_summary() -> dict:
        """
        Get a summary of the current registry state for debugging.
        
        :return: Dictionary with registry summary
        """
        registry = get_registry()
        stats = registry.get_registry_stats()
        
        summary = {
            "stats": stats,
            "request_types": registry.get_supported_request_types(),
            "handlers": {}
        }
        
        for request_type in summary["request_types"]:
            handlers = registry.get_handlers_for_request_type(request_type)
            summary["handlers"][request_type] = {
                handler_type.value: metadata.name
                for handler_type, metadata in handlers.items()
            }
        
        return summary