#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
Unit tests for ExtensionRegistry.
"""

import unittest
from unittest.mock import Mock

from osml_extensions.registry import (
    ExtensionRegistry,
    HandlerMetadata,
    HandlerRegistrationError,
    HandlerType,
    get_registry,
    reset_registry,
)


class TestExtensionRegistry(unittest.TestCase):
    """Test cases for ExtensionRegistry."""

    def setUp(self):
        """Set up test fixtures."""
        self.registry = ExtensionRegistry()

        # Create mock handler classes
        self.mock_region_handler = Mock()
        self.mock_image_handler = Mock()

        # Create test metadata
        self.region_metadata = HandlerMetadata(
            name="test_region_handler",
            handler_class=self.mock_region_handler,
            handler_type=HandlerType.REGION_REQUEST_HANDLER,
            supported_endpoints=["test"],
            dependencies=["dep1", "dep2"],
            version="1.0.0",
            description="Test region handler",
        )

        self.image_metadata = HandlerMetadata(
            name="test_image_handler",
            handler_class=self.mock_image_handler,
            handler_type=HandlerType.IMAGE_REQUEST_HANDLER,
            supported_endpoints=["test"],
            dependencies=["dep1", "dep3"],
            version="1.0.0",
            description="Test image handler",
        )

    def test_register_handler_success(self):
        """Test successful handler registration."""
        self.registry.register_handler("test_type", HandlerType.REGION_REQUEST_HANDLER, self.region_metadata)

        # Verify handler is registered
        self.assertTrue(self.registry.is_registered("test_type", HandlerType.REGION_REQUEST_HANDLER))

        # Verify handler can be retrieved
        retrieved = self.registry.get_handler("test_type", HandlerType.REGION_REQUEST_HANDLER)
        self.assertEqual(retrieved, self.region_metadata)

    def test_register_handler_invalid_request_type(self):
        """Test registration with invalid request type."""
        with self.assertRaises(HandlerRegistrationError):
            self.registry.register_handler("", HandlerType.REGION_REQUEST_HANDLER, self.region_metadata)

    def test_register_handler_invalid_handler_type(self):
        """Test registration with invalid handler type."""
        with self.assertRaises(HandlerRegistrationError):
            self.registry.register_handler("test_type", "invalid", self.region_metadata)

    def test_register_handler_invalid_metadata(self):
        """Test registration with invalid metadata."""
        with self.assertRaises(HandlerRegistrationError):
            self.registry.register_handler("test_type", HandlerType.REGION_REQUEST_HANDLER, "invalid")

    def test_register_handler_override(self):
        """Test handler override behavior."""
        # Register initial handler
        self.registry.register_handler("test_type", HandlerType.REGION_REQUEST_HANDLER, self.region_metadata)

        # Create new metadata
        new_metadata = HandlerMetadata(
            name="new_region_handler",
            handler_class=Mock(),
            handler_type=HandlerType.REGION_REQUEST_HANDLER,
            supported_endpoints=["new"],
            dependencies=[],
            version="2.0.0",
            description="New region handler",
        )

        # Register override
        self.registry.register_handler("test_type", HandlerType.REGION_REQUEST_HANDLER, new_metadata)

        # Verify new handler is registered
        retrieved = self.registry.get_handler("test_type", HandlerType.REGION_REQUEST_HANDLER)
        self.assertEqual(retrieved, new_metadata)

    def test_get_handler_not_found(self):
        """Test getting non-existent handler."""
        result = self.registry.get_handler("nonexistent", HandlerType.REGION_REQUEST_HANDLER)
        self.assertIsNone(result)

    def test_get_handlers_for_request_type(self):
        """Test getting all handlers for a request type."""
        # Register both handlers
        self.registry.register_handler("test_type", HandlerType.REGION_REQUEST_HANDLER, self.region_metadata)
        self.registry.register_handler("test_type", HandlerType.IMAGE_REQUEST_HANDLER, self.image_metadata)

        # Get all handlers
        handlers = self.registry.get_handlers_for_request_type("test_type")

        self.assertEqual(len(handlers), 2)
        self.assertEqual(handlers[HandlerType.REGION_REQUEST_HANDLER], self.region_metadata)
        self.assertEqual(handlers[HandlerType.IMAGE_REQUEST_HANDLER], self.image_metadata)

    def test_get_handlers_for_request_type_empty(self):
        """Test getting handlers for non-existent request type."""
        handlers = self.registry.get_handlers_for_request_type("nonexistent")
        self.assertEqual(handlers, {})

    def test_get_supported_request_types(self):
        """Test getting supported request types."""
        # Initially empty
        self.assertEqual(self.registry.get_supported_request_types(), [])

        # Register handlers
        self.registry.register_handler("type1", HandlerType.REGION_REQUEST_HANDLER, self.region_metadata)
        self.registry.register_handler("type2", HandlerType.IMAGE_REQUEST_HANDLER, self.image_metadata)

        # Check supported types
        supported = self.registry.get_supported_request_types()
        self.assertIn("type1", supported)
        self.assertIn("type2", supported)
        self.assertEqual(len(supported), 2)

    def test_is_registered(self):
        """Test handler registration check."""
        # Initially not registered
        self.assertFalse(self.registry.is_registered("test_type", HandlerType.REGION_REQUEST_HANDLER))

        # Register handler
        self.registry.register_handler("test_type", HandlerType.REGION_REQUEST_HANDLER, self.region_metadata)

        # Now registered
        self.assertTrue(self.registry.is_registered("test_type", HandlerType.REGION_REQUEST_HANDLER))

        # Different handler type not registered
        self.assertFalse(self.registry.is_registered("test_type", HandlerType.IMAGE_REQUEST_HANDLER))

    def test_clear_registry(self):
        """Test registry clearing."""
        # Register handlers
        self.registry.register_handler("test_type", HandlerType.REGION_REQUEST_HANDLER, self.region_metadata)
        self.registry.register_handler("test_type", HandlerType.IMAGE_REQUEST_HANDLER, self.image_metadata)

        # Verify registered
        self.assertTrue(self.registry.is_registered("test_type", HandlerType.REGION_REQUEST_HANDLER))

        # Clear registry
        self.registry.clear_registry()

        # Verify cleared
        self.assertFalse(self.registry.is_registered("test_type", HandlerType.REGION_REQUEST_HANDLER))
        self.assertEqual(self.registry.get_supported_request_types(), [])

    def test_get_registry_stats(self):
        """Test registry statistics."""
        # Initially empty
        stats = self.registry.get_registry_stats()
        self.assertEqual(stats["total_request_types"], 0)
        self.assertEqual(stats["total_handlers"], 0)

        # Register handlers
        self.registry.register_handler("type1", HandlerType.REGION_REQUEST_HANDLER, self.region_metadata)
        self.registry.register_handler("type1", HandlerType.IMAGE_REQUEST_HANDLER, self.image_metadata)
        self.registry.register_handler("type2", HandlerType.REGION_REQUEST_HANDLER, self.region_metadata)

        # Check stats
        stats = self.registry.get_registry_stats()
        self.assertEqual(stats["total_request_types"], 2)
        self.assertEqual(stats["total_handlers"], 3)
        self.assertEqual(stats["handlers_for_type1"], 2)
        self.assertEqual(stats["handlers_for_type2"], 1)


class TestGlobalRegistry(unittest.TestCase):
    """Test cases for global registry functions."""

    def setUp(self):
        """Set up test fixtures."""
        reset_registry()

    def tearDown(self):
        """Clean up after tests."""
        reset_registry()

    def test_get_registry_singleton(self):
        """Test global registry singleton behavior."""
        registry1 = get_registry()
        registry2 = get_registry()

        # Should be the same instance
        self.assertIs(registry1, registry2)

    def test_reset_registry(self):
        """Test registry reset functionality."""
        registry1 = get_registry()
        reset_registry()
        registry2 = get_registry()

        # Should be different instances after reset
        self.assertIsNot(registry1, registry2)


if __name__ == "__main__":
    unittest.main()
