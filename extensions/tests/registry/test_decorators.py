#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
Unit tests for registration decorators.
"""

import unittest
from unittest.mock import Mock, patch

from osml_extensions.registry import (
    register_handler,
    HandlerType,
    HandlerRegistrationError,
    get_registry,
    reset_registry
)


class TestRegisterHandlerDecorator(unittest.TestCase):
    """Test cases for @register_handler decorator."""
    
    def setUp(self):
        """Set up test fixtures."""
        reset_registry()
        self.registry = get_registry()
    
    def tearDown(self):
        """Clean up after tests."""
        reset_registry()
    
    def test_register_handler_success(self):
        """Test successful handler registration via decorator."""
        @register_handler(
            request_type="test_type",
            handler_type=HandlerType.REGION_REQUEST_HANDLER,
            name="test_handler",
            supported_endpoints=["test"],
            dependencies=["dep1"],
            version="1.0.0",
            description="Test handler"
        )
        class TestHandler:
            def __init__(self, dep1):
                self.dep1 = dep1
        
        # Verify handler was registered
        self.assertTrue(self.registry.is_registered("test_type", HandlerType.REGION_REQUEST_HANDLER))
        
        # Verify metadata
        metadata = self.registry.get_handler("test_type", HandlerType.REGION_REQUEST_HANDLER)
        self.assertEqual(metadata.name, "test_handler")
        self.assertEqual(metadata.handler_class, TestHandler)
        self.assertEqual(metadata.supported_endpoints, ["test"])
        self.assertEqual(metadata.dependencies, ["dep1"])
        self.assertEqual(metadata.version, "1.0.0")
        self.assertEqual(metadata.description, "Test handler")
    
    def test_register_handler_minimal_params(self):
        """Test handler registration with minimal parameters."""
        @register_handler(
            request_type="test_type",
            handler_type=HandlerType.REGION_REQUEST_HANDLER,
            name="minimal_handler"
        )
        class MinimalHandler:
            pass
        
        # Verify handler was registered
        self.assertTrue(self.registry.is_registered("test_type", HandlerType.REGION_REQUEST_HANDLER))
        
        # Verify default values
        metadata = self.registry.get_handler("test_type", HandlerType.REGION_REQUEST_HANDLER)
        self.assertEqual(metadata.name, "minimal_handler")
        self.assertEqual(metadata.supported_endpoints, [])
        self.assertEqual(metadata.dependencies, [])
        self.assertEqual(metadata.version, "1.0.0")
        self.assertEqual(metadata.description, "")
    
    def test_register_handler_empty_request_type(self):
        """Test registration with empty request type."""
        with self.assertRaises(HandlerRegistrationError) as context:
            @register_handler(
                request_type="",
                handler_type=HandlerType.REGION_REQUEST_HANDLER,
                name="test_handler"
            )
            class TestHandler:
                pass
        
        self.assertIn("request_type cannot be empty", str(context.exception))
    
    def test_register_handler_invalid_handler_type(self):
        """Test registration with invalid handler type."""
        with self.assertRaises(HandlerRegistrationError) as context:
            @register_handler(
                request_type="test_type",
                handler_type="invalid_type",
                name="test_handler"
            )
            class TestHandler:
                pass
        
        self.assertIn("handler_type must be a HandlerType enum", str(context.exception))
    
    def test_register_handler_empty_name(self):
        """Test registration with empty name."""
        with self.assertRaises(HandlerRegistrationError) as context:
            @register_handler(
                request_type="test_type",
                handler_type=HandlerType.REGION_REQUEST_HANDLER,
                name=""
            )
            class TestHandler:
                pass
        
        self.assertIn("name cannot be empty", str(context.exception))
    
    def test_register_handler_none_class(self):
        """Test registration with None class."""
        with self.assertRaises(HandlerRegistrationError):
            decorator = register_handler(
                request_type="test_type",
                handler_type=HandlerType.REGION_REQUEST_HANDLER,
                name="test_handler"
            )
            decorator(None)
    
    def test_register_handler_returns_class(self):
        """Test that decorator returns the original class."""
        @register_handler(
            request_type="test_type",
            handler_type=HandlerType.REGION_REQUEST_HANDLER,
            name="test_handler"
        )
        class TestHandler:
            def test_method(self):
                return "test"
        
        # Verify the class is unchanged
        self.assertEqual(TestHandler.__name__, "TestHandler")
        self.assertTrue(hasattr(TestHandler, "test_method"))
        
        # Verify we can instantiate and use the class
        instance = TestHandler()
        self.assertEqual(instance.test_method(), "test")
    
    @patch('osml_extensions.registry.decorators.logger')
    def test_register_handler_logging(self, mock_logger):
        """Test that registration logs appropriately."""
        @register_handler(
            request_type="test_type",
            handler_type=HandlerType.REGION_REQUEST_HANDLER,
            name="test_handler"
        )
        class TestHandler:
            pass
        
        # Verify success logging
        mock_logger.info.assert_called_with(
            "Successfully registered handler 'test_handler' for request_type='test_type'"
        )
    
    @patch('osml_extensions.registry.decorators.get_registry')
    def test_register_handler_registry_error(self, mock_get_registry):
        """Test registration with registry error."""
        # Mock registry to raise exception
        mock_registry = Mock()
        mock_registry.register_handler.side_effect = Exception("Registry error")
        mock_get_registry.return_value = mock_registry
        
        with self.assertRaises(HandlerRegistrationError) as context:
            @register_handler(
                request_type="test_type",
                handler_type=HandlerType.REGION_REQUEST_HANDLER,
                name="test_handler"
            )
            class TestHandler:
                pass
        
        self.assertIn("Failed to register handler 'test_handler'", str(context.exception))
    
    def test_register_multiple_handlers(self):
        """Test registering multiple handlers."""
        @register_handler(
            request_type="test_type",
            handler_type=HandlerType.REGION_REQUEST_HANDLER,
            name="region_handler"
        )
        class RegionHandler:
            pass
        
        @register_handler(
            request_type="test_type",
            handler_type=HandlerType.IMAGE_REQUEST_HANDLER,
            name="image_handler"
        )
        class ImageHandler:
            pass
        
        # Verify both handlers are registered
        self.assertTrue(self.registry.is_registered("test_type", HandlerType.REGION_REQUEST_HANDLER))
        self.assertTrue(self.registry.is_registered("test_type", HandlerType.IMAGE_REQUEST_HANDLER))
        
        # Verify correct handlers are retrieved
        region_metadata = self.registry.get_handler("test_type", HandlerType.REGION_REQUEST_HANDLER)
        image_metadata = self.registry.get_handler("test_type", HandlerType.IMAGE_REQUEST_HANDLER)
        
        self.assertEqual(region_metadata.handler_class, RegionHandler)
        self.assertEqual(image_metadata.handler_class, ImageHandler)
    
    def test_register_handler_override(self):
        """Test handler override behavior with decorator."""
        @register_handler(
            request_type="test_type",
            handler_type=HandlerType.REGION_REQUEST_HANDLER,
            name="first_handler"
        )
        class FirstHandler:
            pass
        
        @register_handler(
            request_type="test_type",
            handler_type=HandlerType.REGION_REQUEST_HANDLER,
            name="second_handler"
        )
        class SecondHandler:
            pass
        
        # Verify second handler overrode first
        metadata = self.registry.get_handler("test_type", HandlerType.REGION_REQUEST_HANDLER)
        self.assertEqual(metadata.handler_class, SecondHandler)
        self.assertEqual(metadata.name, "second_handler")


if __name__ == '__main__':
    unittest.main()