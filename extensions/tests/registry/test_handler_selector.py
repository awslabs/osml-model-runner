#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
Unit tests for HandlerSelector.
"""

import os
import unittest
from unittest.mock import Mock, patch

from osml_extensions.registry import (
    HandlerSelector,
    ExtensionRegistry,
    HandlerMetadata,
    HandlerType,
    HandlerSelectionError,
    reset_registry
)


class TestHandlerSelector(unittest.TestCase):
    """Test cases for HandlerSelector."""
    
    def setUp(self):
        """Set up test fixtures."""
        reset_registry()
        self.selector = HandlerSelector()
        self.registry = self.selector.registry
        
        # Create mock handler classes
        self.mock_region_handler = Mock()
        self.mock_image_handler = Mock()
        
        # Create test metadata
        self.region_metadata = HandlerMetadata(
            name="test_region_handler",
            handler_class=self.mock_region_handler,
            handler_type=HandlerType.REGION_REQUEST_HANDLER,
            supported_endpoints=["sagemaker", "async"],
            dependencies=["dep1"],
            version="1.0.0",
            description="Test region handler"
        )
        
        self.image_metadata = HandlerMetadata(
            name="test_image_handler",
            handler_class=self.mock_image_handler,
            handler_type=HandlerType.IMAGE_REQUEST_HANDLER,
            supported_endpoints=["sagemaker", "async"],
            dependencies=["dep1"],
            version="1.0.0",
            description="Test image handler"
        )
        
        # Register test handlers
        self.registry.register_handler("test_type", HandlerType.REGION_REQUEST_HANDLER, self.region_metadata)
        self.registry.register_handler("test_type", HandlerType.IMAGE_REQUEST_HANDLER, self.image_metadata)
    
    def tearDown(self):
        """Clean up after tests."""
        reset_registry()
    
    def test_select_handlers_explicit_request_type(self):
        """Test handler selection with explicit request type."""
        region_handler, image_handler = self.selector.select_handlers(request_type="test_type")
        
        self.assertEqual(region_handler, self.region_metadata)
        self.assertEqual(image_handler, self.image_metadata)
    
    def test_select_handlers_unsupported_request_type(self):
        """Test handler selection with unsupported request type."""
        # Register http handlers for fallback
        http_region_metadata = HandlerMetadata(
            name="http_region_handler",
            handler_class=Mock(),
            handler_type=HandlerType.REGION_REQUEST_HANDLER,
            supported_endpoints=["http"],
            dependencies=[],
            version="1.0.0",
            description="HTTP region handler"
        )
        
        http_image_metadata = HandlerMetadata(
            name="http_image_handler",
            handler_class=Mock(),
            handler_type=HandlerType.IMAGE_REQUEST_HANDLER,
            supported_endpoints=["http"],
            dependencies=[],
            version="1.0.0",
            description="HTTP image handler"
        )
        
        self.registry.register_handler("http", HandlerType.REGION_REQUEST_HANDLER, http_region_metadata)
        self.registry.register_handler("http", HandlerType.IMAGE_REQUEST_HANDLER, http_image_metadata)
        
        # Request unsupported type, should fallback to http
        region_handler, image_handler = self.selector.select_handlers(request_type="unsupported")
        
        self.assertEqual(region_handler, http_region_metadata)
        self.assertEqual(image_handler, http_image_metadata)
    
    def test_select_handlers_missing_region_handler(self):
        """Test handler selection with missing region handler."""
        # Remove region handler
        self.registry.clear_registry()
        self.registry.register_handler("test_type", HandlerType.IMAGE_REQUEST_HANDLER, self.image_metadata)
        
        with self.assertRaises(HandlerSelectionError) as context:
            self.selector.select_handlers(request_type="test_type")
        
        self.assertIn("No region request handler found", str(context.exception))
    
    def test_select_handlers_missing_image_handler(self):
        """Test handler selection with missing image handler."""
        # Remove image handler
        self.registry.clear_registry()
        self.registry.register_handler("test_type", HandlerType.REGION_REQUEST_HANDLER, self.region_metadata)
        
        with self.assertRaises(HandlerSelectionError) as context:
            self.selector.select_handlers(request_type="test_type")
        
        self.assertIn("No image request handler found", str(context.exception))
    
    @patch.dict(os.environ, {"REQUEST_TYPE": "env_test_type"})
    def test_determine_request_type_from_env(self):
        """Test request type determination from environment variable."""
        result = self.selector._determine_request_type(None, None, None)
        self.assertEqual(result, "env_test_type")
    
    def test_determine_request_type_explicit(self):
        """Test request type determination with explicit parameter."""
        result = self.selector._determine_request_type("explicit_type", None, None)
        self.assertEqual(result, "explicit_type")
    
    def test_determine_request_type_from_endpoint_config(self):
        """Test request type determination from endpoint configuration."""
        endpoint_config = {"endpoint_name": "async-sagemaker-endpoint", "endpoint_type": "async"}
        result = self.selector._determine_request_type(None, None, endpoint_config)
        self.assertEqual(result, "async_sm_endpoint")
        
        endpoint_config = {"endpoint_name": "sagemaker-endpoint", "endpoint_type": "sm"}
        result = self.selector._determine_request_type(None, None, endpoint_config)
        self.assertEqual(result, "sm_endpoint")
        
        endpoint_config = {"endpoint_name": "http-endpoint", "endpoint_type": "rest"}
        result = self.selector._determine_request_type(None, None, endpoint_config)
        self.assertEqual(result, "http")
    
    def test_determine_request_type_extensions_disabled(self):
        """Test request type determination with extensions disabled."""
        config = Mock()
        config.use_extensions = False
        
        result = self.selector._determine_request_type(None, config, None)
        self.assertEqual(result, "http")
    
    def test_determine_request_type_default(self):
        """Test request type determination with default fallback."""
        result = self.selector._determine_request_type(None, None, None)
        self.assertEqual(result, "http")
    
    def test_infer_request_type_from_endpoint(self):
        """Test request type inference from endpoint configuration."""
        # Test async endpoint
        endpoint_config = {"endpoint_name": "my-async-endpoint"}
        result = self.selector._infer_request_type_from_endpoint(endpoint_config)
        self.assertEqual(result, "async_sm_endpoint")
        
        # Test sagemaker endpoint
        endpoint_config = {"endpoint_type": "sagemaker"}
        result = self.selector._infer_request_type_from_endpoint(endpoint_config)
        self.assertEqual(result, "sm_endpoint")
        
        # Test http endpoint
        endpoint_config = {"endpoint_name": "rest-api"}
        result = self.selector._infer_request_type_from_endpoint(endpoint_config)
        self.assertEqual(result, "http")
    
    def test_validate_request_type_support(self):
        """Test request type support validation."""
        # Supported type
        self.assertTrue(self.selector._validate_request_type_support("test_type"))
        
        # Unsupported type
        self.assertFalse(self.selector._validate_request_type_support("unsupported"))
    
    def test_validate_endpoint_compatibility(self):
        """Test endpoint compatibility validation."""
        # Compatible endpoint
        endpoint_config = {"endpoint_type": "sagemaker", "endpoint_name": "test-endpoint"}
        self.assertTrue(self.selector._validate_endpoint_compatibility(self.region_metadata, endpoint_config))
        
        # Incompatible endpoint
        endpoint_config = {"endpoint_type": "bedrock", "endpoint_name": "bedrock-endpoint"}
        self.assertFalse(self.selector._validate_endpoint_compatibility(self.region_metadata, endpoint_config))
        
        # Handler with no supported endpoints (should be compatible)
        metadata_no_endpoints = HandlerMetadata(
            name="generic_handler",
            handler_class=Mock(),
            handler_type=HandlerType.REGION_REQUEST_HANDLER,
            supported_endpoints=[],
            dependencies=[],
            version="1.0.0",
            description="Generic handler"
        )
        
        self.assertTrue(self.selector._validate_endpoint_compatibility(metadata_no_endpoints, endpoint_config))
    
    def test_select_handlers_with_endpoint_validation(self):
        """Test handler selection with endpoint compatibility validation."""
        endpoint_config = {"endpoint_type": "sagemaker", "endpoint_name": "test-endpoint"}
        
        # Should succeed with compatible endpoint
        region_handler, image_handler = self.selector.select_handlers(
            request_type="test_type",
            endpoint_config=endpoint_config
        )
        
        self.assertEqual(region_handler, self.region_metadata)
        self.assertEqual(image_handler, self.image_metadata)


if __name__ == '__main__':
    unittest.main()