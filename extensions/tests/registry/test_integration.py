#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
Integration tests for the extension registry system.
"""

import os
import unittest
from unittest.mock import Mock, patch

from osml_extensions.registry import (
    DependencyInjector,
    HandlerSelector,
    HandlerType,
    get_registry,
    register_handler,
    reset_registry,
)


class TestRegistryIntegration(unittest.TestCase):
    """Integration tests for the complete registry system."""

    def setUp(self):
        """Set up test fixtures."""
        reset_registry()
        self.registry = get_registry()
        self.selector = HandlerSelector()
        self.injector = DependencyInjector()

    def tearDown(self):
        """Clean up after tests."""
        reset_registry()

    def test_end_to_end_handler_registration_and_selection(self):
        """Test complete flow from registration to handler creation."""

        # Define test handlers using decorators
        @register_handler(
            request_type="test_type",
            handler_type=HandlerType.REGION_REQUEST_HANDLER,
            name="test_region_handler",
            supported_endpoints=["test"],
            dependencies=["config", "table"],
            version="1.0.0",
            description="Test region handler",
        )
        class TestRegionHandler:
            def __init__(self, config, table):
                self.config = config
                self.table = table

        @register_handler(
            request_type="test_type",
            handler_type=HandlerType.IMAGE_REQUEST_HANDLER,
            name="test_image_handler",
            supported_endpoints=["test"],
            dependencies=["config", "queue", "region_request_handler"],
            version="1.0.0",
            description="Test image handler",
        )
        class TestImageHandler:
            def __init__(self, config, queue, region_request_handler):
                self.config = config
                self.queue = queue
                self.region_request_handler = region_request_handler

        # Verify handlers are registered
        self.assertTrue(self.registry.is_registered("test_type", HandlerType.REGION_REQUEST_HANDLER))
        self.assertTrue(self.registry.is_registered("test_type", HandlerType.IMAGE_REQUEST_HANDLER))

        # Select handlers
        region_metadata, image_metadata = self.selector.select_handlers(request_type="test_type")

        # Verify correct handlers selected
        self.assertEqual(region_metadata.name, "test_region_handler")
        self.assertEqual(image_metadata.name, "test_image_handler")

        # Prepare dependencies
        mock_config = Mock()
        mock_table = Mock()
        mock_queue = Mock()

        dependencies = {"config": mock_config, "table": mock_table, "queue": mock_queue}

        # Create region handler first
        region_handler = self.injector.create_handler(region_metadata, dependencies)
        self.assertIsInstance(region_handler, TestRegionHandler)
        self.assertEqual(region_handler.config, mock_config)
        self.assertEqual(region_handler.table, mock_table)

        # Add region handler to dependencies for image handler
        dependencies["region_request_handler"] = region_handler

        # Create image handler
        image_handler = self.injector.create_handler(image_metadata, dependencies)
        self.assertIsInstance(image_handler, TestImageHandler)
        self.assertEqual(image_handler.config, mock_config)
        self.assertEqual(image_handler.queue, mock_queue)
        self.assertEqual(image_handler.region_request_handler, region_handler)

    @patch.dict(os.environ, {"REQUEST_TYPE": "async_sm_endpoint"})
    def test_environment_driven_selection(self):
        """Test handler selection driven by environment variables."""

        # Register handlers for different request types
        @register_handler(request_type="http", handler_type=HandlerType.REGION_REQUEST_HANDLER, name="http_region_handler")
        class HttpRegionHandler:
            pass

        @register_handler(request_type="http", handler_type=HandlerType.IMAGE_REQUEST_HANDLER, name="http_image_handler")
        class HttpImageHandler:
            pass

        @register_handler(
            request_type="async_sm_endpoint", handler_type=HandlerType.REGION_REQUEST_HANDLER, name="async_region_handler"
        )
        class AsyncRegionHandler:
            pass

        @register_handler(
            request_type="async_sm_endpoint", handler_type=HandlerType.IMAGE_REQUEST_HANDLER, name="async_image_handler"
        )
        class AsyncImageHandler:
            pass

        # Select handlers (should use environment variable)
        region_metadata, image_metadata = self.selector.select_handlers()

        # Verify async handlers were selected based on environment
        self.assertEqual(region_metadata.name, "async_region_handler")
        self.assertEqual(image_metadata.name, "async_image_handler")

    def test_fallback_behavior(self):
        """Test fallback to base handlers when extensions fail."""

        # Register only http handlers (base handlers)
        @register_handler(request_type="http", handler_type=HandlerType.REGION_REQUEST_HANDLER, name="base_region_handler")
        class BaseRegionHandler:
            pass

        @register_handler(request_type="http", handler_type=HandlerType.IMAGE_REQUEST_HANDLER, name="base_image_handler")
        class BaseImageHandler:
            pass

        # Try to select unsupported request type
        region_metadata, image_metadata = self.selector.select_handlers(request_type="unsupported_type")

        # Should fallback to http handlers
        self.assertEqual(region_metadata.name, "base_region_handler")
        self.assertEqual(image_metadata.name, "base_image_handler")

    def test_endpoint_configuration_inference(self):
        """Test request type inference from endpoint configuration."""

        # Register handlers for different request types
        @register_handler(
            request_type="async_sm_endpoint",
            handler_type=HandlerType.REGION_REQUEST_HANDLER,
            name="async_region_handler",
            supported_endpoints=["sagemaker", "async"],
        )
        class AsyncRegionHandler:
            pass

        @register_handler(
            request_type="async_sm_endpoint",
            handler_type=HandlerType.IMAGE_REQUEST_HANDLER,
            name="async_image_handler",
            supported_endpoints=["sagemaker", "async"],
        )
        class AsyncImageHandler:
            pass

        @register_handler(
            request_type="sm_endpoint",
            handler_type=HandlerType.REGION_REQUEST_HANDLER,
            name="sm_region_handler",
            supported_endpoints=["sagemaker"],
        )
        class SmRegionHandler:
            pass

        @register_handler(
            request_type="sm_endpoint",
            handler_type=HandlerType.IMAGE_REQUEST_HANDLER,
            name="sm_image_handler",
            supported_endpoints=["sagemaker"],
        )
        class SmImageHandler:
            pass

        # Test async endpoint configuration
        async_endpoint_config = {"endpoint_name": "my-async-sagemaker-endpoint", "endpoint_type": "async_sagemaker"}

        region_metadata, image_metadata = self.selector.select_handlers(endpoint_config=async_endpoint_config)

        self.assertEqual(region_metadata.name, "async_region_handler")
        self.assertEqual(image_metadata.name, "async_image_handler")

        # Test regular sagemaker endpoint configuration
        sm_endpoint_config = {"endpoint_name": "my-sagemaker-endpoint", "endpoint_type": "sagemaker"}

        region_metadata, image_metadata = self.selector.select_handlers(endpoint_config=sm_endpoint_config)

        self.assertEqual(region_metadata.name, "sm_region_handler")
        self.assertEqual(image_metadata.name, "sm_image_handler")

    def test_multiple_request_types_coexistence(self):
        """Test that multiple request types can coexist in the registry."""

        # Register handlers for multiple request types
        request_types = ["http", "sm_endpoint", "async_sm_endpoint"]

        for request_type in request_types:

            @register_handler(
                request_type=request_type,
                handler_type=HandlerType.REGION_REQUEST_HANDLER,
                name=f"{request_type}_region_handler",
            )
            class RegionHandler:
                pass

            @register_handler(
                request_type=request_type,
                handler_type=HandlerType.IMAGE_REQUEST_HANDLER,
                name=f"{request_type}_image_handler",
            )
            class ImageHandler:
                pass

        # Verify all request types are supported
        supported_types = self.registry.get_supported_request_types()
        for request_type in request_types:
            self.assertIn(request_type, supported_types)

        # Verify we can select handlers for each request type
        for request_type in request_types:
            region_metadata, image_metadata = self.selector.select_handlers(request_type=request_type)
            self.assertEqual(region_metadata.name, f"{request_type}_region_handler")
            self.assertEqual(image_metadata.name, f"{request_type}_image_handler")

    def test_registry_statistics(self):
        """Test registry statistics collection."""

        # Register handlers for multiple request types
        @register_handler(request_type="type1", handler_type=HandlerType.REGION_REQUEST_HANDLER, name="type1_region_handler")
        class Type1RegionHandler:
            pass

        @register_handler(request_type="type1", handler_type=HandlerType.IMAGE_REQUEST_HANDLER, name="type1_image_handler")
        class Type1ImageHandler:
            pass

        @register_handler(request_type="type2", handler_type=HandlerType.REGION_REQUEST_HANDLER, name="type2_region_handler")
        class Type2RegionHandler:
            pass

        # Check statistics
        stats = self.registry.get_registry_stats()

        self.assertEqual(stats["total_request_types"], 2)
        self.assertEqual(stats["total_handlers"], 3)
        self.assertEqual(stats["handlers_for_type1"], 2)
        self.assertEqual(stats["handlers_for_type2"], 1)


if __name__ == "__main__":
    unittest.main()
