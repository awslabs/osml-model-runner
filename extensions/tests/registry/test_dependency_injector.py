#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
Unit tests for DependencyInjector.
"""

import unittest
from unittest.mock import Mock

from osml_extensions.registry import DependencyInjectionError, DependencyInjector, HandlerMetadata, HandlerType


class TestDependencyInjector(unittest.TestCase):
    """Test cases for DependencyInjector."""

    def setUp(self):
        """Set up test fixtures."""
        self.injector = DependencyInjector()

    def test_create_handler_success(self):
        """Test successful handler creation with dependency injection."""
        # Create mock handler class
        mock_handler_class = Mock()
        mock_instance = Mock()
        mock_handler_class.return_value = mock_instance

        # Create metadata
        metadata = HandlerMetadata(
            name="test_handler",
            handler_class=mock_handler_class,
            handler_type=HandlerType.REGION_REQUEST_HANDLER,
            supported_endpoints=["test"],
            dependencies=["dep1", "dep2"],
            version="1.0.0",
            description="Test handler",
        )

        # Create dependencies
        dependencies = {"dep1": "value1", "dep2": "value2", "extra_dep": "extra_value"}

        # Create handler
        result = self.injector.create_handler(metadata, dependencies)

        # Verify handler was created
        self.assertEqual(result, mock_instance)
        mock_handler_class.assert_called_once()

    def test_create_handler_missing_dependencies(self):
        """Test handler creation with missing dependencies."""
        mock_handler_class = Mock()

        metadata = HandlerMetadata(
            name="test_handler",
            handler_class=mock_handler_class,
            handler_type=HandlerType.REGION_REQUEST_HANDLER,
            supported_endpoints=["test"],
            dependencies=["dep1", "dep2"],
            version="1.0.0",
            description="Test handler",
        )

        # Missing dep2
        dependencies = {"dep1": "value1"}

        with self.assertRaises(DependencyInjectionError) as context:
            self.injector.create_handler(metadata, dependencies)

        self.assertIn("Missing required dependencies", str(context.exception))
        self.assertIn("dep2", str(context.exception))

    def test_create_handler_instantiation_error(self):
        """Test handler creation with instantiation error."""
        # Create mock handler class that raises exception
        mock_handler_class = Mock()
        mock_handler_class.side_effect = ValueError("Instantiation failed")

        metadata = HandlerMetadata(
            name="test_handler",
            handler_class=mock_handler_class,
            handler_type=HandlerType.REGION_REQUEST_HANDLER,
            supported_endpoints=["test"],
            dependencies=["dep1"],
            version="1.0.0",
            description="Test handler",
        )

        dependencies = {"dep1": "value1"}

        with self.assertRaises(DependencyInjectionError) as context:
            self.injector.create_handler(metadata, dependencies)

        self.assertIn("Failed to create handler", str(context.exception))

    def test_validate_dependencies_success(self):
        """Test successful dependency validation."""
        metadata = HandlerMetadata(
            name="test_handler",
            handler_class=Mock(),
            handler_type=HandlerType.REGION_REQUEST_HANDLER,
            supported_endpoints=["test"],
            dependencies=["dep1", "dep2"],
            version="1.0.0",
            description="Test handler",
        )

        dependencies = {"dep1": "value1", "dep2": "value2", "extra_dep": "extra_value"}

        result = self.injector.validate_dependencies(metadata, dependencies)
        self.assertTrue(result)

    def test_validate_dependencies_missing(self):
        """Test dependency validation with missing dependencies."""
        metadata = HandlerMetadata(
            name="test_handler",
            handler_class=Mock(),
            handler_type=HandlerType.REGION_REQUEST_HANDLER,
            supported_endpoints=["test"],
            dependencies=["dep1", "dep2", "dep3"],
            version="1.0.0",
            description="Test handler",
        )

        dependencies = {"dep1": "value1", "dep2": "value2"}  # Missing dep3

        result = self.injector.validate_dependencies(metadata, dependencies)
        self.assertFalse(result)

    def test_validate_dependencies_empty_requirements(self):
        """Test dependency validation with no required dependencies."""
        metadata = HandlerMetadata(
            name="test_handler",
            handler_class=Mock(),
            handler_type=HandlerType.REGION_REQUEST_HANDLER,
            supported_endpoints=["test"],
            dependencies=[],
            version="1.0.0",
            description="Test handler",
        )

        dependencies = {"extra_dep": "extra_value"}

        result = self.injector.validate_dependencies(metadata, dependencies)
        self.assertTrue(result)

    def test_resolve_dependencies_success(self):
        """Test successful dependency resolution."""

        # Create a handler class with specific constructor parameters
        class TestHandler:
            def __init__(self, param1, param2, param3=None):
                self.param1 = param1
                self.param2 = param2
                self.param3 = param3

        metadata = HandlerMetadata(
            name="test_handler",
            handler_class=TestHandler,
            handler_type=HandlerType.REGION_REQUEST_HANDLER,
            supported_endpoints=["test"],
            dependencies=["param1", "param2"],
            version="1.0.0",
            description="Test handler",
        )

        context = {"param1": "value1", "param2": "value2", "param3": "value3", "extra_param": "extra_value"}

        resolved = self.injector.resolve_dependencies(metadata, context)

        # Should include all constructor parameters that are available in context
        expected = {"param1": "value1", "param2": "value2", "param3": "value3"}
        self.assertEqual(resolved, expected)

    def test_resolve_dependencies_missing_required(self):
        """Test dependency resolution with missing required parameter."""

        class TestHandler:
            def __init__(self, param1, param2):
                self.param1 = param1
                self.param2 = param2

        metadata = HandlerMetadata(
            name="test_handler",
            handler_class=TestHandler,
            handler_type=HandlerType.REGION_REQUEST_HANDLER,
            supported_endpoints=["test"],
            dependencies=["param1", "param2"],
            version="1.0.0",
            description="Test handler",
        )

        context = {"param1": "value1"}  # Missing param2

        with self.assertRaises(DependencyInjectionError) as context_manager:
            self.injector.resolve_dependencies(metadata, context)

        self.assertIn("Required dependency 'param2' not available", str(context_manager.exception))

    def test_get_constructor_parameters(self):
        """Test constructor parameter inspection."""

        class TestHandler:
            def __init__(self, param1: str, param2: int, param3=None):
                pass

        params_info = self.injector.get_constructor_parameters(TestHandler)

        self.assertIn("param1", params_info)
        self.assertIn("param2", params_info)
        self.assertIn("param3", params_info)

        # Check parameter details
        self.assertTrue(params_info["param1"]["required"])
        self.assertTrue(params_info["param2"]["required"])
        self.assertFalse(params_info["param3"]["required"])
        self.assertIsNone(params_info["param3"]["default"])

    def test_get_constructor_parameters_error(self):
        """Test constructor parameter inspection with error."""
        # Mock class that will cause inspection to fail
        mock_class = Mock()
        mock_class.__init__ = None

        params_info = self.injector.get_constructor_parameters(mock_class)
        self.assertEqual(params_info, {})

    def test_detect_circular_dependencies(self):
        """Test circular dependency detection."""
        metadata = HandlerMetadata(
            name="test_handler",
            handler_class=Mock(),
            handler_type=HandlerType.REGION_REQUEST_HANDLER,
            supported_endpoints=["test"],
            dependencies=["dep1"],
            version="1.0.0",
            description="Test handler",
        )

        context = {"dep1": "value1"}

        # This should not raise an exception for simple case
        try:
            self.injector._detect_circular_dependencies(metadata, context)
        except DependencyInjectionError:
            self.fail("_detect_circular_dependencies raised DependencyInjectionError unexpectedly")


if __name__ == "__main__":
    unittest.main()
