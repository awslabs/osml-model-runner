#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import os
import unittest
from unittest.mock import Mock, patch, MagicMock

from aws.osml.model_runner.api import ModelInvokeMode

from osml_extensions.api import ExtendedModelInvokeMode
from osml_extensions.errors import ExtensionRuntimeError
from osml_extensions.factory import EnhancedFeatureDetectorFactory


class TestEnhancedFeatureDetectorFactory(unittest.TestCase):
    """Test cases for EnhancedFeatureDetectorFactory."""

    def setUp(self):
        """Set up test fixtures."""
        self.endpoint = "test-endpoint"
        self.credentials = {"access_key": "test", "secret_key": "test"}

    def test_init_with_base_mode(self):
        """Test initialization with base ModelInvokeMode."""
        factory = EnhancedFeatureDetectorFactory(
            endpoint=self.endpoint,
            endpoint_mode=ModelInvokeMode.SM_ENDPOINT,
            assumed_credentials=self.credentials
        )
        
        self.assertEqual(factory.endpoint, self.endpoint)
        self.assertEqual(factory.original_endpoint_mode, ModelInvokeMode.SM_ENDPOINT)
        self.assertEqual(factory.endpoint_mode, ModelInvokeMode.SM_ENDPOINT)

    def test_init_with_extended_mode(self):
        """Test initialization with ExtendedModelInvokeMode."""
        factory = EnhancedFeatureDetectorFactory(
            endpoint=self.endpoint,
            endpoint_mode=ExtendedModelInvokeMode.SM_ENDPOINT_ASYNC,
            assumed_credentials=self.credentials
        )
        
        self.assertEqual(factory.endpoint, self.endpoint)
        self.assertEqual(factory.original_endpoint_mode, ExtendedModelInvokeMode.SM_ENDPOINT_ASYNC)
        # Should be converted to base mode for parent class
        self.assertEqual(factory.endpoint_mode, ModelInvokeMode.SM_ENDPOINT)

    def test_get_compatible_base_mode(self):
        """Test mapping of extended modes to base modes."""
        factory = EnhancedFeatureDetectorFactory(
            endpoint=self.endpoint,
            endpoint_mode=ModelInvokeMode.SM_ENDPOINT
        )
        
        base_mode = factory._get_compatible_base_mode(ExtendedModelInvokeMode.SM_ENDPOINT_ASYNC)
        self.assertEqual(base_mode, ModelInvokeMode.SM_ENDPOINT)

    @patch.dict(os.environ, {"USE_EXTENSIONS": "true"})
    def test_should_use_extensions_from_env(self):
        """Test extension usage determination from environment."""
        factory = EnhancedFeatureDetectorFactory(
            endpoint=self.endpoint,
            endpoint_mode=ModelInvokeMode.SM_ENDPOINT
        )
        
        self.assertTrue(factory._should_use_extensions())

    @patch.dict(os.environ, {"USE_EXTENSIONS": "false"})
    def test_should_not_use_extensions_from_env(self):
        """Test extension usage determination from environment."""
        factory = EnhancedFeatureDetectorFactory(
            endpoint=self.endpoint,
            endpoint_mode=ModelInvokeMode.SM_ENDPOINT
        )
        
        self.assertFalse(factory._should_use_extensions())

    def test_should_use_extensions_override(self):
        """Test extension usage override parameter."""
        factory = EnhancedFeatureDetectorFactory(
            endpoint=self.endpoint,
            endpoint_mode=ModelInvokeMode.SM_ENDPOINT,
            use_extensions=True
        )
        
        self.assertTrue(factory._should_use_extensions())

    @patch('osml_extensions.factory.enhanced_factory.super')
    def test_build_with_extensions_disabled(self, mock_super):
        """Test build method when extensions are disabled."""
        mock_detector = Mock()
        mock_super.return_value.build.return_value = mock_detector
        
        factory = EnhancedFeatureDetectorFactory(
            endpoint=self.endpoint,
            endpoint_mode=ModelInvokeMode.SM_ENDPOINT,
            use_extensions=False
        )
        
        result = factory.build()
        
        self.assertEqual(result, mock_detector)
        mock_super.return_value.build.assert_called_once()

    @patch('osml_extensions.factory.enhanced_factory.super')
    def test_build_with_base_mode_and_extensions_enabled(self, mock_super):
        """Test build method with base mode when extensions are enabled."""
        mock_detector = Mock()
        mock_super.return_value.build.return_value = mock_detector
        
        factory = EnhancedFeatureDetectorFactory(
            endpoint=self.endpoint,
            endpoint_mode=ModelInvokeMode.SM_ENDPOINT,
            use_extensions=True
        )
        
        result = factory.build()
        
        self.assertEqual(result, mock_detector)
        mock_super.return_value.build.assert_called_once()

    @patch('osml_extensions.factory.enhanced_factory.AsyncSMDetectorBuilder')
    def test_build_enhanced_detector_success(self, mock_builder_class):
        """Test successful enhanced detector creation."""
        mock_detector = Mock()
        mock_builder = Mock()
        mock_builder.build.return_value = mock_detector
        mock_builder_class.return_value = mock_builder
        
        factory = EnhancedFeatureDetectorFactory(
            endpoint=self.endpoint,
            endpoint_mode=ExtendedModelInvokeMode.SM_ENDPOINT_ASYNC,
            use_extensions=True
        )
        
        result = factory._build_enhanced_detector()
        
        self.assertEqual(result, mock_detector)
        mock_builder_class.assert_called_once_with(
            endpoint=self.endpoint,
            assumed_credentials=None
        )

    def test_build_enhanced_detector_import_error(self):
        """Test enhanced detector creation with import error."""
        factory = EnhancedFeatureDetectorFactory(
            endpoint=self.endpoint,
            endpoint_mode=ExtendedModelInvokeMode.SM_ENDPOINT_ASYNC,
            use_extensions=True
        )
        
        with patch('osml_extensions.factory.enhanced_factory.AsyncSMDetectorBuilder', side_effect=ImportError("Module not found")):
            with self.assertRaises(ExtensionRuntimeError):
                factory._build_enhanced_detector()

    @patch('osml_extensions.factory.enhanced_factory.super')
    @patch('osml_extensions.factory.enhanced_factory.AsyncSMDetectorBuilder')
    def test_build_with_enhanced_mode_success(self, mock_builder_class, mock_super):
        """Test build method with enhanced mode success."""
        mock_detector = Mock()
        mock_builder = Mock()
        mock_builder.build.return_value = mock_detector
        mock_builder_class.return_value = mock_builder
        
        factory = EnhancedFeatureDetectorFactory(
            endpoint=self.endpoint,
            endpoint_mode=ExtendedModelInvokeMode.SM_ENDPOINT_ASYNC,
            use_extensions=True
        )
        
        result = factory.build()
        
        self.assertEqual(result, mock_detector)
        mock_super.return_value.build.assert_not_called()

    @patch('osml_extensions.factory.enhanced_factory.super')
    @patch('osml_extensions.factory.enhanced_factory.AsyncSMDetectorBuilder')
    @patch.dict(os.environ, {"EXTENSION_FALLBACK_ENABLED": "true"})
    def test_build_with_enhanced_mode_fallback(self, mock_builder_class, mock_super):
        """Test build method with enhanced mode fallback on error."""
        mock_builder_class.side_effect = ImportError("Module not found")
        mock_base_detector = Mock()
        mock_super.return_value.build.return_value = mock_base_detector
        
        factory = EnhancedFeatureDetectorFactory(
            endpoint=self.endpoint,
            endpoint_mode=ExtendedModelInvokeMode.SM_ENDPOINT_ASYNC,
            use_extensions=True
        )
        
        result = factory.build()
        
        self.assertEqual(result, mock_base_detector)
        mock_super.return_value.build.assert_called_once()


if __name__ == '__main__':
    unittest.main()