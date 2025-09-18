#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

from unittest.mock import patch

from osml_extensions.factory.enhanced_factory import EnhancedFeatureDetectorFactory

from aws.osml.model_runner.api import ModelInvokeMode


class TestEnhancedFeatureDetectorFactory:
    """Test suite for EnhancedFeatureDetectorFactory class."""

    def test_init_default_config(self):
        """Test factory initialization with default configuration."""
        with patch("osml_extensions.factory.enhanced_factory.ExtensionConfig") as mock_config:
            mock_config.use_extensions.return_value = True
            mock_config.get_extension_config.return_value = {}
            mock_config.validate_config.return_value = True

            factory = EnhancedFeatureDetectorFactory(endpoint="test-endpoint", endpoint_mode=ModelInvokeMode.SM_ENDPOINT)

            assert factory.endpoint == "test-endpoint"
            assert factory.endpoint_mode == ModelInvokeMode.SM_ENDPOINT
            assert factory.use_extensions is True
            assert factory.extension_config == {}

    def test_init_explicit_config(self):
        """Test factory initialization with explicit configuration."""
        custom_config = {"preprocessing_enabled": False}

        factory = EnhancedFeatureDetectorFactory(
            endpoint="test-endpoint",
            endpoint_mode=ModelInvokeMode.SM_ENDPOINT,
            use_extensions=False,
            extension_config=custom_config,
        )

        assert factory.use_extensions is False
        assert factory.extension_config == custom_config

    def test_init_with_credentials(self):
        """Test factory initialization with credentials."""
        credentials = {"AccessKeyId": "test-key", "SecretAccessKey": "test-secret"}

        with patch("osml_extensions.factory.enhanced_factory.ExtensionConfig") as mock_config:
            mock_config.use_extensions.return_value = True
            mock_config.get_extension_config.return_value = {}
            mock_config.validate_config.return_value = True

            factory = EnhancedFeatureDetectorFactory(
                endpoint="test-endpoint", endpoint_mode=ModelInvokeMode.SM_ENDPOINT, assumed_credentials=credentials
            )

            assert factory.assumed_credentials == credentials

    def test_init_invalid_config_fallback(self):
        """Test factory initialization with invalid config falls back to defaults."""
        invalid_config = {"invalid": "config"}

        with patch("osml_extensions.factory.enhanced_factory.ExtensionConfig") as mock_config:
            mock_config.validate_config.return_value = False

            factory = EnhancedFeatureDetectorFactory(
                endpoint="test-endpoint", endpoint_mode=ModelInvokeMode.SM_ENDPOINT, extension_config=invalid_config
            )

            assert factory.extension_config == {}

    def test_create_from_environment(self):
        """Test creating factory from environment configuration."""
        with patch("osml_extensions.factory.enhanced_factory.ExtensionConfig") as mock_config:
            mock_config.set_extension_logging.return_value = None
            mock_config.use_extensions.return_value = True
            mock_config.get_extension_config.return_value = {"env": "config"}
            mock_config.validate_config.return_value = True

            credentials = {"AccessKeyId": "test-key"}

            factory = EnhancedFeatureDetectorFactory.create_from_environment(
                endpoint="test-endpoint", endpoint_mode=ModelInvokeMode.SM_ENDPOINT, assumed_credentials=credentials
            )

            mock_config.set_extension_logging.assert_called_once()
            assert factory.endpoint == "test-endpoint"
            assert factory.endpoint_mode == ModelInvokeMode.SM_ENDPOINT
            assert factory.use_extensions is True
            assert factory.extension_config == {"env": "config"}
            assert factory.assumed_credentials == credentials

    def test_inheritance_from_feature_detector_factory(self):
        """Test that EnhancedFeatureDetectorFactory properly inherits from FeatureDetectorFactory."""
        from aws.osml.model_runner.inference.endpoint_factory import FeatureDetectorFactory

        factory = EnhancedFeatureDetectorFactory(endpoint="test-endpoint", endpoint_mode=ModelInvokeMode.SM_ENDPOINT)

        assert isinstance(factory, FeatureDetectorFactory)
        assert hasattr(factory, "endpoint")
        assert hasattr(factory, "endpoint_mode")
        assert hasattr(factory, "assumed_credentials")
        assert hasattr(factory, "build")

    def test_factory_logging(self):
        """Test that factory logs appropriate messages."""
        with patch("osml_extensions.factory.enhanced_factory.logger") as mock_logger:
            EnhancedFeatureDetectorFactory(endpoint="test-endpoint", endpoint_mode=ModelInvokeMode.SM_ENDPOINT)

            # Should log initialization
            mock_logger.info.assert_called()
