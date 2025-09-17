#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

from unittest.mock import Mock, patch

from osml_extensions.detectors.async_sm_detector import AsyncSMDetector
from osml_extensions.factory.enhanced_factory import EnhancedFeatureDetectorFactory

from aws.osml.model_runner.api import ModelInvokeMode
from aws.osml.model_runner.inference.detector import Detector


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

    def test_build_with_extensions_sm_endpoint(self):
        """Test building detector with extensions for SageMaker endpoint."""
        factory = EnhancedFeatureDetectorFactory(
            endpoint="test-endpoint", endpoint_mode=ModelInvokeMode.SM_ENDPOINT, use_extensions=True
        )

        with patch.object(factory, "_build_async_sm_detector") as mock_build_async:
            mock_detector = Mock(spec=AsyncSMDetector)
            mock_build_async.return_value = mock_detector

            result = factory.build()

            mock_build_async.assert_called_once()
            assert result == mock_detector

    def test_build_with_extensions_http_endpoint(self):
        """Test building detector with extensions for HTTP endpoint (not supported)."""
        factory = EnhancedFeatureDetectorFactory(
            endpoint="test-endpoint", endpoint_mode=ModelInvokeMode.HTTP_ENDPOINT, use_extensions=True
        )

        with patch.object(factory, "_build_base_detector") as mock_build_base:
            mock_detector = Mock(spec=Detector)
            mock_build_base.return_value = mock_detector

            result = factory.build()

            mock_build_base.assert_called_once()
            assert result == mock_detector

    def test_build_without_extensions(self):
        """Test building detector without extensions."""
        factory = EnhancedFeatureDetectorFactory(
            endpoint="test-endpoint", endpoint_mode=ModelInvokeMode.SM_ENDPOINT, use_extensions=False
        )

        with patch.object(factory, "_build_base_detector") as mock_build_base:
            mock_detector = Mock(spec=Detector)
            mock_build_base.return_value = mock_detector

            result = factory.build()

            mock_build_base.assert_called_once()
            assert result == mock_detector

    def test_build_extension_failure_fallback(self):
        """Test building falls back to base when extension fails."""
        factory = EnhancedFeatureDetectorFactory(
            endpoint="test-endpoint", endpoint_mode=ModelInvokeMode.SM_ENDPOINT, use_extensions=True
        )

        with patch.object(factory, "_build_with_extensions", return_value=None):
            with patch.object(factory, "_build_base_detector") as mock_build_base:
                mock_detector = Mock(spec=Detector)
                mock_build_base.return_value = mock_detector

                result = factory.build()

                mock_build_base.assert_called_once()
                assert result == mock_detector

    def test_build_all_failure(self):
        """Test building when both extension and base fail."""
        factory = EnhancedFeatureDetectorFactory(
            endpoint="test-endpoint", endpoint_mode=ModelInvokeMode.SM_ENDPOINT, use_extensions=True
        )

        with patch.object(factory, "_build_with_extensions", return_value=None):
            with patch.object(factory, "_build_base_detector", return_value=None):
                result = factory.build()
                assert result is None

    def test_build_with_extensions_import_error(self):
        """Test building with extensions handles import errors."""
        factory = EnhancedFeatureDetectorFactory(
            endpoint="test-endpoint", endpoint_mode=ModelInvokeMode.SM_ENDPOINT, use_extensions=True
        )

        with patch.object(factory, "_build_async_sm_detector", side_effect=ImportError("Module not found")):
            with patch.object(factory, "_build_base_detector") as mock_build_base:
                mock_detector = Mock(spec=Detector)
                mock_build_base.return_value = mock_detector

                result = factory.build()

                mock_build_base.assert_called_once()
                assert result == mock_detector

    def test_build_async_sm_detector_success(self):
        """Test successful AsyncSMDetector building."""
        factory = EnhancedFeatureDetectorFactory(endpoint="test-endpoint", endpoint_mode=ModelInvokeMode.SM_ENDPOINT)

        with patch("osml_extensions.factory.enhanced_factory.AsyncSMDetectorBuilder") as mock_builder_class:
            mock_builder = Mock()
            mock_detector = Mock(spec=AsyncSMDetector)
            mock_builder.validate_config.return_value = True
            mock_builder.build.return_value = mock_detector
            mock_builder_class.return_value = mock_builder

            result = factory._build_async_sm_detector()

            mock_builder_class.assert_called_once_with(endpoint="test-endpoint", assumed_credentials=None, custom_config={})
            mock_builder.validate_config.assert_called_once()
            mock_builder.build.assert_called_once()
            assert result == mock_detector

    def test_build_async_sm_detector_invalid_config(self):
        """Test AsyncSMDetector building with invalid config."""
        factory = EnhancedFeatureDetectorFactory(endpoint="test-endpoint", endpoint_mode=ModelInvokeMode.SM_ENDPOINT)

        with patch("osml_extensions.factory.enhanced_factory.AsyncSMDetectorBuilder") as mock_builder_class:
            mock_builder = Mock()
            mock_detector = Mock(spec=AsyncSMDetector)
            mock_builder.validate_config.return_value = False
            mock_builder.get_default_config.return_value = {"default": "config"}
            mock_builder.build.return_value = mock_detector
            mock_builder_class.return_value = mock_builder

            result = factory._build_async_sm_detector()

            # Should use default config when validation fails
            assert mock_builder.custom_config == {"default": "config"}
            assert result == mock_detector

    def test_build_async_sm_detector_exception(self):
        """Test AsyncSMDetector building handles exceptions."""
        factory = EnhancedFeatureDetectorFactory(endpoint="test-endpoint", endpoint_mode=ModelInvokeMode.SM_ENDPOINT)

        with patch("osml_extensions.factory.enhanced_factory.AsyncSMDetectorBuilder", side_effect=Exception("Test error")):
            result = factory._build_async_sm_detector()
            assert result is None

    def test_build_base_detector_success(self):
        """Test successful base detector building."""
        factory = EnhancedFeatureDetectorFactory(endpoint="test-endpoint", endpoint_mode=ModelInvokeMode.SM_ENDPOINT)

        mock_detector = Mock(spec=Detector)
        with patch.object(EnhancedFeatureDetectorFactory.__bases__[0], "build", return_value=mock_detector):
            result = factory._build_base_detector()
            assert result == mock_detector

    def test_build_base_detector_exception(self):
        """Test base detector building handles exceptions."""
        factory = EnhancedFeatureDetectorFactory(endpoint="test-endpoint", endpoint_mode=ModelInvokeMode.SM_ENDPOINT)

        with patch.object(EnhancedFeatureDetectorFactory.__bases__[0], "build", side_effect=Exception("Test error")):
            result = factory._build_base_detector()
            assert result is None

    def test_get_factory_info(self):
        """Test getting factory information."""
        credentials = {"AccessKeyId": "test-key"}
        custom_config = {"preprocessing_enabled": False}

        with patch("osml_extensions.factory.enhanced_factory.ExtensionConfig") as mock_config:
            mock_config.get_config_summary.return_value = {"summary": "data"}

            factory = EnhancedFeatureDetectorFactory(
                endpoint="test-endpoint",
                endpoint_mode=ModelInvokeMode.SM_ENDPOINT,
                use_extensions=True,
                extension_config=custom_config,
                assumed_credentials=credentials,
            )

            info = factory.get_factory_info()

            expected_info = {
                "endpoint": "test-endpoint",
                "endpoint_mode": "SM_ENDPOINT",
                "use_extensions": True,
                "extension_config": custom_config,
                "has_credentials": True,
                "extension_config_summary": {"summary": "data"},
            }

            assert info == expected_info

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

    def test_is_extension_available_async_sm_detector(self):
        """Test checking if AsyncSMDetector extension is available."""
        factory = EnhancedFeatureDetectorFactory(endpoint="test-endpoint", endpoint_mode=ModelInvokeMode.SM_ENDPOINT)

        # Should return True since we can import AsyncSMDetector
        assert factory.is_extension_available("AsyncSMDetector") is True

    def test_is_extension_available_unknown(self):
        """Test checking if unknown extension is available."""
        factory = EnhancedFeatureDetectorFactory(endpoint="test-endpoint", endpoint_mode=ModelInvokeMode.SM_ENDPOINT)

        assert factory.is_extension_available("UnknownExtension") is False

    def test_is_extension_available_import_error(self):
        """Test checking extension availability with import error."""
        factory = EnhancedFeatureDetectorFactory(endpoint="test-endpoint", endpoint_mode=ModelInvokeMode.SM_ENDPOINT)

        with patch("osml_extensions.factory.enhanced_factory.AsyncSMDetector", side_effect=ImportError):
            assert factory.is_extension_available("AsyncSMDetector") is False

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
