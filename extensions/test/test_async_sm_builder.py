#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

from unittest.mock import Mock, patch

from osml_extensions.builders.async_sm_builder import AsyncSMDetectorBuilder
from osml_extensions.detectors.async_sm_detector import AsyncSMDetector


class TestAsyncSMDetectorBuilder:
    """Test suite for AsyncSMDetectorBuilder class."""

    def test_init_default_config(self):
        """Test AsyncSMDetectorBuilder initialization with default configuration."""
        builder = AsyncSMDetectorBuilder("test-endpoint")

        assert builder.endpoint == "test-endpoint"
        assert builder.assumed_credentials is None
        assert builder.custom_config == {}

    def test_init_with_credentials(self):
        """Test AsyncSMDetectorBuilder initialization with credentials."""
        credentials = {"AccessKeyId": "test-key", "SecretAccessKey": "test-secret", "SessionToken": "test-token"}

        builder = AsyncSMDetectorBuilder("test-endpoint", assumed_credentials=credentials)

        assert builder.endpoint == "test-endpoint"
        assert builder.assumed_credentials == credentials

    def test_init_with_custom_config(self):
        """Test AsyncSMDetectorBuilder initialization with custom configuration."""
        custom_config = {
            "preprocessing_enabled": False,
            "postprocessing_enabled": True,
            "timeout_multiplier": 2.0,
            "custom_parameters": {"confidence_threshold": 0.8},
        }

        builder = AsyncSMDetectorBuilder("test-endpoint", custom_config=custom_config)

        assert builder.custom_config == custom_config

    def test_build_success(self):
        """Test successful detector building."""
        builder = AsyncSMDetectorBuilder("test-endpoint")

        detector = builder.build()

        assert detector is not None
        assert isinstance(detector, AsyncSMDetector)
        assert detector.endpoint == "test-endpoint"

    def test_build_with_custom_config(self):
        """Test detector building with custom configuration."""
        custom_config = {
            "preprocessing_enabled": False,
            "postprocessing_enabled": True,
            "custom_parameters": {"confidence_threshold": 0.9},
        }

        builder = AsyncSMDetectorBuilder("test-endpoint", custom_config=custom_config)
        detector = builder.build()

        assert detector is not None
        assert isinstance(detector, AsyncSMDetector)
        assert detector.custom_config == custom_config
        assert detector.preprocessing_enabled is False
        assert detector.postprocessing_enabled is True

    def test_build_with_credentials(self):
        """Test detector building with credentials."""
        credentials = {"AccessKeyId": "test-key", "SecretAccessKey": "test-secret", "SessionToken": "test-token"}

        with patch("boto3.client"):
            builder = AsyncSMDetectorBuilder("test-endpoint", assumed_credentials=credentials)
            detector = builder.build()

            assert detector is not None
            assert isinstance(detector, AsyncSMDetector)

    def test_build_exception_fallback(self):
        """Test that build falls back to base implementation on exception."""
        builder = AsyncSMDetectorBuilder("test-endpoint")

        # Mock AsyncSMDetector to raise an exception
        with patch("osml_extensions.builders.async_sm_builder.AsyncSMDetector", side_effect=Exception("Test error")):
            with patch.object(builder.__class__.__bases__[0], "build", return_value=Mock()) as mock_super_build:
                detector = builder.build()

                # Should fall back to super().build()
                mock_super_build.assert_called_once()
                assert detector is not None

    def test_validate_config_valid(self):
        """Test configuration validation with valid config."""
        valid_config = {
            "preprocessing_enabled": True,
            "postprocessing_enabled": False,
            "timeout_multiplier": 1.5,
            "custom_parameters": {"confidence_threshold": 0.8},
        }

        builder = AsyncSMDetectorBuilder("test-endpoint", custom_config=valid_config)

        assert builder.validate_config() is True

    def test_validate_config_invalid_preprocessing(self):
        """Test configuration validation with invalid preprocessing setting."""
        invalid_config = {"preprocessing_enabled": "invalid"}  # Should be boolean

        builder = AsyncSMDetectorBuilder("test-endpoint", custom_config=invalid_config)

        assert builder.validate_config() is False

    def test_validate_config_invalid_postprocessing(self):
        """Test configuration validation with invalid postprocessing setting."""
        invalid_config = {"postprocessing_enabled": "invalid"}  # Should be boolean

        builder = AsyncSMDetectorBuilder("test-endpoint", custom_config=invalid_config)

        assert builder.validate_config() is False

    def test_validate_config_invalid_timeout_multiplier(self):
        """Test configuration validation with invalid timeout multiplier."""
        invalid_configs = [
            {"timeout_multiplier": "invalid"},  # Should be number
            {"timeout_multiplier": -1},  # Should be positive
            {"timeout_multiplier": 0},  # Should be positive
        ]

        for invalid_config in invalid_configs:
            builder = AsyncSMDetectorBuilder("test-endpoint", custom_config=invalid_config)
            assert builder.validate_config() is False

    def test_validate_config_invalid_custom_parameters(self):
        """Test configuration validation with invalid custom parameters."""
        invalid_config = {"custom_parameters": "invalid"}  # Should be dict

        builder = AsyncSMDetectorBuilder("test-endpoint", custom_config=invalid_config)

        assert builder.validate_config() is False

    def test_validate_config_invalid_confidence_threshold(self):
        """Test configuration validation with invalid confidence threshold."""
        invalid_configs = [
            {"custom_parameters": {"confidence_threshold": "invalid"}},  # Should be number
            {"custom_parameters": {"confidence_threshold": -0.1}},  # Should be >= 0
            {"custom_parameters": {"confidence_threshold": 1.1}},  # Should be <= 1
        ]

        for invalid_config in invalid_configs:
            builder = AsyncSMDetectorBuilder("test-endpoint", custom_config=invalid_config)
            assert builder.validate_config() is False

    def test_validate_config_empty(self):
        """Test configuration validation with empty config."""
        builder = AsyncSMDetectorBuilder("test-endpoint", custom_config={})

        assert builder.validate_config() is True

    def test_validate_config_exception(self):
        """Test configuration validation handles exceptions."""
        # Create a config that will cause an exception during validation
        builder = AsyncSMDetectorBuilder("test-endpoint")
        builder.custom_config = None  # This will cause TypeError in validation

        assert builder.validate_config() is False

    def test_get_default_config(self):
        """Test getting default configuration."""
        builder = AsyncSMDetectorBuilder("test-endpoint")

        default_config = builder.get_default_config()

        expected_config = {
            "preprocessing_enabled": True,
            "postprocessing_enabled": True,
            "timeout_multiplier": 1.0,
            "custom_parameters": {},
        }

        assert default_config == expected_config

    def test_merge_with_defaults_empty_config(self):
        """Test merging empty config with defaults."""
        builder = AsyncSMDetectorBuilder("test-endpoint")

        merged_config = builder.merge_with_defaults({})
        expected_config = builder.get_default_config()

        assert merged_config == expected_config

    def test_merge_with_defaults_partial_config(self):
        """Test merging partial config with defaults."""
        builder = AsyncSMDetectorBuilder("test-endpoint")

        partial_config = {"preprocessing_enabled": False, "custom_parameters": {"confidence_threshold": 0.9}}

        merged_config = builder.merge_with_defaults(partial_config)

        expected_config = {
            "preprocessing_enabled": False,  # From partial config
            "postprocessing_enabled": True,  # From defaults
            "timeout_multiplier": 1.0,  # From defaults
            "custom_parameters": {"confidence_threshold": 0.9},  # From partial config
        }

        assert merged_config == expected_config

    def test_merge_with_defaults_custom_parameters_merge(self):
        """Test merging custom parameters with defaults."""
        builder = AsyncSMDetectorBuilder("test-endpoint")

        # Set some default custom parameters
        builder.get_default_config = lambda: {
            "preprocessing_enabled": True,
            "postprocessing_enabled": True,
            "timeout_multiplier": 1.0,
            "custom_parameters": {"default_param": "default_value"},
        }

        config = {"custom_parameters": {"confidence_threshold": 0.8}}

        merged_config = builder.merge_with_defaults(config)

        expected_custom_params = {"default_param": "default_value", "confidence_threshold": 0.8}

        assert merged_config["custom_parameters"] == expected_custom_params

    def test_inheritance_from_sm_detector_builder(self):
        """Test that AsyncSMDetectorBuilder properly inherits from SMDetectorBuilder."""
        from aws.osml.model_runner.inference.sm_detector import SMDetectorBuilder

        builder = AsyncSMDetectorBuilder("test-endpoint")
        assert isinstance(builder, SMDetectorBuilder)
        assert hasattr(builder, "endpoint")
        assert hasattr(builder, "assumed_credentials")
        assert hasattr(builder, "build")

    def test_build_returns_detector_interface(self):
        """Test that build method returns object implementing Detector interface."""
        from aws.osml.model_runner.inference.detector import Detector

        builder = AsyncSMDetectorBuilder("test-endpoint")
        detector = builder.build()

        assert detector is not None
        assert isinstance(detector, Detector)
        assert hasattr(detector, "find_features")
        assert hasattr(detector, "mode")

    def test_builder_logging(self):
        """Test that builder logs appropriate messages."""
        with patch("osml_extensions.builders.async_sm_builder.logger") as mock_logger:
            builder = AsyncSMDetectorBuilder("test-endpoint")
            builder.build()

            # Should log initialization and successful build
            mock_logger.info.assert_called()
            mock_logger.debug.assert_called()
