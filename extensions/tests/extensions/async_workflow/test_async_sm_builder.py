#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import unittest
from unittest.mock import Mock, patch

from ..src.osml_extensions.detectors import AsyncSMDetector, AsyncSMDetectorBuilder
from ..src.osml_extensions.errors import ExtensionConfigurationError


class TestAsyncSMDetectorBuilder(unittest.TestCase):
    """Test cases for AsyncSMDetectorBuilder."""

    def setUp(self):
        """Set up test fixtures."""
        self.endpoint = "test-endpoint"
        self.credentials = {"access_key": "test", "secret_key": "test"}

    def test_init_with_endpoint_only(self):
        """Test builder initialization with endpoint only."""
        builder = AsyncSMDetectorBuilder(self.endpoint)

        self.assertEqual(builder.endpoint, self.endpoint)
        self.assertEqual(builder.assumed_credentials, {})

    def test_init_with_credentials(self):
        """Test builder initialization with credentials."""
        builder = AsyncSMDetectorBuilder(self.endpoint, self.credentials)

        self.assertEqual(builder.endpoint, self.endpoint)
        self.assertEqual(builder.assumed_credentials, self.credentials)

    def test_init_with_none_credentials(self):
        """Test builder initialization with None credentials."""
        builder = AsyncSMDetectorBuilder(self.endpoint, None)

        self.assertEqual(builder.endpoint, self.endpoint)
        self.assertEqual(builder.assumed_credentials, {})

    def test_validate_parameters_success(self):
        """Test successful parameter validation."""
        builder = AsyncSMDetectorBuilder(self.endpoint, self.credentials)

        # Should not raise any exception
        builder._validate_parameters()

    def test_validate_parameters_empty_endpoint(self):
        """Test parameter validation with empty endpoint."""
        builder = AsyncSMDetectorBuilder("", self.credentials)

        with self.assertRaises(ExtensionConfigurationError) as context:
            builder._validate_parameters()

        self.assertIn("Endpoint name is required", str(context.exception))

    def test_validate_parameters_none_endpoint(self):
        """Test parameter validation with None endpoint."""
        builder = AsyncSMDetectorBuilder(None, self.credentials)

        with self.assertRaises(ExtensionConfigurationError) as context:
            builder._validate_parameters()

        self.assertIn("Endpoint name is required", str(context.exception))

    def test_validate_parameters_non_string_endpoint(self):
        """Test parameter validation with non-string endpoint."""
        builder = AsyncSMDetectorBuilder(123, self.credentials)

        with self.assertRaises(ExtensionConfigurationError) as context:
            builder._validate_parameters()

        self.assertIn("Endpoint name must be a string", str(context.exception))

    def test_validate_parameters_invalid_credentials(self):
        """Test parameter validation with invalid credentials."""
        builder = AsyncSMDetectorBuilder(self.endpoint, "invalid")

        with self.assertRaises(ExtensionConfigurationError) as context:
            builder._validate_parameters()

        self.assertIn("Assumed credentials must be a dictionary", str(context.exception))

    @patch("osml_extensions.builders.async_sm_builder.AsyncSMDetector")
    def test_build_success(self, mock_detector_class):
        """Test successful detector building."""
        mock_detector = Mock(spec=AsyncSMDetector)
        mock_detector_class.return_value = mock_detector

        builder = AsyncSMDetectorBuilder(self.endpoint, self.credentials)
        result = builder.build()

        self.assertEqual(result, mock_detector)
        mock_detector_class.assert_called_once_with(endpoint=self.endpoint, assumed_credentials=self.credentials)

    def test_build_validation_error(self):
        """Test building with validation error."""
        builder = AsyncSMDetectorBuilder("", self.credentials)

        with self.assertRaises(ExtensionConfigurationError):
            builder.build()

    @patch("osml_extensions.builders.async_sm_builder.AsyncSMDetector")
    def test_build_detector_creation_error(self, mock_detector_class):
        """Test building when detector creation fails."""
        mock_detector_class.side_effect = Exception("Creation failed")

        builder = AsyncSMDetectorBuilder(self.endpoint, self.credentials)
        result = builder.build()

        # Should return None on creation failure
        self.assertIsNone(result)

    @patch("osml_extensions.builders.async_sm_builder.AsyncSMDetector")
    def test_build_with_empty_credentials(self, mock_detector_class):
        """Test building with empty credentials."""
        mock_detector = Mock(spec=AsyncSMDetector)
        mock_detector_class.return_value = mock_detector

        builder = AsyncSMDetectorBuilder(self.endpoint)
        result = builder.build()

        self.assertEqual(result, mock_detector)
        mock_detector_class.assert_called_once_with(endpoint=self.endpoint, assumed_credentials={})


if __name__ == "__main__":
    unittest.main()
