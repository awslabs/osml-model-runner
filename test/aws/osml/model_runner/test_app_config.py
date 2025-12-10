#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

import logging
import os
from importlib import reload
from unittest.mock import patch

import pytest

import aws.osml.model_runner.app_config


class TestServiceConfigValidation:
    """
    Test suite for ServiceConfig validation logic.
    """

    @pytest.fixture(autouse=True)
    def setup_env(self):
        """
        Set up required environment variables for ServiceConfig initialization.
        """
        env_vars = {
            "AWS_DEFAULT_REGION": "us-west-2",
            "IMAGE_REQUEST_TABLE": "test-image-request-table",
            "OUTSTANDING_IMAGE_REQUEST_TABLE": "test-outstanding-table",
            "REGION_REQUEST_TABLE": "test-region-request-table",
            "ENDPOINT_TABLE": "test-endpoint-table",
            "FEATURE_TABLE": "test-feature-table",
            "IMAGE_QUEUE": "test-image-queue",
            "IMAGE_DLQ": "test-image-dlq",
            "REGION_QUEUE": "test-region-queue",
            "WORKERS_PER_CPU": "2",
            "WORKERS": "4",
        }
        with patch.dict(os.environ, env_vars, clear=False):
            yield

    def test_valid_capacity_target_percentage_values(self):
        """
        Test that valid capacity_target_percentage values (0.8, 1.0, 1.2) are accepted.
        """
        test_values = [0.8, 1.0, 1.2]

        for value in test_values:
            with patch.dict(os.environ, {"CAPACITY_TARGET_PERCENTAGE": str(value)}, clear=False):
                reload(aws.osml.model_runner.app_config)
                from aws.osml.model_runner.app_config import ServiceConfig

                config = ServiceConfig()
                assert config.capacity_target_percentage == value

    def test_invalid_capacity_target_percentage_defaults_with_warning(self, caplog):
        """
        Test that invalid capacity_target_percentage (0.0, -0.5) defaults to 1.0 with warning.
        """
        test_values = [0.0, -0.5]

        for value in test_values:
            with patch.dict(os.environ, {"CAPACITY_TARGET_PERCENTAGE": str(value)}, clear=False):
                reload(aws.osml.model_runner.app_config)
                from aws.osml.model_runner.app_config import ServiceConfig

                with caplog.at_level(logging.WARNING):
                    config = ServiceConfig()

                assert config.capacity_target_percentage == 1.0
                assert any(
                    "Invalid capacity_target_percentage" in record.message and "Defaulting to 1.0" in record.message
                    for record in caplog.records
                )
                caplog.clear()

    def test_invalid_default_instance_concurrency_defaults_with_warning(self, caplog):
        """
        Test that invalid default_instance_concurrency (0, -1) defaults to 2 with warning.
        """
        test_values = [0, -1]

        for value in test_values:
            with patch.dict(os.environ, {"DEFAULT_INSTANCE_CONCURRENCY": str(value)}, clear=False):
                reload(aws.osml.model_runner.app_config)
                from aws.osml.model_runner.app_config import ServiceConfig

                with caplog.at_level(logging.WARNING):
                    config = ServiceConfig()

                assert config.default_instance_concurrency == 2
                assert any(
                    "Invalid default_instance_concurrency" in record.message and "Defaulting to 2" in record.message
                    for record in caplog.records
                )
                caplog.clear()

    def test_invalid_tile_workers_per_instance_defaults_with_warning(self, caplog):
        """
        Test that invalid tile_workers_per_instance (0, -1) defaults to 4 with warning.
        """
        test_values = [0, -1]

        for value in test_values:
            with patch.dict(os.environ, {"TILE_WORKERS_PER_INSTANCE": str(value)}, clear=False):
                reload(aws.osml.model_runner.app_config)
                from aws.osml.model_runner.app_config import ServiceConfig

                with caplog.at_level(logging.WARNING):
                    config = ServiceConfig()

                assert config.tile_workers_per_instance == 4
                assert any(
                    "Invalid tile_workers_per_instance" in record.message and "Defaulting to 4" in record.message
                    for record in caplog.records
                )
                caplog.clear()

    def test_default_values_when_env_vars_not_set(self):
        """
        Test that default values are used when environment variables are not set.
        """
        # Ensure the capacity-based throttling env vars are not set
        env_to_remove = [
            "SCHEDULER_THROTTLING_ENABLED",
            "DEFAULT_INSTANCE_CONCURRENCY",
            "DEFAULT_HTTP_ENDPOINT_CONCURRENCY",
            "TILE_WORKERS_PER_INSTANCE",
            "CAPACITY_TARGET_PERCENTAGE",
        ]

        env_copy = os.environ.copy()
        for var in env_to_remove:
            env_copy.pop(var, None)

        with patch.dict(os.environ, env_copy, clear=True):
            reload(aws.osml.model_runner.app_config)
            from aws.osml.model_runner.app_config import ServiceConfig

            config = ServiceConfig()

            assert config.scheduler_throttling_enabled is True
            assert config.default_instance_concurrency == 2
            assert config.default_http_endpoint_concurrency == 10
            assert config.tile_workers_per_instance == 4
            assert config.capacity_target_percentage == 1.0
