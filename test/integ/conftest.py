#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
Pytest configuration for integration tests.

This module provides pytest fixtures and configuration specific to integration tests.
"""

import os

import pytest


def pytest_configure(config):
    """
    Configure pytest for integration tests.

    Adds custom markers for integration test categorization.
    """
    config.addinivalue_line(
        "markers",
        "integration: marks tests as integration tests (deselect with '-m \"not integration\"')",
    )
    config.addinivalue_line(
        "markers",
        "slow: marks tests as slow running (deselect with '-m \"not slow\"')",
    )


@pytest.fixture(scope="session")
def verbose() -> bool:
    """
    Pytest fixture for verbose logging control.

    Can be controlled via INTEG_TEST_VERBOSE environment variable or pytest -v flag.

    :returns: True if verbose logging should be enabled.
    """
    return os.environ.get("INTEG_TEST_VERBOSE", "false").lower() == "true"
