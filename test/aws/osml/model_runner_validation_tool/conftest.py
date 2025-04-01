#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

import os

import pytest


@pytest.fixture(autouse=True)
def setup_test_environment():
    """Set up environment variables required for tests"""
    # Store original environment variables
    original_env = {}
    for key in ["SAGEMAKER_EXECUTION_ROLE_ARN", "REPORT_BUCKET"]:
        if key in os.environ:
            original_env[key] = os.environ[key]

    # Set environment variables for tests
    os.environ["SAGEMAKER_EXECUTION_ROLE_ARN"] = "test-role-arn"
    os.environ["REPORT_BUCKET"] = "test-bucket"

    yield

    # Restore original environment variables
    for key in ["SAGEMAKER_EXECUTION_ROLE_ARN", "REPORT_BUCKET"]:
        if key in original_env:
            os.environ[key] = original_env[key]
        elif key in os.environ:
            del os.environ[key]
