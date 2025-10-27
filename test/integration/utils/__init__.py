#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
Integration test utilities for OSML Model Runner.

This package provides utilities for integration testing including:
- Test configuration
- Image processing utilities
- Result validation
- Load testing support
"""

from .config import OSMLConfig
from .integ_utils import (
    build_image_processing_request,
    count_features,
    count_region_request_items,
    monitor_job_status,
    queue_image_processing_job,
    validate_expected_feature_count,
    validate_expected_region_request_items,
    validate_features_match,
)

__all__ = [
    # Configuration
    "OSMLConfig",
    # Integration utilities
    "queue_image_processing_job",
    "monitor_job_status",
    "validate_features_match",
    "build_image_processing_request",
    "count_features",
    "validate_expected_feature_count",
    "count_region_request_items",
    "validate_expected_region_request_items",
]
