#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
Integration test utilities for OSML Model Runner.

This package provides utilities for integration testing including:
- Test configuration
- Result validation helpers
- Feature comparison utilities
- Load testing support
"""

from .config import OSMLConfig
from .integ_utils import (
    count_region_request_items,
    get_expected_image_feature_count,
    get_expected_region_request_count,
    validate_expected_feature_count,
    validate_expected_region_request_items,
)

__all__ = [
    # Configuration
    "OSMLConfig",
    # Legacy integration utilities (kept for backward compatibility)
    "count_region_request_items",
    "validate_expected_feature_count",
    "validate_expected_region_request_items",
    "get_expected_image_feature_count",
    "get_expected_region_request_count",
]
