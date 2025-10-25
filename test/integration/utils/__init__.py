#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
Integration test utilities for OSML Model Runner.

This package provides utilities for integration testing including:
- AWS client management
- Test configuration
- Image processing utilities
- Result validation
- Load testing support
"""

from .clients import (
    cw_client,
    ddb_client,
    elb_client,
    get_all_clients,
    get_session_credentials,
    kinesis_client,
    s3_client,
    sm_client,
    sqs_client,
)
from .config import OSMLConfig, OSMLLoadTestConfig
from .integ_utils import (
    build_image_processing_request,
    count_features,
    count_region_request_items,
    monitor_job_status,
    queue_image_processing_job,
    run_model_on_image,
    validate_expected_feature_count,
    validate_expected_region_request_items,
    validate_features_match,
)

__all__ = [
    # Clients
    "get_session_credentials",
    "get_all_clients",
    "sqs_client",
    "ddb_client",
    "s3_client",
    "kinesis_client",
    "sm_client",
    "cw_client",
    "elb_client",
    # Configuration
    "OSMLConfig",
    "OSMLLoadTestConfig",
    # Integration utilities
    "run_model_on_image",
    "queue_image_processing_job",
    "monitor_job_status",
    "validate_features_match",
    "build_image_processing_request",
    "count_features",
    "validate_expected_feature_count",
    "count_region_request_items",
    "validate_expected_region_request_items",
]
