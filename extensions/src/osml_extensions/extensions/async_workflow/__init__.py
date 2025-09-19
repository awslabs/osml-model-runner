#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
OSML Extensions - Async Workflow Package

This package contains all the components for the SageMaker Async Endpoint integration.
"""

# Import all main components
from .config import AsyncEndpointConfig
from .detectors import AsyncSMDetector, AsyncSMDetectorBuilder
from .enhanced_image_handler import EnhancedImageRequestHandler

# Import handlers to trigger registration
from .enhanced_region_handler import EnhancedRegionRequestHandler
from .errors import ExtensionConfigurationError, ExtensionRuntimeError
from .metrics import AsyncMetricsTracker
from .polling import AsyncInferencePoller, AsyncInferenceTimeoutError
from .s3 import S3Manager, S3OperationError
from .utils import CleanupPolicy, ResourceManager, ResourceType
from .workers import AsyncPollingWorker, AsyncSubmissionWorker, AsyncTileWorkerPool

__all__ = [
    # Configuration
    "AsyncEndpointConfig",
    # Detectors
    "AsyncSMDetector",
    "AsyncSMDetectorBuilder",
    # S3 Management
    "S3Manager",
    "S3OperationError",
    # Polling
    "AsyncInferencePoller",
    "AsyncInferenceTimeoutError",
    # Worker Pool
    "AsyncTileWorkerPool",
    "AsyncSubmissionWorker",
    "AsyncPollingWorker",
    # Metrics
    "AsyncMetricsTracker",
    # Resource Management
    "ResourceManager",
    "CleanupPolicy",
    "ResourceType",
    # Errors
    "ExtensionRuntimeError",
    "ExtensionConfigurationError",
    # Handlers
    "EnhancedRegionRequestHandler",
    "EnhancedImageRequestHandler",
]
