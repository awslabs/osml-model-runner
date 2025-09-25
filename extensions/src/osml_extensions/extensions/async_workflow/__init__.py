# Telling flake8 to not flag errors in this file. It is normal that these classes are imported but not used in an
# __init__.py file.
# flake8: noqa
#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
OSML Extensions - Async Workflow Package

This package contains all the components for the SageMaker Async Endpoint integration.
"""

from .errors import ExtensionConfigurationError, ExtensionRuntimeError
from .async_app_config import AsyncServiceConfig
from .s3 import S3Manager, S3OperationError

# Import all main components
from .detectors import AsyncSMDetector, AsyncSMDetectorBuilder
from .enhanced_image_handler import EnhancedImageRequestHandler
from .enhanced_region_handler import EnhancedRegionRequestHandler
from .metrics import AsyncMetricsTracker

from .database import TileRequestItem, TileRequestTable
from .workers import AsyncPollingWorker, AsyncSubmissionWorker, AsyncTileWorkerPool

__all__ = [
    # Detectors
    "AsyncSMDetector",
    "AsyncSMDetectorBuilder",
    # S3 Management
    "S3Manager",
    "S3OperationError",
    # Polling
    # "AsyncInferencePoller",
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
    # Tile Tracking
    "TileRequestItem",
    "TileRequestTable",
    # Errors
    "ExtensionRuntimeError",
    "ExtensionConfigurationError",
    # Handlers
    "EnhancedRegionRequestHandler",
    "EnhancedImageRequestHandler",
]
