#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
Detector extensions for the OSML Model Runner.
"""

from .async_sm_detector import AsyncSMDetector, AsyncSMDetectorBuilder

__all__ = ["AsyncSMDetector", "AsyncSMDetectorBuilder"]
