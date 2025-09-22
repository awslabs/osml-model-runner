# Telling flake8 to not flag errors in this file. It is normal that these classes are imported but not used in an
# __init__.py file.
# flake8: noqa
#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
Detector extensions for the OSML Model Runner.
"""

from .async_sm_detector import AsyncSMDetector, AsyncSMDetectorBuilder

__all__ = ["AsyncSMDetector", "AsyncSMDetectorBuilder"]
