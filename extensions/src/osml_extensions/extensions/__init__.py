#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
OSML Extensions Package

This package contains all extension modules for the OSML Model Runner.
Importing this package will trigger registration of all available extensions.
"""

# Import all extension modules to trigger handler registration
from . import async_workflow

__all__ = [
    "async_workflow",
]