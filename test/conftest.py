#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
Configuration file for pytest.

This file contains configuration settings and fixtures for the test suite.
"""

import os
import sys

# Add the src directory to the Python path so that modules can be imported
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

# Add any shared fixtures or configuration here
