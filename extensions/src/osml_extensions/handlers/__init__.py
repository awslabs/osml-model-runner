#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
Handler extensions for the OSML Model Runner.
"""

from .enhanced_image_handler import EnhancedImageRequestHandler
from .enhanced_region_handler import EnhancedRegionRequestHandler

__all__ = ["EnhancedImageRequestHandler", "EnhancedRegionRequestHandler"]