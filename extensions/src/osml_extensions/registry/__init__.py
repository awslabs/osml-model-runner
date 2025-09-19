#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
Extension Registry System

This module provides the core registry infrastructure for managing handler extensions
in the OSML Model Runner.
"""

from .handler_metadata import HandlerMetadata, HandlerType
from .extension_registry import ExtensionRegistry, get_registry, reset_registry
from .handler_selector import HandlerSelector
from .decorators import register_handler
from .errors import (
    ExtensionRegistryError,
    HandlerRegistrationError,
    HandlerSelectionError,
    DependencyInjectionError,
)
# Import base handlers to trigger auto-registration
from . import base_handlers

__all__ = [
    # Core types
    "HandlerMetadata",
    "HandlerType",
    # Registry components
    "ExtensionRegistry",
    "HandlerSelector", 
    # Decorators
    "register_handler",
    # Errors
    "ExtensionRegistryError",
    "HandlerRegistrationError",
    "HandlerSelectionError",
    "DependencyInjectionError",
]