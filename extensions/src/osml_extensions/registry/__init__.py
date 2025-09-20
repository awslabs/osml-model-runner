# Telling flake8 to not flag errors in this file. It is normal that these classes are imported but not used in an
# __init__.py file.
# flake8: noqa
#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
Extension Registry System

This module provides the core registry infrastructure for managing handler extensions
in the OSML Model Runner.
"""

# Import base handlers to trigger auto-registration
from . import base_handlers
from .decorators import register_handler
from .errors import DependencyInjectionError, ExtensionRegistryError, HandlerRegistrationError, HandlerSelectionError
from .extension_registry import ExtensionRegistry, get_registry, reset_registry
from .handler_metadata import HandlerMetadata, HandlerType
from .handler_selector import HandlerSelector

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
