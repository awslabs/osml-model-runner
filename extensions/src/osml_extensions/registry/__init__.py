# Telling flake8 to not flag errors in this file. It is normal that these classes are imported but not used in an
# __init__.py file.
# flake8: noqa
#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
Extension Registry System

This module provides the core registry infrastructure for managing component extensions
in the OSML Model Runner.
"""

# Import model runners to trigger auto-registration
from . import model_runner_registry
from .decorators import register_component, register_handler  # register_handler for backward compatibility
from .errors import ExtensionRegistryError, ComponentRegistrationError, ComponentSelectionError
from .extension_registry import ExtensionRegistry, get_registry, reset_registry
from .component_metadata import ComponentMetadata, ComponentType
from .model_runner_selector import ModelRunnerSelector

__all__ = [
    # Core types
    "ComponentMetadata",
    "ComponentType",
    # Registry components
    "ExtensionRegistry",
    "ModelRunnerSelector",
    # Decorators
    "register_component",
    # Errors
    "ExtensionRegistryError",
    "ComponentRegistrationError",
    "ComponentSelectionError",
]
