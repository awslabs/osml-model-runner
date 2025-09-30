#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
Exception classes for the extension registry system.
"""


class ExtensionRegistryError(Exception):
    """Base exception for registry errors."""

    pass


class ComponentRegistrationError(ExtensionRegistryError):
    """Raised when component registration fails."""

    pass


class ComponentSelectionError(ExtensionRegistryError):
    """Raised when no suitable component can be selected."""

    pass
