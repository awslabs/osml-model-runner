#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
S3 management module for OSML extensions.
"""

from .s3_manager import S3Manager, S3OperationError

__all__ = [
    "S3Manager",
    "S3OperationError"
]