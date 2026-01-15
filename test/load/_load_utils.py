# Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

"""
Utility helpers for Locust-based Model Runner load tests.

This file intentionally uses flat imports (no package structure) so the entire
directory can be passed to Locust:

    locust -f ./test/load --class-picker ...
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional


def safe_add_argument(parser, *args, **kwargs) -> None:
    """
    Add a CLI argument to Locust's parser, but ignore duplicates.

    When Locust is invoked with `-f <directory>`, it may import modules in a way
    that causes `events.init_command_line_parser` listeners to run multiple times.

    :param parser: Locust/argparse parser instance.
    :param args: Positional arguments forwarded to `add_argument`.
    :param kwargs: Keyword arguments forwarded to `add_argument`.
    :returns: None
    """
    try:
        parser.add_argument(*args, **kwargs)
    except argparse.ArgumentError:
        # Conflicting option string means it was already registered; ignore.
        return


def resolve_path(
    path_str: str,
    *,
    relative_to: Optional[Path] = None,
    additional_search_roots: Optional[list[Path]] = None,
) -> Path:
    """
    Resolve a path that might be absolute, relative to cwd, or relative to `relative_to`.

    This is useful in test tooling where commands may be run from different working
    directories.

    :param path_str: Path to resolve.
    :param relative_to: Optional base directory to check when `path_str` is relative.
    :param additional_search_roots: Optional list of additional base directories to check.
    :returns: A resolved `Path`. If no candidate exists, returns the original `Path`.
    """
    p = Path(path_str)
    if p.is_absolute() and p.exists():
        return p

    candidates: list[Path] = []
    candidates.append(Path.cwd() / path_str)
    if relative_to is not None:
        candidates.append(relative_to / path_str)
    if additional_search_roots:
        for root in additional_search_roots:
            candidates.append(root / path_str)

    for c in candidates:
        if c.exists():
            return c

    # Fall back to the original path (useful for error messages).
    return p


def split_s3_path(s3_path: str) -> tuple[str, str]:
    """
    Split an S3 path into bucket and key components.

    Supports `s3://...`, virtual hosted-style URLs (`bucket.s3.<region>.amazonaws.com/key`),
    and path-style formats.

    :param s3_path: S3 path or S3 URL.
    :returns: A pair of `(bucket, key)` where `key` may be empty.
    """
    # Remove any protocol prefix (s3:// or https://)
    if s3_path.startswith("s3://"):
        path = s3_path[5:]
    elif s3_path.startswith("https://"):
        path = s3_path[8:]
    else:
        path = s3_path

    # Handle virtual hosted-style URLs (bucket.s3.region.amazonaws.com/key)
    if ".s3." in path:
        parts = path.split("/", 1)
        bucket = parts[0].split(".s3.")[0]
        prefix = parts[1] if len(parts) > 1 else ""
        return bucket, prefix

    # Handle path-style URLs and s3:// paths
    parts = path.split("/", 1)
    if len(parts) == 1:
        return parts[0], ""

    return parts[0], parts[1]
