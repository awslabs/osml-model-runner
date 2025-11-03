#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

from enum import Enum


class AutoStringEnum(Enum):
    """
    An enum that automatically generates string values from member names.

    This enum automatically assigns the member name as the string value,
    eliminating the need to manually specify string values for each member.
    """

    @staticmethod
    def _generate_next_value_(name, start, count, last_values) -> str:
        return name
