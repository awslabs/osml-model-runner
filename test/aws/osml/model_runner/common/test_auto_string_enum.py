#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.
from enum import auto
from unittest import TestCase, main

from aws.osml.model_runner.common.auto_string_enum import AutoStringEnum


class SampleEnum(str, AutoStringEnum):
    FIRST = auto()
    SECOND_VALUE = auto()


class TestAutoStringEnum(TestCase):
    def test_auto_string_values(self):
        """
        Test auto-generated values match member names.
        """
        assert SampleEnum.FIRST.value == "FIRST"
        assert SampleEnum.SECOND_VALUE.value == "SECOND_VALUE"

    def test_enum_membership(self):
        """
        Test expected enum members exist.
        """
        assert SampleEnum.FIRST in SampleEnum
        assert SampleEnum.SECOND_VALUE in SampleEnum


if __name__ == "__main__":
    main()
