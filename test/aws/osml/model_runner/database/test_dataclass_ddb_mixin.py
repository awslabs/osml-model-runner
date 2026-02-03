#  Copyright 2025-2026 Amazon.com, Inc. or its affiliates.

from dataclasses import dataclass
from decimal import Decimal
from typing import List, Optional

import pytest

from aws.osml.model_runner.database.dataclass_ddb_mixin import DataclassDDBMixin, decimal_to_numeric, numeric_to_decimal


def test_mixin():
    @dataclass
    class DummyDataclass(DataclassDDBMixin):
        one: int = 1
        one_five: float = 1.5
        optional: Optional[str] = None

    test_dataclass = DummyDataclass()
    test_dataclass_item = test_dataclass.to_ddb_item()
    assert isinstance(test_dataclass_item["one"], Decimal)
    assert isinstance(test_dataclass_item["one_five"], Decimal)
    assert len(test_dataclass_item.keys()) == 2

    new_test_dataclass = DummyDataclass.from_ddb_item(test_dataclass_item)
    assert isinstance(new_test_dataclass, DummyDataclass)
    assert isinstance(new_test_dataclass.one, int)
    assert isinstance(new_test_dataclass.one_five, float)
    assert new_test_dataclass == test_dataclass


def test_nested_structures():
    original = {"number": 14.5, "text": "hello", "nested": {"values": [1, "text", None], "bool": True}}

    # Convert to decimals
    decimal_version = numeric_to_decimal(original)
    assert isinstance(decimal_version["number"], Decimal)
    assert isinstance(decimal_version["nested"]["values"][0], Decimal)
    assert decimal_version["nested"]["values"][1] == "text"
    assert decimal_version["nested"]["values"][2] is None
    assert decimal_version["nested"]["bool"] is True

    # Convert back to numeric
    numeric_version = decimal_to_numeric(decimal_version)
    assert numeric_version == original


def test_nested_dataclasses():
    @dataclass
    class Point(DataclassDDBMixin):
        x: float
        y: float

    @dataclass
    class Line(DataclassDDBMixin):
        a: Point
        b: Point

    @dataclass
    class MultiPoint(DataclassDDBMixin):
        count: int
        points: Optional[List[Point]] = None

    test_line_as_dict = {"a": {"x": Decimal(1.0), "y": Decimal(2.0)}, "b": {"x": Decimal(3), "y": Decimal(4)}}
    test_line = Line.from_ddb_item(test_line_as_dict)
    assert isinstance(test_line, Line)
    assert isinstance(test_line.a, Point)
    assert isinstance(test_line.b, Point)
    assert test_line.a.x == 1.0
    assert test_line.a.y == 2.0
    assert test_line.b.x == 3.0
    assert test_line.b.y == 4.0

    test_multipoint_as_dict = {
        "count": Decimal(2),
        "points": [{"x": Decimal(1.0), "y": Decimal(2.0)}, {"x": Decimal(3), "y": Decimal(4)}],
    }
    test_multipoint = MultiPoint.from_ddb_item(test_multipoint_as_dict)
    assert isinstance(test_multipoint, MultiPoint)
    assert isinstance(test_multipoint.points, List)
    assert test_multipoint.count == 2
    assert len(test_multipoint.points) == 2
    assert isinstance(test_multipoint.points[0], Point)
    assert isinstance(test_multipoint.points[1], Point)

    test_multipoint_as_dict = {"count": Decimal(0)}
    test_multipoint = MultiPoint.from_ddb_item(test_multipoint_as_dict)
    assert isinstance(test_multipoint, MultiPoint)
    assert test_multipoint.count == 0
    assert test_multipoint.points is None


def test_create_dataclass_from_dict_raises_for_non_dataclass():
    """Test create_dataclass_from_dict raises ValueError for non-dataclass"""
    from aws.osml.model_runner.database.dataclass_ddb_mixin import create_dataclass_from_dict

    # Arrange - regular class (not a dataclass)
    class RegularClass:
        def __init__(self, value):
            self.value = value

    # Act / Assert
    with pytest.raises(ValueError) as context:
        create_dataclass_from_dict(RegularClass, {"value": 10})

    # Verify error message
    assert "is not a dataclass" in str(context.value)


def test_create_dataclass_from_dict_returns_none_for_none_data():
    """Test create_dataclass_from_dict returns None when data is None"""
    from aws.osml.model_runner.database.dataclass_ddb_mixin import create_dataclass_from_dict

    @dataclass
    class TestDataclass:
        value: int

    # Act
    result = create_dataclass_from_dict(TestDataclass, None)

    # Assert
    assert result is None


def test_process_field_value_returns_none_for_none_value():
    """Test _process_field_value returns None when value is None"""
    from aws.osml.model_runner.database.dataclass_ddb_mixin import _process_field_value

    # Act
    result = _process_field_value(str, None)

    # Assert
    assert result is None


def test_process_field_value_list_without_type_args_returns_unchanged():
    """Test _process_field_value with bare List (no type args) returns value unchanged"""
    from typing import List

    from aws.osml.model_runner.database.dataclass_ddb_mixin import _process_field_value

    # Arrange - list value
    test_list = [1, "two", 3.0]

    # Act - use bare List type (no type arguments)
    result = _process_field_value(List, test_list)

    # Assert - value returned unchanged
    assert result == test_list
    assert result is test_list


def test_process_field_value_dict_without_type_args_returns_unchanged():
    """Test _process_field_value with bare Dict (no type args) returns value unchanged"""
    from typing import Dict

    from aws.osml.model_runner.database.dataclass_ddb_mixin import _process_field_value

    # Arrange - dict value
    test_dict = {"key1": 1, "key2": "value"}

    # Act - use bare Dict type (no type arguments)
    result = _process_field_value(Dict, test_dict)

    # Assert - value returned unchanged
    assert result == test_dict
    assert result is test_dict
