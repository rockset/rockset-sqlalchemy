from unittest import mock

import pytest
import rockset

from rockset_sqlalchemy.sqlalchemy import dialect
from rockset_sqlalchemy.sqlalchemy.types import Timestamp, String, Object, NullType


@pytest.fixture
def columns_from_describe_results():
    """Schema results based on Person in example.py"""
    return [
        (["_event_time"], "timestamp", 8, 8),
        (["_id"], "string", 8, 8),
        (["info"], "object", 8, 8),
        (["name"], "string", 8, 8),
    ]


@pytest.fixture
def columns_from_select_row_results():
    """Schema results based on Person in example.py"""
    return [
        ("_id", "string", None, None, None, None, False),
        ("_event_time", "string", None, None, None, None, True),
        ("_meta", "null", None, None, None, None, True),
        ("name", "string", None, None, None, None, True),
        ("info", "object", None, None, None, None, True),
    ]


def test_get_columns_with_describe_returns_expected_structure(
    engine, columns_from_describe_results
):
    with mock.patch(
        "rockset_sqlalchemy.cursor.Cursor.execute_query", mock.Mock()
    ) as _, mock.patch(
        "rockset_sqlalchemy.sqlalchemy.dialect.RocksetDialect._validate_query",
        mock.Mock(),
    ) as _, mock.patch(
        "rockset_sqlalchemy.sqlalchemy.dialect.RocksetDialect._exec_query",
        mock.Mock(return_value=columns_from_describe_results),
    ) as _:
        expected_results = [
            {
                "name": "_event_time",
                "type": Timestamp,
                "nullable": True,
                "default": None,
            },
            {"name": "_id", "type": String, "nullable": False, "default": None},
            {"name": "info", "type": Object, "nullable": True, "default": None},
            {"name": "name", "type": String, "nullable": True, "default": None},
        ]

        rs_dialect = dialect.RocksetDialect()
        columns = rs_dialect.get_columns(engine, "people")
        assert expected_results == columns


def test_get_columns_falls_back_on_rockset_exception_to_select_one_row(
    engine, columns_from_select_row_results
):
    with mock.patch(
        "rockset_sqlalchemy.sqlalchemy.dialect.RocksetDialect._get_table_columns_describe",
        mock.Mock(side_effect=rockset.exceptions.RocksetException()),
    ) as _, mock.patch(
        "rockset_sqlalchemy.sqlalchemy.dialect.RocksetDialect._exec_query_description",
        mock.Mock(return_value=columns_from_select_row_results),
    ) as _:
        expected_results = [
            {"name": "_id", "type": String, "nullable": False, "default": None},
            {"name": "_event_time", "type": String, "nullable": True, "default": None},
            {
                "name": "_meta",
                "type": NullType,
                "nullable": True,
                "default": None,
            },
            {"name": "name", "type": String, "nullable": True, "default": None},
            {"name": "info", "type": Object, "nullable": True, "default": None},
        ]

        rs_dialect = dialect.RocksetDialect()
        columns = rs_dialect.get_columns(engine, "people")
        assert expected_results == columns
