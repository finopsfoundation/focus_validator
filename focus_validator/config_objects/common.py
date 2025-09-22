import json
import os
from enum import Enum
from typing import Dict, List, Literal, Union

import sqlglot
from pydantic import BaseModel, field_validator


class AllowNullsCheck(BaseModel):
    allow_nulls: bool


class ValueInCheck(BaseModel):
    value_in: List[str]


class SQLQueryCheck(BaseModel):
    sql_query: str

    @field_validator("sql_query")
    def check_sql_query(cls, sql_query):
        returned_columns = [
            column.alias
            for column in sqlglot.parse_one(sql_query).find_all(sqlglot.exp.Alias)
        ]

        assert returned_columns == [
            "check_output"
        ], "SQL query must only return a column called 'check_output'"
        return sql_query


class ValueComparisonCheck(BaseModel):
    # Handles CheckNotValue, CheckValue, CheckGreaterOrEqualThanValue, CheckSameValue, CheckNotSameValue
    operator: Literal["equals", "not_equals", "greater_equal", "not_equals_column", "equals_column"]
    value: Union[str, float, int, None]


class FormatCheck(BaseModel):
    # Handles FormatNumeric, FormatDateTime, FormatBillingCurrencyCode, etc
    format_type: Literal["numeric", "datetime", "currency_code", "string", "key_value", "unit"]


class DistinctCountCheck(BaseModel):
    # Handles CheckDistinctCount
    column_a_name: str
    column_b_name: str
    expected_count: int


class ConformanceRuleCheck(BaseModel):
    # Handles CheckConformanceRule
    conformance_rule_id: str


class ColumnComparisonCheck(BaseModel):
    # Handles ColumnByColumnEqualsColumnValue
    comparison_column: str
    operator: Literal["equals", "not_equals"]


SIMPLE_CHECKS = Literal["check_unique", "column_required"]


class DataTypes(Enum):
    STRING = "string"
    DECIMAL = "decimal"
    DATETIME = "datetime"
    CURRENCY_CODE = "currency-code"
    STRINGIFIED_JSON_OBJECT = "stringified-json-object"


class DataTypeCheck(BaseModel):
    data_type: DataTypes


class ChecklistObjectStatus(Enum):
    ERRORED = "errored"
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"
    PENDING = "pending"


def generate_check_friendly_name(check, column_id, description=None):
    if description:
        return description.strip()
    return "Rule that does something"
