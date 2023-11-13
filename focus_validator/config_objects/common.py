from enum import Enum
from typing import List, Literal

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


def generate_check_friendly_name(check, column_id):
    if check == "check_unique":
        return f"{column_id}, requires unique values."
    elif check == "column_required":
        return f"{column_id} is a required column."
    elif isinstance(check, ValueInCheck):
        return (
            f"{column_id} must have a value from the list: {','.join(check.value_in)}."
        )
    elif isinstance(check, AllowNullsCheck):
        if check.allow_nulls:
            return f"{column_id} allows null values."
        else:
            return f"{column_id} does not allow null values."
    elif isinstance(check, DataTypeCheck):
        return f"{column_id} requires values of type {check.data_type.value}."
