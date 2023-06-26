from enum import Enum
from typing import List, Literal

from pydantic import BaseModel


class AllowNullsCheck(BaseModel):
    allow_nulls: bool


class ValueInCheck(BaseModel):
    value_in: List[str]


SIMPLE_CHECKS = Literal["check_unique", "column_required"]


class DataTypes(Enum):
    STRING = "string"
    DECIMAL = "decimal"
    DATETIME = "datetime"
    CURRENCY_CODE = "currency-code"


class DataTypeCheck(BaseModel):
    data_type: DataTypes


class ChecklistObjectStatus(Enum):
    ERRORED = "errored"
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"
    PENDING = "pending"


def generate_check_friendly_name(check, column):
    if check == "check_unique":
        return f"{column}, requires unique values."
    elif check == "column_required":
        return f"{column} is a required column."
    elif isinstance(check, ValueInCheck):
        return f"{column} must have a value from the list: {','.join(check.value_in)}."
    elif isinstance(check, AllowNullsCheck):
        if check.allow_nulls:
            return f"{column} allows null values."
        else:
            return f"{column} does not allow null values."
    elif isinstance(check, DataTypeCheck):
        return f"{column} requires values of type {check.data_type.value}."
