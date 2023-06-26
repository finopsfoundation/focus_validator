from enum import Enum
from typing import List, Literal

from pydantic import BaseModel


class AllowNullsCheck(BaseModel):
    allow_nulls: bool


class ValueInCheck(BaseModel):
    value_in: List[str]


SIMPLE_CHECKS = Literal["check_unique", "dimension_required"]


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


def generate_check_friendly_name(check, dimension):
    if check == "check_unique":
        return f"{dimension}, requires unique values."
    elif check == "dimension_required":
        return f"{dimension} is a required dimension."
    elif isinstance(check, ValueInCheck):
        return (
            f"{dimension} must have a value from the list: {','.join(check.value_in)}."
        )
    elif isinstance(check, AllowNullsCheck):
        if check.allow_nulls:
            return f"{dimension} allows null values."
        else:
            return f"{dimension} does not allow null values."
    elif isinstance(check, DataTypeCheck):
        return f"{dimension} requires values of type {check.data_type.value}."
