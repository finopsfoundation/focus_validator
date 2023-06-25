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
