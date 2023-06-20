from enum import Enum
from typing import List, Literal

from pydantic import BaseModel, Field


class AllowNullsCheck(BaseModel):
    allow_nulls: bool


class ValueIn(BaseModel):
    value_in: List[str]


SIMPLE_CHECKS = Literal["check_unique", "dimension_required"]


class DataTypes(Enum):
    STRING = "string"
    DECIMAL = "decimal"
    DATETIME = "datetime"


class DataTypeConfig(BaseModel):
    data_type: DataTypes
    check_type_friendly_name: str = Field("DataTypeCheck", const=True)

    class Config:
        frozen = True


class ChecklistObjectStatus(Enum):
    ERRORED = "errored"
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"
    PENDING = "pending"
