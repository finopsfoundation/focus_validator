from enum import Enum
from typing import List, Optional

from pydantic import BaseModel


class AllowNullsCheck(BaseModel):
    allow_nulls: bool


class ValueIn(BaseModel):
    value_in: List[str]


SIMPLE_CHECKS = ["check_unique"]


class DataTypes(Enum):
    STRING = "string"
    DECIMAL = "decimal"


class DataTypeConfig(BaseModel):
    data_type: DataTypes


class ChecklistObjectStatus(Enum):
    ERRORED = "errored"
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"
    PENDING = "pending"


class ChecklistObject(BaseModel):
    check_name: str
    dimension: str
    friendly_name: Optional[str]
    error: Optional[str]
    status: ChecklistObjectStatus
