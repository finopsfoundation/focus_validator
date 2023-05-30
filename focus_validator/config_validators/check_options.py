from typing import List

from pydantic import BaseModel


class AllowNullsCheck(BaseModel):
    allow_nulls: bool


class ValueIn(BaseModel):
    value_in: List[str]


SIMPLE_CHECKS = ["check_unique"]
