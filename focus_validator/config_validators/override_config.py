from typing import List
from pydantic import BaseModel


class DimensionOverrideConfig(BaseModel):
    skip: List[str]


class ValidationOverrideConfig(BaseModel):
    overrides: DimensionOverrideConfig
