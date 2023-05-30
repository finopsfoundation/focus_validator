from enum import Enum

from pydantic import BaseModel


class DataTypes(Enum):
    STRING = "string"
    decimal = "decimal"


class DataTypeConfig(BaseModel):
    data_type: DataTypes
