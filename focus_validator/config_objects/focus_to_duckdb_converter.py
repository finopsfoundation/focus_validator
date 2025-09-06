import os
from itertools import groupby
from typing import Dict, List, Optional, Set, Union

import pandas as pd
import duckdb
import sqlglot
from pandera.api.pandas.types import PandasDtypeInputTypes

from focus_validator.config_objects import ChecklistObject, InvalidRule, Rule
from focus_validator.config_objects.common import (
    AllowNullsCheck,
    ChecklistObjectStatus,
    DataTypeCheck,
    DataTypes,
    FormatCheck,
    SQLQueryCheck,
    ValueComparisonCheck,
    ValueInCheck,
)
from focus_validator.config_objects.override import Override
from focus_validator.exceptions import FocusNotImplementedError

class FocusToPanderaSchemaConverter:
    @staticmethod
    def __generate_duckdb_check__(rule: Rule, check_id):
        pass
