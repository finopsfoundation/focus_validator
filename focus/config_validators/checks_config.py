from itertools import groupby
from typing import List

import pandera as pa
from pydantic import BaseModel

from focus.exceptions import FocusNotImplementedError
from .dimension_value_types import DIMENSION_VALUE_TYPE_MAP
from .override_config import ValidationOverrideConfig
from .version_0_5_config import CheckConfig05


class CheckConfigs(BaseModel):
    check_name: str
    dimension: str
    check_friendly_name: str
    validation_config: CheckConfig05

    def __process_validation_config__(self) -> pa.Check:
        validation_config = self.validation_config

        if isinstance(validation_config, CheckConfig05):
            return validation_config.generate_pandera_rule(
                check_name=self.check_name, friendly_name=self.check_friendly_name
            )
        else:
            raise FocusNotImplementedError("Check for version: {} not implemented.".format(type(validation_config)))

    @classmethod
    def generate_schema(cls, schemas: List['CheckConfigs'], override_config: ValidationOverrideConfig = None):
        schema_dict = {}
        overrides = {}
        if override_config:
            overrides = set(override_config.overrides.skip)

        for dimension_name, check_configs in groupby(
                sorted(schemas, key=lambda item: item.dimension), key=lambda item: item.dimension
        ):
            try:
                value_type = DIMENSION_VALUE_TYPE_MAP[dimension_name]
            except FocusNotImplementedError:
                raise

            check_configs: List[CheckConfigs] = list(check_configs)
            checks = []
            for check_config in check_configs:
                if check_config.check_name not in overrides:
                    check = check_config.__process_validation_config__()
                    checks.append(check)
            schema_dict[dimension_name] = pa.Column(
                value_type, required=False, checks=checks
            )
        return pa.DataFrameSchema(schema_dict, strict=False)
