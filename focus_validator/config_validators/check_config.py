from itertools import groupby
from typing import List, Union

import pandera as pa
from pydantic import BaseModel

from focus_validator.config_validators.data_type_config import DataTypeConfig
from focus_validator.config_validators.dimension_value_types import DIMENSION_VALUE_TYPE_MAP
from focus_validator.config_validators.override_config import ValidationOverrideConfig
from focus_validator.config_validators.validation_config import ValidationConfig
from focus_validator.exceptions import FocusNotImplementedError


class CheckConfig(BaseModel):
    check_name: str
    dimension: str
    check_friendly_name: str
    validation_config: Union[ValidationConfig, DataTypeConfig]

    def __process_validation_config__(self) -> pa.Check:
        validation_config = self.validation_config

        if isinstance(validation_config, ValidationConfig):
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
                value_type, required=False, checks=checks, nullable=True
            )
        return pa.DataFrameSchema(schema_dict, strict=False)
