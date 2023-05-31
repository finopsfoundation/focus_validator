from enum import Enum
from itertools import groupby
from typing import List, Union

import pandera as pa
import yaml
from focus_validator.config_validators.override_validator import (
    ValidationOverrideConfig,
)
from pydantic import BaseModel, validator

from focus_validator.exceptions import FocusNotImplementedError


class ValidationConfig(BaseModel):
    check: Union[str, AllowNullsCheck, ValueIn]

    @validator("check")
    def validate_checks(cls, check):
        if isinstance(check, str):
            assert check in SIMPLE_CHECKS
            return check
        else:
            return check

    def generate_pandera_rule(self, check_name, friendly_name):
        check = self.check
        error_string = "{}: {}".format(check_name, friendly_name)

        if isinstance(check, str):
            if check == "check_unique":
                return pa.Check.check_unique(error=error_string)
            else:
                raise FocusNotImplementedError(
                    msg="Check type: {} not implemented.".format(check)
                )
        elif isinstance(check, ValueIn):
            error_string = error_string.format(", ".join(check.value_in))
            return pa.Check.check_value_in(
                allowed_values=check.value_in, error=error_string
            )
        elif isinstance(check, AllowNullsCheck):
            return pa.Check.check_not_null(error=error_string, ignore_na=False)
        else:
            raise FocusNotImplementedError(
                msg="Check type: {} not implemented.".format(type(check))
            )


class AllowNullsCheck(BaseModel):
    allow_nulls: bool


class ValueIn(BaseModel):
    value_in: List[str]


SIMPLE_CHECKS = ["check_unique"]


class DataTypes(Enum):
    STRING = "string"
    decimal = "decimal"


class DataTypeConfig(BaseModel):
    data_type: DataTypes


class Rule(BaseModel):
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
            raise FocusNotImplementedError(
                "Check for version: {} not implemented.".format(type(validation_config))
            )

    @classmethod
    def generate_schema(
            cls,
            schemas: List["CheckConfigs"],
            override_config: ValidationOverrideConfig = None,
    ):
        schema_dict = {}
        overrides = {}
        if override_config:
            overrides = set(override_config.overrides.skip)

        for dimension_name, check_configs in groupby(
                sorted(schemas, key=lambda item: item.dimension),
                key=lambda item: item.dimension,
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

    @staticmethod
    def load_yaml(rule_path):
        with open(rule_path, "r") as f:
            rule_obj = yaml.safe_load(f)
        return Rule.parse_obj(rule_obj)

    def parse_friendly_name(self):
        if "value_in" in self.validation_config["check"]:
            self.check_friendly_name = self.check_friendly_name.replace(
                "{values}", str(self.validation_config["check"]["value_in"])
            )

    def handle_overrides(self, override_config):
        if not override_config:
            return
        if self.check_id in override_config.overrides.skip:
            self.skipped = True
