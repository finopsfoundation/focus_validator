from enum import Enum
from itertools import groupby
from typing import List, Union

import pandera as pa
import yaml
from pydantic import BaseModel, validator

from focus_validator.config_objects.override import Override
from focus_validator.exceptions import FocusNotImplementedError


class AllowNullsCheck(BaseModel):
    allow_nulls: bool


class ValueIn(BaseModel):
    value_in: List[str]


SIMPLE_CHECKS = ["check_unique"]


class ValidationConfig(BaseModel):
    check: Union[str, AllowNullsCheck, ValueIn]
    check_friendly_name: str

    @validator("check")
    def validate_checks(cls, check):
        if isinstance(check, str):
            assert check in SIMPLE_CHECKS
            return check
        else:
            return check

    def parse_friendly_name(self):
        check_friendly_name = self.check_friendly_name
        if isinstance(self.check, ValueIn):
            check_friendly_name = check_friendly_name.replace(
                "{values}", ",".join(self.check.value_in)
            )
        return check_friendly_name

    def generate_pandera_rule(self, check_name):
        check = self.check
        error_string = "{}::: {}".format(check_name, self.parse_friendly_name())

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


class DataTypes(Enum):
    STRING = "string"
    DECIMAL = "decimal"


class DataTypeConfig(BaseModel):
    data_type: DataTypes


class Rule(BaseModel):
    check_id: str
    dimension: str
    validation_config: Union[ValidationConfig, DataTypeConfig]

    def __process_validation_config__(self) -> pa.Check:
        validation_config = self.validation_config

        if isinstance(validation_config, ValidationConfig):
            return validation_config.generate_pandera_rule(check_name=self.check_id)
        else:
            raise FocusNotImplementedError(
                "Check for version: {} not implemented.".format(type(validation_config))
            )

    @classmethod
    def generate_schema(
        cls,
        rules: List["Rule"],
        override_config: Override = None,
    ):
        schema_dict = {}
        overrides = {}
        if override_config:
            overrides = set(override_config.overrides)

        value_type_maps = {}
        validation_rules = []
        for rule in rules:
            if isinstance(rule.validation_config, DataTypeConfig):
                data_type = rule.validation_config.data_type
                if data_type == DataTypes.DECIMAL:
                    pandera_type = pa.Decimal
                else:
                    pandera_type = pa.String
                value_type_maps[rule.dimension] = pandera_type
            else:
                validation_rules.append(rule)

        checklist = {}
        for dimension_name, dimension_rules in groupby(
            sorted(validation_rules, key=lambda item: item.dimension),
            key=lambda item: item.dimension,
        ):
            try:
                value_type = value_type_maps[dimension_name]
            except FocusNotImplementedError:
                raise FocusNotImplementedError(
                    msg="Dimension config not implemented for: {}".format(
                        dimension_name
                    )
                )

            dimension_rules: List[Rule] = list(dimension_rules)
            checks = []
            for rule in dimension_rules:
                skipped = rule.check_id in overrides
                if not skipped:
                    check = rule.__process_validation_config__()
                    checks.append(check)
                checklist[rule.check_id] = {
                    "Check Name": rule.check_id,
                    "Friendly Name": rule.validation_config.parse_friendly_name(),
                    "Status": "Skipped" if skipped else "Pending",
                }
            schema_dict[dimension_name] = pa.Column(
                value_type, required=False, checks=checks, nullable=True
            )
        return pa.DataFrameSchema(schema_dict, strict=False), checklist

    @staticmethod
    def load_yaml(rule_path):
        with open(rule_path, "r") as f:
            rule_obj = yaml.safe_load(f)
        return Rule.parse_obj(rule_obj)
