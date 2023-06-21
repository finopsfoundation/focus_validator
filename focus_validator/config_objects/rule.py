from itertools import groupby
from typing import List, Union, Optional

import pandera as pa
import yaml
from pydantic import BaseModel, validator

from focus_validator.config_objects.common import (
    ValueIn,
    AllowNullsCheck,
    SIMPLE_CHECKS,
    DataTypeConfig,
    DataTypes,
    ChecklistObjectStatus,
)
from focus_validator.config_objects.override import Override
from focus_validator.exceptions import FocusNotImplementedError


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


class InvalidRule(BaseModel):
    rule_path: str
    error: str
    error_type: str


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
        checklist = {}
        overrides = {}
        if override_config:
            overrides = set(override_config.overrides)

        validation_rules = []
        for rule in rules:
            if isinstance(rule, InvalidRule):
                checklist[rule.rule_path] = ChecklistObject(
                    check_name=rule.rule_path,
                    dimension="Unknown",
                    error=f"{rule.error_type}: {rule.error}",
                    status=ChecklistObjectStatus.ERRORED,
                    rule_ref=rule,
                )
                continue
            if isinstance(rule.validation_config, DataTypeConfig):
                data_type = rule.validation_config.data_type
                if data_type == DataTypes.DECIMAL:
                    pandera_type = pa.Decimal
                else:
                    pandera_type = pa.String

                checklist[rule.check_id] = ChecklistObject(
                    check_name=rule.check_id,
                    dimension=rule.dimension,
                    status=ChecklistObjectStatus.SKIPPED
                    if rule.check_id in overrides
                    else ChecklistObjectStatus.PENDING,
                    friendly_name=f"Ensures that dimension is of {data_type.value} type.",
                    rule_ref=rule,
                )
                schema_dict[rule.dimension] = pa.Column(
                    pandera_type,
                    required=False,
                    checks=[],
                    nullable=True,
                )
            else:
                validation_rules.append(rule)

        for dimension_name, dimension_rules in groupby(
            sorted(validation_rules, key=lambda item: item.dimension),
            key=lambda item: item.dimension,
        ):
            dimension_rules: List[Rule] = list(dimension_rules)
            try:
                pa_column = schema_dict[dimension_name]
            except KeyError:
                pa_column = None
            for rule in dimension_rules:
                checklist[rule.check_id] = check_list_object = ChecklistObject(
                    check_name=rule.check_id,
                    dimension=dimension_name,
                    friendly_name=rule.validation_config.parse_friendly_name(),
                    status=ChecklistObjectStatus.PENDING,
                    rule_ref=rule,
                )

                if pa_column is None:
                    check_list_object.error = (
                        "ConfigurationError: No configuration found for dimension."
                    )
                    check_list_object.status = ChecklistObjectStatus.ERRORED
                elif rule.check_id in overrides:
                    check_list_object.status = ChecklistObjectStatus.SKIPPED
                else:
                    if rule.validation_config.check == "dimension_required":
                        pa_column.required = True
                    else:
                        check = rule.__process_validation_config__()
                        pa_column.checks.append(check)

        return pa.DataFrameSchema(schema_dict, strict=False), checklist

    @staticmethod
    def load_yaml(rule_path) -> Union["Rule", InvalidRule]:
        try:
            with open(rule_path, "r") as f:
                rule_obj = yaml.safe_load(f)
            return Rule.parse_obj(rule_obj)
        except Exception as e:
            return InvalidRule(
                rule_path=rule_path, error=str(e), error_type=e.__class__.__name__
            )


class ChecklistObject(BaseModel):
    check_name: str
    dimension: str
    friendly_name: Optional[str]
    error: Optional[str]
    status: ChecklistObjectStatus
    rule_ref: Union[InvalidRule, Rule]
