from itertools import groupby
from typing import List, Union, Optional

import pandera as pa
import yaml
from pydantic import BaseModel, root_validator

from focus_validator.config_objects.common import (
    ValueInCheck,
    AllowNullsCheck,
    SIMPLE_CHECKS,
    DataTypes,
    ChecklistObjectStatus,
    DataTypeCheck,
    generate_check_friendly_name,
)
from focus_validator.config_objects.override import Override
from focus_validator.exceptions import FocusNotImplementedError


class InvalidRule(BaseModel):
    rule_path: str
    error: str
    error_type: str


class Rule(BaseModel):
    check_id: str
    column_id: str
    check: Union[SIMPLE_CHECKS, AllowNullsCheck, ValueInCheck, DataTypeCheck]

    check_friendly_name: str = None  # auto generated or else can be overwritten
    check_type_friendly_name: str = None

    class Config:
        extra = "forbid"  # prevents config from containing any undesirable keys
        frozen = (
            True  # prevents any modification to any attribute onces loaded from config
        )

    @root_validator
    def root_val(cls, values):
        check = values.get("check")
        check_friendly_name = values.get("check_friendly_name")
        column_id = values.get("column_id")
        if check is not None:
            if isinstance(check, str):
                check_type_friendly_name = "".join(
                    [word.title() for word in check.split("_")]
                )
            else:
                check_type_friendly_name = check.__class__.__name__
            values["check_type_friendly_name"] = check_type_friendly_name

            if check_friendly_name is None and column_id is not None:
                values["check_friendly_name"] = generate_check_friendly_name(
                    check=check, column_id=column_id
                )

        return values

    def generate_pandera_rule(self, check_id):
        check = self.check
        error_string = "{}::: {}".format(check_id, self.check_friendly_name)

        if isinstance(check, str):
            if check == "check_unique":
                return pa.Check.check_unique(error=error_string)
            else:
                raise FocusNotImplementedError(
                    msg="Check type: {} not implemented.".format(check)
                )
        elif isinstance(check, ValueInCheck):
            return pa.Check.check_value_in(
                allowed_values=check.value_in, error=error_string
            )
        elif isinstance(check, AllowNullsCheck):
            return pa.Check.check_not_null(error=error_string, ignore_na=False)
        else:
            raise FocusNotImplementedError(
                msg="Check type: {} not implemented.".format(type(check))
            )

    @classmethod
    def generate_schema(
        cls,
        rules: List[Union["Rule", InvalidRule]],
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
                    column_id="Unknown",
                    error=f"{rule.error_type}: {rule.error}",
                    status=ChecklistObjectStatus.ERRORED,
                    rule_ref=rule,
                )
                continue

            if isinstance(rule.check, DataTypeCheck):
                column_checks = []

                data_type = rule.check.data_type
                if data_type == DataTypes.DECIMAL:
                    pandera_type = pa.Float
                elif data_type == DataTypes.DATETIME:
                    pandera_type = None
                    column_checks.append(
                        pa.Check.check_datetime_dtype(
                            ignore_na=True,
                            error=f"{rule.check_id}:::Ensures that column is of {data_type.value} type.",
                        )
                    )
                elif data_type == DataTypes.CURRENCY_CODE:
                    pandera_type = None
                    column_checks.append(
                        pa.Check.check_currency_code_dtype(
                            ignore_na=True,
                            error=f"{rule.check_id}:::Ensures that column is of {data_type.value} type.",
                        )
                    )
                else:
                    pandera_type = pa.String

                checklist[rule.check_id] = ChecklistObject(
                    check_name=rule.check_id,
                    column_id=rule.column_id,
                    status=ChecklistObjectStatus.SKIPPED
                    if rule.check_id in overrides
                    else ChecklistObjectStatus.PENDING,
                    friendly_name=f"Ensures that column is of {data_type.value} type.",
                    rule_ref=rule,
                )
                schema_dict[rule.column_id] = pa.Column(
                    pandera_type,
                    required=False,
                    checks=column_checks,
                    nullable=True,
                )
            else:
                validation_rules.append(rule)

        for column_id, column_rules in groupby(
            sorted(validation_rules, key=lambda item: item.column_id),
            key=lambda item: item.column_id,
        ):
            column_rules: List[Rule] = list(column_rules)
            try:
                pa_column = schema_dict[column_id]
            except KeyError:
                pa_column = None
            for rule in column_rules:
                checklist[rule.check_id] = check_list_object = ChecklistObject(
                    check_name=rule.check_id,
                    column_id=column_id,
                    friendly_name=rule.check_friendly_name,
                    status=ChecklistObjectStatus.PENDING,
                    rule_ref=rule,
                )

                if pa_column is None:
                    check_list_object.error = (
                        "ConfigurationError: No configuration found for column."
                    )
                    check_list_object.status = ChecklistObjectStatus.ERRORED
                elif rule.check_id in overrides:
                    check_list_object.status = ChecklistObjectStatus.SKIPPED
                else:
                    if rule.check == "column_required":
                        pa_column.required = True
                    else:
                        check = rule.generate_pandera_rule(check_id=rule.check_id)
                        pa_column.checks.append(check)

        return pa.DataFrameSchema(schema_dict, strict=False), checklist

    @staticmethod
    def load_yaml(
        rule_path, column_namespace: str = None
    ) -> Union["Rule", InvalidRule]:
        try:
            with open(rule_path, "r") as f:
                rule_obj = yaml.safe_load(f)

            if (
                isinstance(rule_obj, dict)
                and rule_obj.get("column")
                and column_namespace
            ):
                rule_obj["column"] = f"{column_namespace}:{rule_obj['column']}"

            return Rule.parse_obj(rule_obj)
        except Exception as e:
            return InvalidRule(
                rule_path=rule_path, error=str(e), error_type=e.__class__.__name__
            )


class ChecklistObject(BaseModel):
    check_name: str
    column_id: str
    friendly_name: Optional[str]
    error: Optional[str]
    status: ChecklistObjectStatus
    rule_ref: Union[InvalidRule, Rule]
