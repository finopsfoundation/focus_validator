import os
from typing import Annotated, Optional, Union

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic_core.core_schema import ValidationInfo

from focus_validator.config_objects.common import (
    SIMPLE_CHECKS,
    AllowNullsCheck,
    ChecklistObjectStatus,
    DataTypeCheck,
    SQLQueryCheck,
    ValueInCheck,
    generate_check_friendly_name,
)


class InvalidRule(BaseModel):
    rule_path: str
    error: str
    error_type: str


class Rule(BaseModel):
    """
    Base rule class that loads spec configs and generate
    a pandera rule that can be validated.
    """

    check_id: str
    column_id: str
    check: Union[
        SIMPLE_CHECKS, AllowNullsCheck, ValueInCheck, DataTypeCheck, SQLQueryCheck
    ]

    check_friendly_name: Annotated[
        Optional[str], Field(validate_default=True)
    ] = None  # auto generated or else can be overwritten
    check_type_friendly_name: Annotated[
        Optional[str], Field(validate_default=True)
    ] = None

    model_config = ConfigDict(
        extra="forbid",  # prevents config from containing any undesirable keys
        frozen=True,  # prevents any modification to any attribute onces loaded from config
    )

    @field_validator("check_friendly_name")
    def validate_or_generate_check_friendly_name(
        cls, check_friendly_name, validation_info: ValidationInfo
    ):
        values = validation_info.data
        if (
            check_friendly_name is None
            and values.get("check") is not None
            and values.get("column_id") is not None
        ):
            check_friendly_name = generate_check_friendly_name(
                check=values["check"], column_id=values["column_id"]
            )
        return check_friendly_name

    @field_validator("check_type_friendly_name")
    def validate_or_generate_check_type_friendly_name(
        cls, check_type_friendly_name, validation_info: ValidationInfo
    ):
        values = validation_info.data
        if values.get("check") is not None and values.get("column_id") is not None:
            check = values.get("check")
            if isinstance(check, str):
                check_type_friendly_name = "".join(
                    [word.title() for word in check.split("_")]
                )
            else:
                check_type_friendly_name = check.__class__.__name__
        return check_type_friendly_name

    @staticmethod
    def load_yaml(
        rule_path, column_namespace: Optional[str] = None
    ) -> Union["Rule", InvalidRule]:
        rule_path_basename = os.path.splitext(os.path.basename(rule_path))[0]

        try:
            with open(rule_path, "r") as f:
                rule_obj = yaml.safe_load(f)

            if (
                isinstance(rule_obj, dict)
                and rule_obj.get("column")
                and column_namespace
            ):
                rule_obj["column"] = f"{column_namespace}:{rule_obj['column']}"

            if isinstance(rule_obj, dict) and "check_id" not in rule_obj:
                rule_obj["check_id"] = rule_path_basename

            return Rule.model_validate(rule_obj)
        except Exception as e:
            return InvalidRule(
                rule_path=rule_path_basename,
                error=str(e),
                error_type=e.__class__.__name__,
            )


class ChecklistObject(BaseModel):
    check_name: str
    column_id: str
    friendly_name: Optional[str] = None
    error: Optional[str] = None
    status: ChecklistObjectStatus
    rule_ref: Union[InvalidRule, Rule]
