from typing import Optional, Union

import yaml
from pydantic import BaseModel, root_validator

from focus_validator.config_objects.common import (
    SIMPLE_CHECKS,
    AllowNullsCheck,
    ChecklistObjectStatus,
    DataTypeCheck,
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
    check: Union[SIMPLE_CHECKS, AllowNullsCheck, ValueInCheck, DataTypeCheck]

    check_friendly_name: Optional[
        str
    ] = None  # auto generated or else can be overwritten
    check_type_friendly_name: Optional[str] = None

    class Config:
        extra = "forbid"  # prevents config from containing any undesirable keys
        frozen = (
            True  # prevents any modification to any attribute onces loaded from config
        )

    @root_validator
    def root_val(cls, values):
        """
        Root validator that checks for all options passed in the config and generate missing options.
        """

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

    @staticmethod
    def load_yaml(
        rule_path, column_namespace: Optional[str] = None
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
    friendly_name: Optional[str] = None
    error: Optional[str] = None
    status: ChecklistObjectStatus
    rule_ref: Union[InvalidRule, Rule]
