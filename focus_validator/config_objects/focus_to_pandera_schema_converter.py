import os
from itertools import groupby
from typing import Dict, List, Optional, Set, Union

import pandas as pd
import pandera as pa
import sqlglot
from pandera.api.pandas.types import PandasDtypeInputTypes

from focus_validator.config_objects import ChecklistObject, InvalidRule, Rule
from focus_validator.config_objects.common import (
    AllowNullsCheck,
    ChecklistObjectStatus,
    DataTypeCheck,
    DataTypes,
    SQLQueryCheck,
    ValueInCheck,
)
from focus_validator.config_objects.override import Override
from focus_validator.exceptions import FocusNotImplementedError

# group index column adds a column to the dataframe which is used to group the dataframe, otherwise the default
# groupby function does not carry forward all rows in the dataframe causing it to not have row numbers
GROUP_INDEX_COLUMN = "group_index_column"


def __groupby_fnc__(df: pd.DataFrame, column_alias: List[str]):
    """
    Custom groupby function to be used with pandera check_sql_query, allowing null values
    Default groupby function does not allow null values
    """
    df[GROUP_INDEX_COLUMN] = range(0, len(df))
    return df.groupby(column_alias + [GROUP_INDEX_COLUMN], dropna=False)


class FocusToPanderaSchemaConverter:
    @staticmethod
    def __generate_pandera_check__(rule: Rule, check_id):
        """
        Generates a single pandera check based on the check config which can then be added to the pa.Column.
        :param rule:
        :param check_id:
        :return:
        """

        check = rule.check
        error_string = "{}::: {}".format(check_id, rule.check_friendly_name)

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
        elif isinstance(check, SQLQueryCheck):
            column_alias = [
                column.alias_or_name
                for column in sqlglot.parse_one(check.sql_query).find_all(
                    sqlglot.exp.Column
                )
            ]
            return pa.Check.check_sql_query(
                sql_query=check.sql_query,
                error=error_string,
                column_alias=column_alias,
                groupby=lambda df: __groupby_fnc__(df=df, column_alias=column_alias),
            )
        elif isinstance(check, AllowNullsCheck):
            return pa.Check.check_not_null(
                error=error_string, ignore_na=False, allow_nulls=check.allow_nulls
            )
        else:
            raise FocusNotImplementedError(
                msg="Check type: {} not implemented.".format(type(check))
            )

    @classmethod
    def __generate_column_definition__(
        cls, rule: Rule, overrides, data_type: DataTypes
    ):
        """
        Generates column data type validation obj and pa.Column which will contain all other checks
        """
        column_checks = []

        pandera_type: Optional[PandasDtypeInputTypes]
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
        elif data_type == DataTypes.STRINGIFIED_JSON_OBJECT:
            pandera_type = None
            column_checks.append(
                pa.Check.check_stringified_json_object_dtype(
                    ignore_na=True,
                    error=f"{rule.check_id}:::Ensures that column is of {data_type.value} type.",
                )
            )
        else:
            pandera_type = pa.String

        check_list_object = ChecklistObject(
            check_name=rule.check_id,
            column_id=rule.column_id,
            status=ChecklistObjectStatus.SKIPPED
            if rule.check_id in overrides
            else ChecklistObjectStatus.PENDING,
            friendly_name=f"Ensures that column is of {data_type.value} type.",
            rule_ref=rule,
        )
        pa_column = pa.Column(
            pandera_type,  # type: ignore
            required=False,
            checks=column_checks,
            nullable=True,
        )
        return check_list_object, pa_column

    @classmethod
    def __generate_non_dtype_check__(
        cls,
        column_id,
        column_rules: List["Rule"],
        schema_dict: Dict[str, pa.Column],
        checklist,
        overrides,
    ):
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
                    check = cls.__generate_pandera_check__(
                        rule=rule, check_id=rule.check_id
                    )
                    pa_column.checks.append(check)

    @classmethod
    def generate_pandera_schema(
        cls,
        rules: List[Union[Rule, InvalidRule]],
        override_config: Optional[Override] = None,
    ):
        schema_dict = {}
        checklist = {}
        overrides: Set[str] = set()
        if override_config:
            overrides = set(override_config.overrides)

        validation_rules = []
        for rule in rules:
            if isinstance(rule, InvalidRule):
                checklist[rule.rule_path] = ChecklistObject(
                    check_name=os.path.splitext(os.path.basename(rule.rule_path))[0],
                    column_id="Unknown",
                    error=f"{rule.error_type}: {rule.error}",
                    status=ChecklistObjectStatus.ERRORED,
                    rule_ref=rule,
                )
                continue

            if isinstance(rule.check, DataTypeCheck):
                check_list_object, pa_column = cls.__generate_column_definition__(
                    rule=rule, overrides=overrides, data_type=rule.check.data_type
                )
                checklist[rule.check_id] = check_list_object
                schema_dict[rule.column_id] = pa_column
            else:
                validation_rules.append(rule)

        # groups check types by column id so that they can be associated with matching column
        for column_id, column_rules in groupby(
            sorted(validation_rules, key=lambda item: item.column_id),
            key=lambda item: item.column_id,
        ):
            cls.__generate_non_dtype_check__(
                column_id=column_id,
                checklist=checklist,
                column_rules=list(column_rules),
                overrides=overrides,
                schema_dict=schema_dict,
            )
        return (
            pa.DataFrameSchema(schema_dict, strict=False),
            checklist,
        )
