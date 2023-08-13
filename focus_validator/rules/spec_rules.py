import os
from typing import Dict, Optional

import pandas as pd
from pandera.errors import SchemaErrors

from focus_validator.config_objects import (
    ChecklistObject,
    ChecklistObjectStatus,
    Override,
    Rule,
)
from focus_validator.config_objects.focus_to_pandera_schema_converter import (
    FocusToPanderaSchemaConverter,
)
from focus_validator.exceptions import UnsupportedVersion


def convert_missing_column_errors(df, checklist):
    def process_row(row):
        if (
            row["schema_context"] == "DataFrameSchema"
            and row["check"] == "column_in_dataframe"
        ):
            for check_name, check_obj in checklist.items():
                if (
                    row["failure_case"] == check_obj.column_id
                    and check_obj.rule_ref.check == "column_required"
                ):
                    row["check"] = f"{check_name}:::{check_obj.friendly_name}"
                    row["column"] = check_obj.column_id
                    row["failure_case"] = None
                    return row
        else:
            return row

    filtered_df = df.apply(process_row, axis=1)
    return filtered_df


def convert_dtype_column_errors(df, checklist):
    def process_row(row):
        if row["schema_context"] == "Column" and row["check"].startswith("dtype"):
            for check_name, check_obj in checklist.items():
                if row["column"] == check_obj.column_id:
                    row["check"] = f"{check_name}:::{check_obj.friendly_name}"
                    row["column"] = check_obj.column_id
                    row["failure_case"] = None
                    return row
        else:
            return row

    filtered_df = df.apply(process_row, axis=1)
    return filtered_df


def restructure_failure_cases_df(failure_cases: pd.DataFrame, checklist):
    failure_cases = convert_missing_column_errors(failure_cases, checklist)
    failure_cases = convert_dtype_column_errors(failure_cases, checklist)
    failure_cases = failure_cases.rename(
        columns={"column": "Column", "index": "Row #", "failure_case": "Values"}
    )

    failure_cases[["Check Name", "Description"]] = failure_cases["check"].str.split(
        ":::", expand=True
    )
    failure_cases = failure_cases.drop("check", axis=1)
    failure_cases = failure_cases.drop("check_number", axis=1)
    failure_cases = failure_cases.drop("schema_context", axis=1)

    failure_cases = failure_cases.rename_axis("#")
    failure_cases.index = failure_cases.index + 1

    failure_cases["Row #"] = failure_cases["Row #"] + 1
    failure_cases = failure_cases[
        ["Column", "Check Name", "Description", "Values", "Row #"]
    ]

    return failure_cases


class ValidationResult:
    checklist: Dict[str, ChecklistObject]
    failure_cases: Optional[pd.DataFrame]

    def __init__(
        self,
        checklist: Dict[str, ChecklistObject],
        failure_cases: Optional[pd.DataFrame] = None,
    ):
        self.__failure_cases__ = failure_cases
        self.__checklist__ = checklist

    def process_result(self):
        failure_cases = self.__failure_cases__
        checklist = self.__checklist__
        if failure_cases is None:
            self.failure_cases = None
        else:
            self.failure_cases = failure_cases = restructure_failure_cases_df(
                failure_cases, checklist
            )
            failed = set(failure_cases["Check Name"])
            for check_name in failed:
                checklist[check_name].status = ChecklistObjectStatus.FAILED

        for check_list_object in checklist.values():
            if check_list_object.status == ChecklistObjectStatus.PENDING:
                check_list_object.status = ChecklistObjectStatus.PASSED
        self.checklist = checklist


class SpecRules:
    def __init__(
        self, override_filename, rule_set_path, rules_version, column_namespace
    ):
        self.override_filename = override_filename
        self.override_config = None
        self.rules_version = rules_version
        self.rule_set_path = rule_set_path
        if self.rules_version not in self.supported_versions():
            raise UnsupportedVersion(
                f"FOCUS version {self.rules_version} not supported."
            )
        self.rules_path = os.path.join(self.rule_set_path, self.rules_version)
        self.rules = []
        self.column_namespace = column_namespace

    def supported_versions(self):
        return sorted([x for x in os.walk(self.rule_set_path)][0][1])

    def load(self):
        self.load_overrides()
        self.load_rules()

    def load_overrides(self):
        if not self.override_filename:
            return {}
        self.override_config = Override.load_yaml(self.override_filename)

    def load_rules(self):
        for rule_path in self.get_rule_paths():
            self.rules.append(
                Rule.load_yaml(rule_path, column_namespace=self.column_namespace)
            )

    def get_rule_paths(self):
        for root, dirs, files in os.walk(self.rules_path, topdown=False):
            for name in files:
                yield os.path.join(root, name)

    def validate(self, focus_data):
        (
            pandera_schema,
            checklist,
        ) = FocusToPanderaSchemaConverter.generate_pandera_schema(
            rules=self.rules, override_config=self.override_config
        )
        try:
            pandera_schema.validate(focus_data, lazy=True)
            failure_cases = None
        except SchemaErrors as e:
            failure_cases = e.failure_cases

        validation_result = ValidationResult(
            checklist=checklist, failure_cases=failure_cases
        )
        validation_result.process_result()
        return validation_result
