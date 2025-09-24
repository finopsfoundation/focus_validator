import os
from typing import Dict, Optional

import pandas as pd
import duckdb

from focus_validator.config_objects import (
    ChecklistObject,
    ChecklistObjectStatus,
    Rule,
    JsonLoader,
)
from focus_validator.config_objects.rule import InvalidRule
from focus_validator.config_objects.focus_to_duckdb_converter import (
    FocusToDuckDBSchemaConverter,
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
        # For DuckDB validation, status is already set in the DuckDB converter
        # Just ensure any remaining pending items are marked as passed
        checklist = self.__checklist__
        self.failure_cases = self.__failure_cases__

        for check_list_object in checklist.values():
            if check_list_object.status == ChecklistObjectStatus.PENDING:
                check_list_object.status = ChecklistObjectStatus.PASSED
        self.checklist = checklist


class SpecRules:
    def __init__(
        self, rule_set_path, rules_version, column_namespace, rule_prefix=None
    ):
        self.rules_version = rules_version
        self.rule_set_path = rule_set_path
        if self.rules_version not in self.supported_versions():
            raise UnsupportedVersion(
                f"FOCUS version {self.rules_version} not supported."
            )
        self.rules_path = os.path.join(self.rule_set_path, self.rules_version)
        self.rules = []
        self.column_namespace = column_namespace
        self.rule_prefix = rule_prefix

        self.json_rules = {}
        self.json_checkfunctions = {}

    def supported_versions(self):
        return sorted([x for x in os.walk(self.rule_set_path)][0][1])

    def load(self):
        self.load_rules()


    def load_rules(self):
        # Load rules from JSON with dependency resolution
        json_rules_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'rules', f'cr-{self.rules_version}.json')
        self.json_rules, self.json_checkfunctions, rule_order = JsonLoader.load_json_rules_with_dependencies(
            json_rules_path, rule_prefix=self.rule_prefix
        )

        # Process rules in dependency order (topologically sorted)
        for rule_id in rule_order:
            ruleDescription = self.json_rules[rule_id]
            
            # Process both static and dynamic rules within version compatibility
            if float(ruleDescription["CRVersionIntroduced"]) <= float(self.rules_version):
                # Check if this is a Dynamic rule - handle specially
                if ruleDescription.get("Type", "").lower() == "dynamic":
                    # Create a minimal rule object for Dynamic rules that will be skipped
                    dynamic_rule = Rule(
                        check_id=rule_id,
                        column_id=ruleDescription.get("Reference", ""),
                        check="column_required",  # Placeholder check type - will be skipped anyway
                        check_friendly_name=ruleDescription.get("ValidationCriteria", {}).get("MustSatisfy", "Dynamic rule")
                    )
                    dynamic_rule._rule_type = "Dynamic"
                    self.rules.append(dynamic_rule)
                else:
                    # Use the new method that creates sub-rules for conditions
                    rule_objects = Rule.load_json_with_subchecks(ruleDescription, rule_id=rule_id, column_namespace=self.column_namespace)

                    for ruleObj in rule_objects:
                        if isinstance(ruleObj, InvalidRule):
                            continue  # Skip invalid rules

                        # Mark rule type for all rules
                        if hasattr(ruleObj, '__dict__'):
                            ruleObj.__dict__['_rule_type'] = ruleDescription.get("Type", "Static")

                        self.rules.append(ruleObj)

    def validate(self, focus_data, connection: Optional[duckdb.DuckDBPyConnection] = None):
        # Generate DuckDB validation checks and checklist
        (
            duckdb_checks,
            checklist,
        ) = FocusToDuckDBSchemaConverter.generateDuckDBValidation(
            rules=self.rules
        )

        if connection is None:
            connection = duckdb.connect(":memory:")
            connection.register("focus_data", focus_data)
            tableName = "focus_data"
        else:
            tableName = "focus_data"  # Default table name, could be parameterized

        # Execute DuckDB validation
        updated_checklist = FocusToDuckDBSchemaConverter.executeDuckDBValidation(
            connection=connection,
            tableName=tableName,
            checks=duckdb_checks,
            checklist=checklist
        )

        # No failure cases for now
        validation_result = ValidationResult(
            checklist=updated_checklist, failure_cases=None
        )
        validation_result.process_result()
        return validation_result
