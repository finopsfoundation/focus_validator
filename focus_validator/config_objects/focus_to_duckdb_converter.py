import os
from itertools import groupby
from typing import Dict, List, Optional, Set, Union

import pandas as pd
import duckdb
import sqlglot

from focus_validator.config_objects import ChecklistObject, InvalidRule, Rule
from focus_validator.config_objects.common import (
    AllowNullsCheck,
    ChecklistObjectStatus,
    DataTypeCheck,
    DataTypes,
    FormatCheck,
    SQLQueryCheck,
    ValueComparisonCheck,
    ValueInCheck,
)
from focus_validator.config_objects.override import Override
from focus_validator.exceptions import FocusNotImplementedError


class DuckDBColumnCheck:
    def __init__(self, column_name: str, check_type: str, check_sql: str, error_message: str):
        self.columnName = column_name
        self.checkType = check_type
        self.checkSql = check_sql
        self.errorMessage = error_message


class FocusToDuckDBSchemaConverter:
    @staticmethod
    def __generate_duckdb_check__(rule: Rule, check_id: str) -> Optional[DuckDBColumnCheck]:
        # Generate column presence check SQL for DuckDB
        check = rule.check
        errorString = f"{check_id}: {rule.check_friendly_name}"

        if rule.check == "column_required":
            # Column presence check - verify column exists in the table
            checkSql = f"""
            SELECT COUNT(*) = 0 as check_failed
            FROM information_schema.columns
            WHERE table_name = '{{table_name}}'
            AND column_name = '{rule.column_id}'
            """

            return DuckDBColumnCheck(
                column_name=rule.column_id,
                check_type="column_presence",
                check_sql=checkSql,
                error_message=f"Column '{rule.column_id}' is required but not present in the table"
            )

        # Only focusing only on column presence checks
        return None

    @classmethod
    def __generate_column_presence_checks__(
        cls, rules: List[Rule], overrides: Set[str]
    ) -> List[DuckDBColumnCheck]:
        # Generate DuckDB column presence checks
        checks = []

        for rule in rules:
            if rule.check == "column_required" and rule.check_id not in overrides:
                check = cls.__generate_duckdb_check__(rule, rule.check_id)
                if check:
                    checks.append(check)

        return checks

    @classmethod
    def generateDuckDBValidation(
        cls,
        rules: List[Union[Rule, InvalidRule]],
        overrideConfig: Optional[Override] = None,
    ) -> tuple[List[DuckDBColumnCheck], Dict[str, ChecklistObject]]:
        # Generate DuckDB validation checks and checklist
        checks = []
        checklist = {}
        overrides: Set[str] = set()
        if overrideConfig:
            overrides = set(overrideConfig.overrides)

        validationRules = []
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

            # Create checklist object for each rule
            checklist[rule.check_id] = ChecklistObject(
                check_name=rule.check_id,
                column_id=rule.column_id,
                friendly_name=rule.check_friendly_name,
                status=ChecklistObjectStatus.SKIPPED if rule.check_id in overrides else ChecklistObjectStatus.PENDING,
                rule_ref=rule,
            )

            validationRules.append(rule)

        # Generate column presence checks
        columnPresenceChecks = cls.__generate_column_presence_checks__(validationRules, overrides)
        checks.extend(columnPresenceChecks)

        return checks, checklist

    @staticmethod
    def executeDuckDBValidation(
        connection: duckdb.DuckDBPyConnection,
        tableName: str,
        checks: List[DuckDBColumnCheck],
        checklist: Dict[str, ChecklistObject]
    ) -> Dict[str, ChecklistObject]:
        # Execute DuckDB validation checks
        for check in checks:
            try:
                # Replace table name placeholder in SQL
                sql = check.checkSql.replace('{table_name}', tableName)
                result = connection.execute(sql).fetchone()

                # Find corresponding checklist item
                checklistItem = None
                for item in checklist.values():
                    if (item.column_id == check.columnName and
                        hasattr(item.rule_ref, 'check') and
                        item.rule_ref.check == "column_required"):
                        checklistItem = item
                        break

                if checklistItem:
                    if result and result[0]:  # check_failed is True
                        checklistItem.status = ChecklistObjectStatus.FAILED
                        checklistItem.error = check.errorMessage
                    else:
                        checklistItem.status = ChecklistObjectStatus.PASSED

            except Exception as e:
                # Find corresponding checklist item and mark as errored
                for item in checklist.values():
                    if (item.column_id == check.columnName and
                        hasattr(item.rule_ref, 'check') and
                        item.rule_ref.check == "column_required"):
                        item.status = ChecklistObjectStatus.ERRORED
                        item.error = f"DuckDB validation error: {str(e)}"
                        break

        return checklist
