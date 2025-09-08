import os
from abc import ABC, abstractmethod
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
from focus_validator.config_objects.rule import CompositeCheck
from focus_validator.config_objects.override import Override
from focus_validator.exceptions import FocusNotImplementedError


class DuckDBColumnCheck:
    def __init__(self, column_name: str, check_type: str, check_sql: str, error_message: str):
        self.columnName = column_name
        self.checkType = check_type
        self.checkSql = check_sql
        self.errorMessage = error_message


class DuckDBCheckGenerator(ABC):
    # Abstract base class for generating DuckDB validation checks
    def __init__(self, rule: Rule, check_id: str):
        self.rule = rule
        self.checkId = check_id
        self.columnName = rule.column_id
        self.errorMessage = f"{check_id}: {rule.check_friendly_name}"

    @abstractmethod
    def generateSql(self) -> str:
        # Generate the SQL query for this check type
        pass

    @abstractmethod
    def getCheckType(self) -> str:
        # Return the check type identifier
        pass

    def generateCheck(self) -> DuckDBColumnCheck:
        # Generate the complete DuckDB check
        return DuckDBColumnCheck(
            column_name=self.columnName,
            check_type=self.getCheckType(),
            check_sql=self.generateSql(),
            error_message=self.errorMessage
        )


class ColumnPresentCheckGenerator(DuckDBCheckGenerator):
    # Generate column presence check SQL for DuckDB
    def generateSql(self) -> str:
        return f"""
        SELECT COUNT(*) = 0 as check_failed
        FROM information_schema.columns
        WHERE table_name = '{{table_name}}'
        AND column_name = '{self.rule.column_id}'
        """

    def getCheckType(self) -> str:
        return "column_presence"


class TypeStringCheckGenerator(DuckDBCheckGenerator):
    # Generate type string validation check
    def generateSql(self) -> str:
        return f"""
        SELECT CASE
            WHEN COUNT(*) = 0 THEN FALSE
            ELSE COUNT(CASE WHEN typeof({self.rule.column_id}) != 'VARCHAR' THEN 1 END) > 0
        END as check_failed
        FROM {{table_name}}
        """

    def getCheckType(self) -> str:
        return "type_string"


class TypeDecimalCheckGenerator(DuckDBCheckGenerator):
    # Generate type decimal validation check
    def generateSql(self) -> str:
        return f"""
        SELECT CASE
            WHEN COUNT(*) = 0 THEN FALSE
            ELSE COUNT(CASE WHEN typeof({self.rule.column_id}) NOT IN ('DECIMAL', 'DOUBLE', 'FLOAT') THEN 1 END) > 0
        END as check_failed
        FROM {{table_name}}
        """

    def getCheckType(self) -> str:
        return "type_decimal"


class CheckValueGenerator(DuckDBCheckGenerator):
    # Generate check for specific value
    def __init__(self, rule: Rule, check_id: str, expected_value):
        super().__init__(rule, check_id)
        self.expectedValue = expected_value

    def generateSql(self) -> str:
        if self.expectedValue is None:
            condition = f"{self.rule.column_id} IS NOT NULL"
        else:
            condition = f"{self.rule.column_id} != '{self.expectedValue}'"

        return f"""
        SELECT COUNT(CASE WHEN {condition} THEN 1 END) > 0 as check_failed
        FROM {{table_name}}
        """

    def getCheckType(self) -> str:
        return "check_value"


class CheckNotValueGenerator(DuckDBCheckGenerator):
    # Generate check for not having specific value
    def __init__(self, rule: Rule, check_id: str, forbidden_value):
        super().__init__(rule, check_id)
        self.forbiddenValue = forbidden_value

    def generateSql(self) -> str:
        if self.forbiddenValue is None:
            condition = f"{self.rule.column_id} IS NULL"
        else:
            condition = f"{self.rule.column_id} = '{self.forbiddenValue}'"

        return f"""
        SELECT COUNT(CASE WHEN {condition} THEN 1 END) > 0 as check_failed
        FROM {{table_name}}
        """

    def getCheckType(self) -> str:
        return "check_not_value"


class FormatNumericGenerator(DuckDBCheckGenerator):
    # Generate numeric format validation check
    def generateSql(self) -> str:
        return f"""
        SELECT COUNT(CASE
            WHEN {self.rule.column_id} IS NOT NULL
            AND NOT ({self.rule.column_id}::TEXT ~ '^[+-]?([0-9]*[.])?[0-9]+$')
            THEN 1
        END) > 0 as check_failed
        FROM {{table_name}}
        """

    def getCheckType(self) -> str:
        return "format_numeric"


class CheckGreaterOrEqualGenerator(DuckDBCheckGenerator):
    # Generate greater than or equal check
    def __init__(self, rule: Rule, check_id: str, min_value):
        super().__init__(rule, check_id)
        self.minValue = min_value

    def generateSql(self) -> str:
        return f"""
        SELECT COUNT(CASE WHEN {self.rule.column_id} < {self.minValue} THEN 1 END) > 0 as check_failed
        FROM {{table_name}}
        """

    def getCheckType(self) -> str:
        return "check_greater_equal"


class CompositeRuleGenerator(DuckDBCheckGenerator):
    # Generate composite rule check (AND/OR of other rules)
    def __init__(self, rule: Rule, check_id: str, dependency_results: Dict[str, bool]):
        super().__init__(rule, check_id)
        self.dependencyResults = dependency_results
        self.composite_check = rule.check

    def generateSql(self) -> str:
        # For composite rules, we don't generate SQL but evaluate dependencies
        if isinstance(self.composite_check, CompositeCheck):
            logic_operator = self.composite_check.logic_operator
            dependency_rule_ids = self.composite_check.dependency_rule_ids

            if logic_operator == "AND":
                # All dependencies must pass for composite to pass
                all_passed = all(
                    not self.dependencyResults.get(dep_id, True) # True means failed, False means passed
                    for dep_id in dependency_rule_ids
                )
                check_failed = not all_passed
            elif logic_operator == "OR":
                # At least one dependency must pass for composite to pass
                any_passed = any(
                    not self.dependencyResults.get(dep_id, True)
                    for dep_id in dependency_rule_ids
                )
                check_failed = not any_passed
            else:
                check_failed = True  # Unknown logic operator

            return f"SELECT {check_failed} as check_failed"

        return "SELECT TRUE as check_failed"  # Fallback

    def getCheckType(self) -> str:
        return "composite_rule"


class FocusToDuckDBSchemaConverter:
    # Generators for all types of checks (Starting with pre 1.0 checks for now) TODO: Add dependencies
    CHECK_GENERATORS = {
        "ColumnPresent": ColumnPresentCheckGenerator,
        "TypeString": TypeStringCheckGenerator,
        "TypeDecimal": TypeDecimalCheckGenerator,
        "CheckValue": CheckValueGenerator,
        "CheckNotValue": CheckNotValueGenerator,
        "FormatNumeric": FormatNumericGenerator,
        "CheckGreaterOrEqualThanValue": CheckGreaterOrEqualGenerator,
        "CompositeRule": CompositeRuleGenerator,
    }

    @classmethod
    def __generate_duckdb_check__(cls, rule: Rule, check_id: str) -> Optional[DuckDBColumnCheck]:
        # Single dispatch method using registry
        check = rule.check

        # Handle simple string checks
        if check == "column_required":
            generator = cls.CHECK_GENERATORS["ColumnPresent"](rule, check_id)
            return generator.generateCheck()

        # Handle DataTypeCheck objects
        elif isinstance(check, DataTypeCheck):
            if check.data_type == DataTypes.DECIMAL:
                generator = cls.CHECK_GENERATORS["TypeDecimal"](rule, check_id)
                return generator.generateCheck()
            elif check.data_type == DataTypes.STRING:
                generator = cls.CHECK_GENERATORS["TypeString"](rule, check_id)
                return generator.generateCheck()

        # Handle ValueComparisonCheck objects
        elif isinstance(check, ValueComparisonCheck):
            if check.operator == "not_equals":
                generator = cls.CHECK_GENERATORS["CheckNotValue"](rule, check_id, check.value)
                return generator.generateCheck()
            elif check.operator == "equals":
                generator = cls.CHECK_GENERATORS["CheckValue"](rule, check_id, check.value)
                return generator.generateCheck()
            elif check.operator == "greater_equal":
                generator = cls.CHECK_GENERATORS["CheckGreaterOrEqualThanValue"](rule, check_id, check.value)
                return generator.generateCheck()

        # Handle FormatCheck objects
        elif isinstance(check, FormatCheck):
            if check.format_type == "numeric":
                generator = cls.CHECK_GENERATORS["FormatNumeric"](rule, check_id)
                return generator.generateCheck()

        # Handle CompositeCheck objects
        elif isinstance(check, CompositeCheck):
            # For composite rules, we need dependency results which are handled separately
            # Return None here and handle composite rules in a special method
            return None

        return None

    @classmethod
    def __generate_checks__(
        cls, rules: List[Rule], overrides: Set[str]
    ) -> List[DuckDBColumnCheck]:
        # Generate DuckDB validation checks using registry
        checks = []

        for rule in rules:
            if rule.check_id not in overrides:
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

            # Check if this is a dynamic rule (marked during loading)
            is_dynamic_rule = hasattr(rule, '_rule_type') and getattr(rule, '_rule_type', '').lower() == "dynamic"
            
            # Create checklist object for each rule
            if rule.check_id in overrides:
                status = ChecklistObjectStatus.SKIPPED
            elif is_dynamic_rule:
                status = ChecklistObjectStatus.SKIPPED
            else:
                status = ChecklistObjectStatus.PENDING
                
            checklist[rule.check_id] = ChecklistObject(
                check_name=rule.check_id,
                column_id=rule.column_id,
                friendly_name=rule.check_friendly_name,
                status=status,
                rule_ref=rule,
            )

            # Only add static rules to validation processing
            if not is_dynamic_rule:
                validationRules.append(rule)

        # Generate validation checks using registry
        validationChecks = cls.__generate_checks__(validationRules, overrides)
        checks.extend(validationChecks)

        return checks, checklist

    @staticmethod
    def executeDuckDBValidation(
        connection: duckdb.DuckDBPyConnection,
        tableName: str,
        checks: List[DuckDBColumnCheck],
        checklist: Dict[str, ChecklistObject],
        dependency_results: Optional[Dict[str, bool]] = None
    ) -> Dict[str, ChecklistObject]:
        # Execute DuckDB validation checks
        for check in checks:
            try:
                # Replace table name placeholder in SQL
                sql = check.checkSql.replace('{table_name}', tableName)
                result = connection.execute(sql).fetchone()

                # Find corresponding checklist item by matching check type and column
                checklistItem = None
                for check_id, item in checklist.items():
                    if (item.column_id == check.columnName and
                        hasattr(item.rule_ref, 'check')):
                        # Try to match by error message which contains the check ID
                        if check_id in check.errorMessage:
                            checklistItem = item
                            break

                # Fallback: if no specific match found, just match by column and check type
                if not checklistItem:
                    for item in checklist.values():
                        if (item.column_id == check.columnName and
                            hasattr(item.rule_ref, 'check')):
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
                errorItem = None
                for check_id, item in checklist.items():
                    if (item.column_id == check.columnName and
                        hasattr(item.rule_ref, 'check')):
                        if check_id in check.errorMessage:
                            errorItem = item
                            break
                
                # Fallback matching
                if not errorItem:
                    for item in checklist.values():
                        if (item.column_id == check.columnName and
                            hasattr(item.rule_ref, 'check')):
                            errorItem = item
                            break

                if errorItem:
                    errorItem.status = ChecklistObjectStatus.ERRORED
                    errorItem.error = f"DuckDB validation error: {str(e)}"

        # Handle composite rules after basic validation
        FocusToDuckDBSchemaConverter._executeCompositeRules(checklist, dependency_results or {})
        
        return checklist

    @staticmethod
    def _executeCompositeRules(checklist: Dict[str, ChecklistObject], dependency_results: Dict[str, bool]):
        """Execute composite rule validation based on dependency results."""
        for check_id, item in checklist.items():
            if (hasattr(item.rule_ref, 'check') and 
                isinstance(item.rule_ref.check, CompositeCheck)):
                
                composite_check = item.rule_ref.check
                logic_operator = composite_check.logic_operator
                dependency_rule_ids = composite_check.dependency_rule_ids
                
                try:
                    if logic_operator == "AND":
                        # All dependencies must pass for composite to pass
                        all_passed = all(
                            dep_id in checklist and checklist[dep_id].status == ChecklistObjectStatus.PASSED
                            for dep_id in dependency_rule_ids
                        )
                        item.status = ChecklistObjectStatus.PASSED if all_passed else ChecklistObjectStatus.FAILED
                        
                    elif logic_operator == "OR":
                        # At least one dependency must pass for composite to pass
                        any_passed = any(
                            dep_id in checklist and checklist[dep_id].status == ChecklistObjectStatus.PASSED
                            for dep_id in dependency_rule_ids
                        )
                        item.status = ChecklistObjectStatus.PASSED if any_passed else ChecklistObjectStatus.FAILED
                    
                    if item.status == ChecklistObjectStatus.FAILED:
                        item.error = f"Composite rule {logic_operator} logic failed for dependencies: {dependency_rule_ids}"
                        
                except Exception as e:
                    item.status = ChecklistObjectStatus.ERRORED
                    item.error = f"Error evaluating composite rule: {str(e)}"
