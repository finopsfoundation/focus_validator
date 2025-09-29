import os
import logging
import time
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
    ColumnComparisonCheck,
    ConformanceRuleCheck,
    DataTypeCheck,
    DataTypes,
    DistinctCountCheck,
    FormatCheck,
    SQLQueryCheck,
    ValueComparisonCheck,
    ValueInCheck,
)
from focus_validator.config_objects.rule import CompositeCheck
from focus_validator.exceptions import FocusNotImplementedError


class DuckDBColumnCheck:
    log = logging.getLogger(f"{__name__}.DuckDBColumnCheck")

    def __init__(self, column_name: str, check_type: str, check_sql: str, error_message: str):
        self.columnName = column_name
        self.checkType = check_type
        self.checkSql = check_sql
        self.errorMessage = error_message


class DuckDBCheckGenerator(ABC):
    # Abstract base class for generating DuckDB validation checks
    log = logging.getLogger(f"{__name__}.DuckDBCheckGenerator")

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


class ColumnComparisonGenerator(DuckDBCheckGenerator):
    # Generate check for comparing two columns (consolidated from CheckSameValue, CheckNotSameValue, ColumnByColumnEquals)
    def __init__(self, rule: Rule, check_id: str, comparison_column: str, operator: str):
        super().__init__(rule, check_id)
        self.comparisonColumn = comparison_column
        self.operator = operator

    def generateSql(self) -> str:
        # operator "equals" means columns should be equal, so fail if they're not equal
        # operator "not_equals" means columns should be different, so fail if they're equal
        if self.operator == "equals":
            condition = f"{self.rule.column_id} != {self.comparisonColumn}"
        elif self.operator == "not_equals":
            condition = f"{self.rule.column_id} = {self.comparisonColumn}"
        else:
            condition = "FALSE"

        return f"""
        SELECT COUNT(CASE WHEN {condition} THEN 1 END) > 0 as check_failed
        FROM {{table_name}}
        """

    def getCheckType(self) -> str:
        return f"column_comparison_{self.operator}"


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


class FormatDateTimeGenerator(DuckDBCheckGenerator):
    # Generate datetime format validation check for ISO 8601 extended format with UTC (YYYY-MM-DDTHH:mm:ssZ)
    def generateSql(self) -> str:
        return f"""
        SELECT COUNT(CASE
            WHEN {self.rule.column_id} IS NOT NULL
            AND NOT ({self.rule.column_id}::TEXT ~ '^[0-9]{{4}}-[0-1][0-9]-[0-3][0-9]T[0-2][0-9]:[0-5][0-9]:[0-5][0-9]Z$')
            THEN 1
        END) > 0 as check_failed
        FROM {{table_name}}
        """

    def getCheckType(self) -> str:
        return "format_datetime"


class FormatStringGenerator(DuckDBCheckGenerator):
    # Generate string format validation check for Pascal case, alphanumeric, and length requirements
    def generateSql(self) -> str:
        return f"""
        SELECT COUNT(CASE
            WHEN {self.rule.column_id} IS NOT NULL
            AND NOT (
                ({self.rule.column_id}::TEXT ~ '^[A-Z][a-zA-Z0-9]*$') OR
                ({self.rule.column_id}::TEXT ~ '^x_[A-Z][a-zA-Z0-9]*$')
            )
            OR LENGTH({self.rule.column_id}::TEXT) > 50
            THEN 1
        END) > 0 as check_failed
        FROM {{table_name}}
        """

    def getCheckType(self) -> str:
        return "format_string"


class FormatBillingCurrencyCodeGenerator(DuckDBCheckGenerator):
    # Generate currency code format validation for ISO 4217 (national) and string handling (virtual) currencies
    def generateSql(self) -> str:
        return f"""
        SELECT COUNT(CASE
            WHEN {self.rule.column_id} IS NOT NULL
            AND NOT (
                ({self.rule.column_id}::TEXT ~ '^[A-Z]{{3}}$') OR
                (
                    ({self.rule.column_id}::TEXT ~ '^[A-Z][a-zA-Z0-9]*$') OR
                    ({self.rule.column_id}::TEXT ~ '^x_[A-Z][a-zA-Z0-9]*$')
                )
            )
            THEN 1
        END) > 0 as check_failed
        FROM {{table_name}}
        """

    def getCheckType(self) -> str:
        return "format_currency_code"


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


class CheckConformanceRuleGenerator(DuckDBCheckGenerator):
    # Generate check for referencing other conformance rules
    def __init__(self, rule: Rule, check_id: str, conformance_rule_id: str):
        super().__init__(rule, check_id)
        self.conformanceRuleId = conformance_rule_id

    def generateSql(self) -> str:
        # For conformance rule checks, we reference other rules' results
        # This will be handled in the execution phase via dependency resolution
        return f"SELECT FALSE as check_failed -- Conformance rule reference: {self.conformanceRuleId}"

    def getCheckType(self) -> str:
        return "conformance_rule_reference"


class CheckDistinctCountGenerator(DuckDBCheckGenerator):
    # Generate distinct count validation check
    def __init__(self, rule: Rule, check_id: str, column_a_name: str, column_b_name: str, expected_count: int):
        super().__init__(rule, check_id)
        self.columnAName = column_a_name
        self.columnBName = column_b_name
        self.expectedCount = expected_count

    def generateSql(self) -> str:
        return f"""
        SELECT COUNT(*) > 0 as check_failed
        FROM (
            SELECT {self.columnAName}, COUNT(DISTINCT {self.columnBName}) as distinct_count
            FROM {{table_name}}
            GROUP BY {self.columnAName}
            HAVING COUNT(DISTINCT {self.columnBName}) != {self.expectedCount}
        ) violations
        """

    def getCheckType(self) -> str:
        return "distinct_count"


class CheckNationalCurrencyGenerator(DuckDBCheckGenerator):
    # Generate national currency code validation check (ISO 4217)
    def generateSql(self) -> str:
        # ISO 4217 currency codes are 3-letter uppercase codes
        return f"""
        SELECT COUNT(CASE
            WHEN {self.rule.column_id} IS NOT NULL
            AND NOT ({self.rule.column_id}::TEXT ~ '^[A-Z]{{3}}$')
            THEN 1
        END) > 0 as check_failed
        FROM {{table_name}}
        """

    def getCheckType(self) -> str:
        return "national_currency"


class FormatCurrencyGenerator(DuckDBCheckGenerator):
    # Generate currency format validation check
    def generateSql(self) -> str:
        return f"""
        SELECT COUNT(CASE
            WHEN {self.rule.column_id} IS NOT NULL
            AND NOT (
                ({self.rule.column_id}::TEXT ~ '^[A-Z]{{3}}$') OR
                (
                    ({self.rule.column_id}::TEXT ~ '^[A-Z][a-zA-Z0-9]*$') OR
                    ({self.rule.column_id}::TEXT ~ '^x_[A-Z][a-zA-Z0-9]*$')
                )
            )
            THEN 1
        END) > 0 as check_failed
        FROM {{table_name}}
        """

    def getCheckType(self) -> str:
        return "format_currency"


class FormatKeyValueGenerator(DuckDBCheckGenerator):
    # Generate key-value format validation check for JSON-like structures
    def generateSql(self) -> str:
        return f"""
        SELECT COUNT(CASE
            WHEN {self.rule.column_id} IS NOT NULL
            AND NOT (
                {self.rule.column_id}::TEXT ~ '^\\{{.*\\}}$' OR
                {self.rule.column_id}::TEXT = '{{}}'
            )
            THEN 1
        END) > 0 as check_failed
        FROM {{table_name}}
        """

    def getCheckType(self) -> str:
        return "format_key_value"


class FormatUnitGenerator(DuckDBCheckGenerator):
    # Generate unit format validation check
    def generateSql(self) -> str:
        return f"""
        SELECT COUNT(CASE
            WHEN {self.rule.column_id} IS NOT NULL
            AND NOT (
                ({self.rule.column_id}::TEXT ~ '^[A-Z][a-zA-Z0-9]*$') OR
                ({self.rule.column_id}::TEXT ~ '^x_[A-Z][a-zA-Z0-9]*$')
            )
            OR LENGTH({self.rule.column_id}::TEXT) > 50
            THEN 1
        END) > 0 as check_failed
        FROM {{table_name}}
        """

    def getCheckType(self) -> str:
        return "format_unit"


class TypeDateTimeGenerator(DuckDBCheckGenerator):
    # Generate datetime type validation check
    def generateSql(self) -> str:
        return f"""
        SELECT CASE
            WHEN COUNT(*) = 0 THEN FALSE
            ELSE COUNT(CASE
                WHEN typeof({self.rule.column_id}) NOT IN ('TIMESTAMP', 'TIMESTAMP WITH TIME ZONE', 'DATE')
                AND NOT ({self.rule.column_id}::TEXT ~ '^[0-9]{{4}}-[0-1][0-9]-[0-3][0-9]T[0-2][0-9]:[0-5][0-9]:[0-5][0-9]Z$')
                THEN 1
            END) > 0
        END as check_failed
        FROM {{table_name}}
        """

    def getCheckType(self) -> str:
        return "type_datetime"


class FocusToDuckDBSchemaConverter:
    # Central registry for all check types with both generators and check object factories
    CHECK_GENERATORS = {
        "ColumnPresent": {
            "generator": ColumnPresentCheckGenerator,
            "factory": lambda args: "column_required"
        },
        "TypeString": {
            "generator": TypeStringCheckGenerator,
            "factory": lambda args: DataTypeCheck(data_type=DataTypes.STRING)
        },
        "TypeDecimal": {
            "generator": TypeDecimalCheckGenerator,
            "factory": lambda args: DataTypeCheck(data_type=DataTypes.DECIMAL)
        },
        "TypeDateTime": {
            "generator": TypeDateTimeGenerator,
            "factory": lambda args: DataTypeCheck(data_type=DataTypes.DATETIME)
        },
        "CheckValue": {
            "generator": CheckValueGenerator,
            "factory": lambda args: ValueComparisonCheck(
                operator="equals",
                value=args.get("Value")
            )
        },
        "CheckNotValue": {
            "generator": CheckNotValueGenerator,
            "factory": lambda args: ValueComparisonCheck(
                operator="not_equals",
                value=args.get("Value")
            )
        },
        "CheckSameValue": {
            "generator": ColumnComparisonGenerator,
            "factory": lambda args: ValueComparisonCheck(
                operator="equals_column",
                value=args.get("ComparisonColumn")
            )
        },
        "CheckNotSameValue": {
            "generator": ColumnComparisonGenerator,
            "factory": lambda args: ValueComparisonCheck(
                operator="not_equals_column",
                value=args.get("ComparisonColumn")
            )
        },
        "CheckDistinctCount": {
            "generator": CheckDistinctCountGenerator,
            "factory": lambda args: DistinctCountCheck(
                column_a_name=args.get("ColumnAName"),
                column_b_name=args.get("ColumnBName"),
                expected_count=args.get("ExpectedCount")
            )
        },
        "CheckConformanceRule": {
            "generator": CheckConformanceRuleGenerator,
            "factory": lambda args: ConformanceRuleCheck(
                conformance_rule_id=args.get("ConformanceRuleId")
            )
        },
        "CheckNationalCurrency": {
            "generator": CheckNationalCurrencyGenerator,
            "factory": lambda args: FormatCheck(format_type="currency_code")
        },
        "ColumnByColumnEqualsColumnValue": {
            "generator": ColumnComparisonGenerator,
            "factory": lambda args: ColumnComparisonCheck(
                comparison_column=args.get("ComparisonColumn"),
                operator="equals"
            )
        },
        "FormatNumeric": {
            "generator": FormatNumericGenerator,
            "factory": lambda args: FormatCheck(format_type="numeric")
        },
        "FormatDateTime": {
            "generator": FormatDateTimeGenerator,
            "factory": lambda args: FormatCheck(format_type="datetime")
        },
        "FormatString": {
            "generator": FormatStringGenerator,
            "factory": lambda args: FormatCheck(format_type="string")
        },
        "FormatCurrency": {
            "generator": FormatCurrencyGenerator,
            "factory": lambda args: FormatCheck(format_type="currency_code")
        },
        "FormatKeyValue": {
            "generator": FormatKeyValueGenerator,
            "factory": lambda args: FormatCheck(format_type="key_value")
        },
        "FormatUnit": {
            "generator": FormatUnitGenerator,
            "factory": lambda args: FormatCheck(format_type="unit")
        },
        "CheckGreaterOrEqualThanValue": {
            "generator": CheckGreaterOrEqualGenerator,
            "factory": lambda args: ValueComparisonCheck(
                operator="greater_equal",
                value=args.get("Value")
            )
        },
        "CompositeRule": {
            "generator": CompositeRuleGenerator,
            "factory": None  # Composite rules are handled separately
        },
        # Additional check types that were in rule.py
        "FormatBillingCurrencyCode": {
            "generator": FormatBillingCurrencyCodeGenerator,
            "factory": lambda args: FormatCheck(format_type="currency_code")
        },
        "AND": {
            "generator": CompositeRuleGenerator,
            "factory": lambda args: CompositeCheck(
                logic_operator="AND",
                dependency_rule_ids=FocusToDuckDBSchemaConverter._extractDependencyRuleIds(args.get("Items", []))
            )
        },
        "OR": {
            "generator": CompositeRuleGenerator,
            "factory": lambda args: CompositeCheck(
                logic_operator="OR",
                dependency_rule_ids=FocusToDuckDBSchemaConverter._extractDependencyRuleIds(args.get("Items", []))
            )
        },
    }

    @staticmethod
    def _extractDependencyRuleIds(items: List) -> List[str]:
        # Extract ConformanceRuleId values
        dependency_rule_ids = []
        for item in items:
            if isinstance(item, dict) and item.get("CheckFunction") == "CheckConformanceRule":
                rule_id = item.get("ConformanceRuleId")
                if rule_id:
                    dependency_rule_ids.append(rule_id)
        return dependency_rule_ids

    @classmethod
    def getCheckFunctionMappings(cls):
        # Central method that returns check function mappings using the registry
        mappings = {}
        for check_function, config in cls.CHECK_GENERATORS.items():
            if config["factory"] is not None:
                mappings[check_function] = config["factory"]
        return mappings

    # Dispatch map for check types to generator configuration
    CHECK_TYPE_DISPATCH = {
        # Simple string checks
        "column_required": ("ColumnPresent", lambda rule, check_id, check: (rule, check_id)),

        # DataTypeCheck mappings
        (DataTypeCheck, DataTypes.DECIMAL): ("TypeDecimal", lambda rule, check_id, check: (rule, check_id)),
        (DataTypeCheck, DataTypes.STRING): ("TypeString", lambda rule, check_id, check: (rule, check_id)),
        (DataTypeCheck, DataTypes.DATETIME): ("TypeDateTime", lambda rule, check_id, check: (rule, check_id)),

        # ValueComparisonCheck mappings
        (ValueComparisonCheck, "not_equals"): ("CheckNotValue", lambda rule, check_id, check: (rule, check_id, check.value)),
        (ValueComparisonCheck, "equals"): ("CheckValue", lambda rule, check_id, check: (rule, check_id, check.value)),
        (ValueComparisonCheck, "greater_equal"): ("CheckGreaterOrEqualThanValue", lambda rule, check_id, check: (rule, check_id, check.value)),
        (ValueComparisonCheck, "not_equals_column"): ("CheckNotSameValue", lambda rule, check_id, check: (rule, check_id, check.value, "not_equals")),
        (ValueComparisonCheck, "equals_column"): ("CheckSameValue", lambda rule, check_id, check: (rule, check_id, check.value, "equals")),

        # FormatCheck mappings
        (FormatCheck, "numeric"): ("FormatNumeric", lambda rule, check_id, check: (rule, check_id)),
        (FormatCheck, "datetime"): ("FormatDateTime", lambda rule, check_id, check: (rule, check_id)),
        (FormatCheck, "string"): ("FormatString", lambda rule, check_id, check: (rule, check_id)),
        (FormatCheck, "currency_code"): ("FormatBillingCurrencyCode", lambda rule, check_id, check: (rule, check_id)),
        (FormatCheck, "key_value"): ("FormatKeyValue", lambda rule, check_id, check: (rule, check_id)),
        (FormatCheck, "unit"): ("FormatUnit", lambda rule, check_id, check: (rule, check_id)),

        # Other check types
        DistinctCountCheck: ("CheckDistinctCount", lambda rule, check_id, check: (rule, check_id, check.column_a_name, check.column_b_name, check.expected_count)),
        ConformanceRuleCheck: ("CheckConformanceRule", lambda rule, check_id, check: (rule, check_id, check.conformance_rule_id)),
        ColumnComparisonCheck: ("ColumnByColumnEqualsColumnValue", lambda rule, check_id, check: (rule, check_id, check.comparison_column, check.operator)),
    }

    @classmethod
    def __generate_duckdb_check__(cls, rule: Rule, check_id: str) -> Optional[DuckDBColumnCheck]:
        check = rule.check

        # Handle CompositeCheck separately (needs dependency results)
        if isinstance(check, CompositeCheck):
            return None

        # Try dispatch by simple string match
        if isinstance(check, str):
            dispatch_config = cls.CHECK_TYPE_DISPATCH.get(check)
        # Try dispatch by (type, attribute) tuple
        elif isinstance(check, (DataTypeCheck, ValueComparisonCheck, FormatCheck)):
            if isinstance(check, DataTypeCheck):
                key = (DataTypeCheck, check.data_type)
            elif isinstance(check, ValueComparisonCheck):
                key = (ValueComparisonCheck, check.operator)
            elif isinstance(check, FormatCheck):
                key = (FormatCheck, check.format_type)
            dispatch_config = cls.CHECK_TYPE_DISPATCH.get(key)
        # Try dispatch by type only
        else:
            dispatch_config = cls.CHECK_TYPE_DISPATCH.get(type(check))

        if dispatch_config:
            generator_name, arg_builder = dispatch_config
            generator_class = cls.CHECK_GENERATORS[generator_name]["generator"]
            args = arg_builder(rule, check_id, check)
            generator = generator_class(*args)
            return generator.generateCheck()

        return None

    @classmethod
    def __generate_checks__(
        cls, rules: List[Rule]
    ) -> List[DuckDBColumnCheck]:
        # Generate DuckDB validation checks using registry
        checks = []

        for rule in rules:
            check = cls.__generate_duckdb_check__(rule, rule.check_id)
            if check:
                checks.append(check)

        return checks

    @classmethod
    def generateDuckDBValidation(
        cls,
        rules: List[Union[Rule, InvalidRule]],
    ) -> tuple[List[DuckDBColumnCheck], Dict[str, ChecklistObject], List[str]]:
        log = logging.getLogger(f"{__name__}.{cls.__name__}")
        log.info("Generating DuckDB validation checks for %d rules", len(rules))

        startTime = time.time()
        checks = []
        checklist = {}
        compositeRuleIds = []
        invalidRuleCount = 0
        dynamicRuleCount = 0
        pendingRuleCount = 0

        for rule in rules:
            if isinstance(rule, InvalidRule):
                log.debug("Processing invalid rule: %s", rule.rule_path)
                checklist[rule.rule_path] = ChecklistObject(
                    check_name=os.path.splitext(os.path.basename(rule.rule_path))[0],
                    column_id="Unknown",
                    error=f"{rule.error_type}: {rule.error}",
                    status=ChecklistObjectStatus.ERRORED,
                    rule_ref=rule,
                )
                invalidRuleCount += 1
                continue

            # Check if this is a dynamic rule (marked during loading)
            is_dynamic_rule = hasattr(rule, '_rule_type') and getattr(rule, '_rule_type', '').lower() == "dynamic"

            if is_dynamic_rule:
                log.debug("Skipping dynamic rule: %s", rule.check_id)
                dynamicRuleCount += 1
            else:
                log.debug("Processing static rule: %s", rule.check_id)
                pendingRuleCount += 1

            # Track composite rules separately for efficient processing
            if isinstance(rule.check, CompositeCheck):
                compositeRuleIds.append(rule.check_id)

            checklist[rule.check_id] = ChecklistObject(
                check_name=rule.check_id,
                column_id=rule.column_id,
                friendly_name=rule.check_friendly_name,
                status=ChecklistObjectStatus.SKIPPED if is_dynamic_rule else ChecklistObjectStatus.PENDING,
                rule_ref=rule,
                reason="Dynamic rule - skipped in static validation" if is_dynamic_rule else None
            )

        # Generate validation checks using registry
        log.debug("Generating validation checks for %d pending rules", pendingRuleCount)
        validationChecks = cls.__generate_checks__([cl.rule_ref for cl in checklist.values() if cl.status == ChecklistObjectStatus.PENDING])
        checks.extend(validationChecks)

        generationTime = time.time() - startTime
        log.info("DuckDB validation generation completed in %.3f seconds", generationTime)
        log.info("  Generated checks: %d", len(checks))
        log.info("  Pending rules: %d", pendingRuleCount)
        log.info("  Composite rules: %d", len(compositeRuleIds))
        log.info("  Dynamic rules (skipped): %d", dynamicRuleCount)
        log.info("  Invalid rules: %d", invalidRuleCount)

        return checks, checklist, compositeRuleIds

    @staticmethod
    def executeDuckDBValidation(
        connection: duckdb.DuckDBPyConnection,
        tableName: str,
        checks: List[DuckDBColumnCheck],
        checklist: Dict[str, ChecklistObject],
        compositeRuleIds: Optional[List[str]] = None,
        dependency_results: Optional[Dict[str, bool]] = None
    ) -> Dict[str, ChecklistObject]:
        log = logging.getLogger(f"{__name__}.FocusToDuckDBSchemaConverter")
        log.info("Executing DuckDB validation with %d checks on table: %s", len(checks), tableName)

        startTime = time.time()
        executedCount = 0
        passedCount = 0
        failedCount = 0
        errorCount = 0

        # Create lookup dictionary for O(1) access: maps (column_name, check_id) to checklist item
        checklistLookup = {}
        for check_id, item in checklist.items():
            if hasattr(item.rule_ref, 'check'):
                checklistLookup[(item.column_id, check_id)] = item

        # Execute DuckDB validation checks
        for check in checks:
            executedCount += 1
            log.debug("Executing check %d/%d: %s on column %s", executedCount, len(checks), check.checkType, check.columnName)
            try:
                # Replace table name placeholder in SQL
                sql = check.checkSql.replace('{table_name}', tableName)
                log.debug("Executing SQL: %s", sql[:100] + "..." if len(sql) > 100 else sql)

                checkStartTime = time.time()
                result = connection.execute(sql).fetchone()
                checkDuration = time.time() - checkStartTime

                # Extract check_id from error message (format: "check_id: friendly_name")
                check_id = check.errorMessage.split(':')[0].strip() if ':' in check.errorMessage else None

                # O(1) lookup using check_id and column name
                checklistItem = None
                if check_id:
                    checklistItem = checklistLookup.get((check.columnName, check_id))

                if checklistItem:
                    if result and result[0]:  # check_failed is True
                        checklistItem.status = ChecklistObjectStatus.FAILED
                        checklistItem.error = check.errorMessage
                        failedCount += 1
                        log.debug("Check FAILED (%.3fs): %s on %s", checkDuration, check.checkType, check.columnName)
                    else:
                        checklistItem.status = ChecklistObjectStatus.PASSED
                        passedCount += 1
                        log.debug("Check PASSED (%.3fs): %s on %s", checkDuration, check.checkType, check.columnName)
                else:
                    log.warning("Could not find matching checklist item for check: %s on %s", check.checkType, check.columnName)

            except Exception as e:
                log.error("DuckDB check failed: %s on column %s - %s", check.checkType, check.columnName, str(e))
                errorCount += 1

                # Extract check_id and lookup checklist item
                check_id = check.errorMessage.split(':')[0].strip() if ':' in check.errorMessage else None
                if check_id:
                    errorItem = checklistLookup.get((check.columnName, check_id))
                    if errorItem:
                        errorItem.status = ChecklistObjectStatus.ERRORED
                        errorItem.error = f"DuckDB validation error: {str(e)}"

            # Progress logging
            if executedCount % 100 == 0:
                log.debug("Progress: %d/%d checks executed", executedCount, len(checks))

        # Handle composite rules after basic validation
        if compositeRuleIds:
            log.debug("Executing %d composite rules...", len(compositeRuleIds))
            FocusToDuckDBSchemaConverter._executeCompositeRules(checklist, compositeRuleIds, dependency_results or {})

        executionTime = time.time() - startTime
        log.info("DuckDB validation execution completed in %.3f seconds", executionTime)
        log.info("Execution summary:")
        log.info("  Total checks executed: %d", executedCount)
        log.info("  Passed: %d", passedCount)
        log.info("  Failed: %d", failedCount)
        log.info("  Errors: %d", errorCount)
        log.info("  Success rate: %.1f%%", (passedCount / executedCount * 100) if executedCount > 0 else 0)

        return checklist

    @staticmethod
    def _executeCompositeRules(checklist: Dict[str, ChecklistObject], compositeRuleIds: List[str], dependency_results: Dict[str, bool]):
        """Execute composite rule validation based on dependency results."""
        for check_id in compositeRuleIds:
            item = checklist.get(check_id)
            if not item or not hasattr(item.rule_ref, 'check'):
                continue

            composite_check = item.rule_ref.check
            if not isinstance(composite_check, CompositeCheck):
                continue

            logic_operator = composite_check.logic_operator
            dependency_rule_ids = composite_check.dependency_rule_ids

            try:
                if logic_operator == "AND":
                    assessment_func = all
                elif logic_operator == "OR":
                    assessment_func = any
                else:
                    raise FocusNotImplementedError(f"Unsupported logic operator: {logic_operator}")

                # All dependencies must pass for composite to pass (SKIPPED counts as PASSED)
                all_passed = assessment_func(
                    dep_id in checklist and checklist[dep_id].status in [ChecklistObjectStatus.PASSED, ChecklistObjectStatus.SKIPPED]
                    for dep_id in dependency_rule_ids
                )
                item.status = ChecklistObjectStatus.PASSED if all_passed else ChecklistObjectStatus.FAILED

                if item.status == ChecklistObjectStatus.FAILED:
                    item.error = f"Composite rule {logic_operator} logic failed for dependencies: {dependency_rule_ids}"

            except Exception as e:
                item.status = ChecklistObjectStatus.ERRORED
                item.error = f"Error evaluating composite rule: {str(e)}"
