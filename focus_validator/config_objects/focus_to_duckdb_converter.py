import json
import logging
import re
import textwrap
import time
from abc import ABC, abstractmethod
from types import MappingProxyType, SimpleNamespace
from typing import Any, Callable, ClassVar, Dict, List, Optional, Tuple, Union

import duckdb  # type: ignore[import-untyped]

from focus_validator.exceptions import InvalidRuleException
from focus_validator.utils.download_currency_codes import get_currency_codes

from .plan_builder import EdgeCtx, ValidationPlan
from .rule import ModelRule

log = logging.getLogger(__name__)


def _compact_json(data: dict, max_len: int = 600) -> str:
    s = json.dumps(data, indent=2, ensure_ascii=False)
    if len(s) <= max_len:
        return s
    return s[:max_len] + " ... (truncated)"


# --- DuckDB check generators -------------------------------------------------


class DuckDBColumnCheck:
    def __init__(
        self,
        rule_id: str,
        rule: ModelRule,
        check_type: str,
        check_sql: str,
        error_message: str,
        nested_checks: List["DuckDBColumnCheck"] | None,
        special_executor: Optional[Callable] = None,
        meta: Optional[Dict[str, Any]] = None,
        exec_mode: Optional[str] = None,
        referenced_rule_id: Optional[str] = None,
        nested_check_handler: Optional[Callable] = None,
    ) -> None:
        self.rule_id = rule_id
        self.rule = rule
        self.checkType = check_type
        self.checkSql = check_sql
        self.errorMessage = error_message
        self.nestedChecks = nested_checks or []
        self.nestedCheckHandler = nested_check_handler
        self.meta = meta or {}  # generator name, row_condition_sql, exec_mode, etc.
        self.special_executor = special_executor
        self.exec_mode = (
            exec_mode or "requirement"
        )  # "requirement" / "condition" / "reference"
        self.referenced_rule_id = referenced_rule_id  # if applicable
        self.force_fail_due_to_upstream: Optional[
            Dict[str, Any]
        ] = None  # For upstream dependency failures


class DuckDBCheckGenerator(ABC):
    # Abstract base class for generating DuckDB validation checks
    RESERVED: ClassVar[set[str]] = {"rule", "rule_id", "params"}
    REQUIRED_KEYS: ClassVar[set[str]] = set()  # subclasses may override
    DEFAULTS: ClassVar[Dict[str, Any]] = {}  # subclasses may override
    FREEZE_PARAMS: ClassVar[bool] = True  # make params read-only

    def __init__(self, rule, rule_id: str, **kwargs: Any) -> None:
        self.rule = rule
        self.rule_id = rule_id
        self.errorMessage: Optional[str] = None
        self.nestedChecks: List[Any] = []
        self.nestedCheckHandler: Optional[Callable] = None
        self.row_condition_sql: Optional[str] = None
        self.compile_condition = kwargs.pop("compile_condition", None)
        self.child_builder = kwargs.pop("child_builder", None)
        self.breadcrumb = kwargs.pop("breadcrumb", rule_id)
        self.parent_results_by_idx = kwargs.pop("parent_results_by_idx", {}) or {}
        self.parent_edges = kwargs.pop("parent_edges", ()) or ()
        self.plan = kwargs.pop("plan", None)
        self.row_condition_sql = kwargs.pop("row_condition_sql", None)
        self.exec_mode = kwargs.pop("exec_mode", "requirement")
        # Validate required keys (allow defaults to satisfy)
        missing = self.REQUIRED_KEYS - (set(kwargs) | set(self.DEFAULTS))
        if missing:
            raise KeyError(f"Missing required generator args: {sorted(missing)}")

        # Merge defaults → kwargs wins
        merged = {**self.DEFAULTS, **kwargs}

        # Guard reserved collisions
        for k in merged:
            if k in self.RESERVED:
                raise ValueError(f"Param name '{k}' is reserved")

        self.p = MappingProxyType(dict(merged))
        self.params = SimpleNamespace(**merged)

    @abstractmethod
    def generateSql(self) -> str:
        # Generate the SQL query for this check type
        pass

    @abstractmethod
    def getCheckType(self) -> str:
        # Return the check type identifier
        pass

    def generateCheck(self) -> DuckDBColumnCheck:
        """
        Build a DuckDBColumnCheck describing this rule’s validation.
        - For leaves: check_sql holds the final SELECT ... AS violations with any effective row condition applied by the generator.
        - For composites: nested_checks holds the children; check_sql is typically None or a trivial SELECT.
        - For special/reference checks: special_executor/exec_mode can be set by the generator.
        """
        # 1) Build own SQL (for leaf) or composite shell (composite often still returns trivial sql)
        sql = self.generateSql()

        # 2) Normalize nested checks: keep existing DuckDBColumnCheck objects as-is,
        #    otherwise wrap minimal structure (rare; depends on how child_builder returns)
        child_checks: List[DuckDBColumnCheck] = []
        for chk in getattr(self, "nestedChecks", []) or []:
            if isinstance(chk, DuckDBColumnCheck):
                child_checks.append(chk)
            else:
                # best-effort wrapping from generator-returned "check-like" objects
                child_checks.append(
                    DuckDBColumnCheck(
                        rule_id=getattr(chk, "rule_id", getattr(self, "rule_id", "")),
                        rule=getattr(chk, "rule", getattr(self, "rule", None)) or ModelRule(),  # type: ignore
                        check_type=getattr(
                            chk, "checkType", getattr(chk, "check_type", "unknown")
                        ),
                        check_sql=getattr(
                            chk, "checkSql", getattr(chk, "check_sql", None)
                        )
                        or "",
                        error_message=getattr(chk, "errorMessage", None)
                        or "No error message",
                        nested_checks=getattr(chk, "nestedChecks", None),
                        nested_check_handler=getattr(chk, "nestedCheckHandler", None),
                        meta=getattr(chk, "meta", None),
                        special_executor=getattr(chk, "special_executor", None),
                        exec_mode=getattr(chk, "exec_mode", None),
                        referenced_rule_id=getattr(chk, "referenced_rule_id", None),
                    )
                )

        # 3) Compose meta for explainability
        meta = {
            "generator": self.__class__.__name__,
            "row_condition_sql": getattr(self, "row_condition_sql", None),
            "exec_mode": getattr(self, "exec_mode", "requirement"),
        }

        # 4) Create the final check object
        chk = DuckDBColumnCheck(
            rule_id=self.rule_id,
            rule=self.rule,
            check_type=self.getCheckType(),
            check_sql=sql,
            error_message=getattr(self, "errorMessage", None) or "No error message",
            nested_checks=child_checks or None,
            nested_check_handler=getattr(self, "nestedCheckHandler", None),
            meta=meta,
            special_executor=getattr(self, "special_executor", None),
            exec_mode=getattr(self, "exec_mode", None),
            referenced_rule_id=getattr(self, "referenced_rule_id", None),
        )

        # 5) Transfer generator-specific attributes to the check object
        if hasattr(self, "force_fail_due_to_upstream"):
            chk.force_fail_due_to_upstream = self.force_fail_due_to_upstream

        return chk

    def _apply_condition(self, violation_pred_sql: str) -> str:
        """
        Given a boolean predicate that defines 'row is a violation',
        AND it with the effective row_condition_sql if present.
        """
        cond = (self.row_condition_sql or "").strip()
        if not cond:
            return violation_pred_sql
        return f"(({violation_pred_sql})) AND ({cond})"

    def _lit(self, v) -> str:
        if v is None:
            return "NULL"
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            return str(v)
        return "'" + str(v).replace("'", "''") + "'"

    def generatePredicate(self) -> str | None:
        """
        Return a SQL boolean expression (no SELECT), suitable for WHERE filters.
        Default: None (generator not usable as a condition).
        Subclasses that support conditions should override.
        """
        return None


class SkippedCheck(DuckDBCheckGenerator):
    REQUIRED_KEYS = set()

    def run(self, _conn) -> tuple[bool, dict]:
        return True, {"skipped": True, "reason": self.errorMessage, "violations": 0}

    def generateSql(self):
        self.errorMessage = "FormatUnit check is dynamic"
        return None

    def getCheckType(self) -> str:
        return "skipped_check"


class SkippedDynamicCheck(SkippedCheck):
    def __init__(self, rule, rule_id: str, **kwargs: Any) -> None:
        super().__init__(rule, rule_id, **kwargs)
        self.errorMessage = "dynamic rule"


class SkippedNonApplicableCheck(SkippedCheck):
    def __init__(self, rule, rule_id: str, **kwargs: Any) -> None:
        super().__init__(rule, rule_id, **kwargs)
        self.errorMessage = "non applicable rule"


class ColumnPresentCheckGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnName"}

    def generateSql(self) -> str:
        col = self.params.ColumnName
        message = self.errorMessage or f"Column '{col}' MUST be present in the table."
        self.errorMessage = message  # <-- make sure run_check can see it
        msg_sql = message.replace("'", "''")

        return f"""
        WITH col_check AS (
            SELECT COUNT(*) AS found
            FROM information_schema.columns
            WHERE table_name = '{{table_name}}'
              AND column_name = '{col}'
        )
        SELECT
            CASE WHEN found = 0 THEN 1 ELSE 0 END AS violations,
            CASE WHEN found = 0 THEN '{msg_sql}' END AS error_message
        FROM col_check
        """

    def getCheckType(self) -> str:
        return "column_presence"


class TypeStringCheckGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnName"}

    # Generate type string validation check
    def generateSql(self) -> str:
        message = (
            self.errorMessage
            or f"{self.params.ColumnName} MUST be of type VARCHAR (string)."
        )
        msg_sql = message.replace("'", "''")
        col = f"{self.params.ColumnName}"

        return f"""
        WITH invalid AS (
            SELECT 1
            FROM {{table_name}}
            WHERE {col} IS NOT NULL
              AND typeof({col}) != 'VARCHAR'
        )
        SELECT
            COUNT(*) AS violations,
            CASE WHEN COUNT(*) > 0 THEN '{msg_sql}' END AS error_message
        FROM invalid
        """

    def getCheckType(self) -> str:
        return "type_string"


class TypeDecimalCheckGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnName"}

    # Generate type decimal validation check
    def generateSql(self) -> str:
        message = (
            self.errorMessage
            or f"{self.params.ColumnName} MUST be of type DECIMAL, DOUBLE, or FLOAT."
        )
        msg_sql = message.replace("'", "''")
        col = f"{self.params.ColumnName}"

        return f"""
        WITH invalid AS (
            SELECT 1
            FROM {{table_name}}
            WHERE {col} IS NOT NULL
              AND typeof({col}) NOT IN ('DECIMAL', 'DOUBLE', 'FLOAT')
        )
        SELECT
            COUNT(*) AS violations,
            CASE WHEN COUNT(*) > 0 THEN '{msg_sql}' END AS error_message
        FROM invalid
        """

    def getCheckType(self) -> str:
        return "type_decimal"


class TypeDateTimeGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnName"}

    # Validate the *type* is datetime-like:
    # - Accept native DATE, TIMESTAMP, TIMESTAMP WITH TIME ZONE
    # - Also accept ISO 8601 UTC text: YYYY-MM-DDTHH:mm:ssZ
    def generateSql(self) -> str:
        message = (
            self.errorMessage
            or f"{self.params.ColumnName} MUST be a DATE/TIMESTAMP (with/without TZ) "
            f"or an ISO 8601 UTC string (YYYY-MM-DDTHH:mm:ssZ)."
        )
        msg_sql = message.replace("'", "''")
        col = f"{self.params.ColumnName}"

        return f"""
        WITH invalid AS (
            SELECT 1
            FROM {{table_name}}
            WHERE {col} IS NOT NULL
              AND typeof({col}) NOT IN ('TIMESTAMP', 'TIMESTAMP WITH TIME ZONE', 'DATE')
              AND NOT ({col}::TEXT ~ '^[0-9]{{4}}-[0-1][0-9]-[0-3][0-9]T[0-2][0-9]:[0-5][0-9]:[0-5][0-9]Z$')
        )
        SELECT
            COUNT(*) AS violations,
            CASE WHEN COUNT(*) > 0 THEN '{msg_sql}' END AS error_message
        FROM invalid
        """

    def getCheckType(self) -> str:
        return "type_datetime"


class FormatNumericGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnName"}

    # Generate numeric format validation check
    def generateSql(self) -> str:
        message = (
            self.errorMessage
            or f"{self.params.ColumnName} MUST be a numeric value (optional +/- sign, optional decimal)."
        )
        msg_sql = message.replace("'", "''")
        col = f"{self.params.ColumnName}"

        return f"""
        WITH invalid AS (
            SELECT 1
            FROM {{table_name}}
            WHERE {col} IS NOT NULL
              AND NOT (
                TRIM({col}::TEXT) ~ '^[+-]?([0-9]*[.])?[0-9]+$'
              )
        )
        SELECT
            COUNT(*) AS violations,
            CASE WHEN COUNT(*) > 0 THEN '{msg_sql}' END AS error_message
        FROM invalid
        """

    def getCheckType(self) -> str:
        return "format_numeric"


class FormatStringGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnName"}

    # Generate string format validation check for ASCII characters
    def generateSql(self) -> str:
        message = (
            self.errorMessage
            or f"{self.params.ColumnName} MUST contain only ASCII characters."
        )
        msg_sql = message.replace("'", "''")
        col = f"{self.params.ColumnName}"

        return f"""
        WITH invalid AS (
            SELECT {col}::TEXT AS value
            FROM {{table_name}}
            WHERE {col} IS NOT NULL
              AND NOT ({col}::TEXT ~ '^[\\x00-\\x7F]*$')  -- Only ASCII characters (0-127)
        )
        SELECT
            COUNT(*) AS violations,
            CASE WHEN COUNT(*) > 0 THEN '{msg_sql}' END AS error_message
        FROM invalid
        """

    def getCheckType(self) -> str:
        return "format_string"


class FormatDateTimeGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnName"}

    # Generate datetime format validation check for ISO 8601 extended format with UTC (YYYY-MM-DDTHH:mm:ssZ)
    def generateSql(self) -> str:
        message = (
            self.errorMessage
            or f"{self.params.ColumnName} MUST be in ISO 8601 UTC format: YYYY-MM-DDTHH:mm:ssZ"
        )
        msg_sql = message.replace("'", "''")
        col = f"{self.params.ColumnName}"

        return f"""
        WITH invalid AS (
            SELECT 1
            FROM {{table_name}}
            WHERE {col} IS NOT NULL
              AND NOT (
                {col}::TEXT ~ '^[0-9]{{4}}-[0-1][0-9]-[0-3][0-9]T[0-2][0-9]:[0-5][0-9]:[0-5][0-9]Z$'
              )
        )
        SELECT
            COUNT(*) AS violations,
            CASE WHEN COUNT(*) > 0 THEN '{msg_sql}' END AS error_message
        FROM invalid
        """

    def getCheckType(self) -> str:
        return "format_datetime"


class FormatBillingCurrencyCodeGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnName"}

    def generateSql(self) -> str:
        message = (
            self.errorMessage
            or f"{self.params.ColumnName} MUST be a valid ISO 4217 currency code (e.g., USD, EUR)."
        )
        msg_sql = message.replace("'", "''")
        col = f"{self.params.ColumnName}"

        # Get valid currency codes from CSV file
        valid_codes = get_currency_codes()
        # Create SQL IN clause with properly quoted currency codes
        codes_list = "', '".join(sorted(valid_codes))

        return f"""
        WITH invalid AS (
            SELECT 1
            FROM {{table_name}}
            WHERE {col} IS NOT NULL
              AND TRIM({col}::TEXT) NOT IN ('{codes_list}')
        )
        SELECT
            COUNT(*) AS violations,
            CASE WHEN COUNT(*) > 0 THEN '{msg_sql}' END AS error_message
        FROM invalid
        """

    def getCheckType(self) -> str:
        return "format_currency_code"


class FormatKeyValueGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnName"}

    # Generate key-value format validation check for JSON-like structures
    def generateSql(self) -> str:
        col = self.params.ColumnName
        self.required_columns = [col]  # NEW
        # Your desired message:
        self.errorMessage = "Format not in key value"

        # Example predicate: invalid if non-null and not key=value;key=value...
        # Adjust to your accepted separators/pattern.
        # DuckDB supports regexp via ~ / !~ operators.
        pattern = r"^[^=;]+=[^=;]+(?:;[^=;]+=[^=;]+)*$"
        cond = f"{col} IS NOT NULL AND NOT ({col} ~ '{pattern}')"

        return f"""
            SELECT
            COUNT(*) FILTER (WHERE {cond}) AS violations
            FROM {{table_name}}
        """

    def getCheckType(self) -> str:
        return "format_key_value"


class FormatCurrencyGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnName"}

    # Generate national currency code validation check (ISO 4217)
    def generateSql(self) -> str:
        message = (
            self.errorMessage
            or f"{self.params.ColumnName} MUST be a valid ISO 4217 currency code (3 uppercase letters, e.g. USD, EUR)."
        )
        msg_sql = message.replace("'", "''")
        col = f"{self.params.ColumnName}"

        return f"""
        WITH invalid AS (
            SELECT 1
            FROM {{table_name}}
            WHERE {col} IS NOT NULL
              AND NOT (TRIM({col}::TEXT) ~ '^[A-Z]{{3}}$')
        )
        SELECT
            COUNT(*) AS violations,
            CASE WHEN COUNT(*) > 0 THEN '{msg_sql}' END AS error_message
        FROM invalid
        """

    def getCheckType(self) -> str:
        return "national_currency"


class FormatUnitGenerator(SkippedCheck):
    REQUIRED_KEYS = {"ColumnName"}

    def __init__(self, rule, rule_id: str, **kwargs: Any) -> None:
        super().__init__(rule, rule_id, **kwargs)
        self.errorMessage = "FormatUnit rule is dynamic"


class CheckValueGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnName", "Value"}

    def generateSql(self) -> str:
        if self.params.Value is None:
            message = self.errorMessage or f"{self.params.ColumnName} MUST be NULL."
            condition = f"{self.params.ColumnName} IS NOT NULL"
        else:
            val = str(self.params.Value).replace("'", "''")
            message = (
                self.errorMessage
                or f"{self.params.ColumnName} MUST equal '{self.params.Value}'."
            )
            condition = f"{self.params.ColumnName} != '{val}'"

        # Apply conditional logic if present
        condition = self._apply_condition(condition)

        msg_sql = message.replace("'", "''")

        return f"""
        WITH invalid AS (
            SELECT 1
            FROM {{table_name}}
            WHERE {condition}
        )
        SELECT
            COUNT(*) AS violations,
            CASE WHEN COUNT(*) > 0 THEN '{msg_sql}' END AS error_message
        FROM invalid
        """

    def getCheckType(self) -> str:
        return "check_value"

    def generatePredicate(self) -> str | None:
        """
        Condition mode: return a boolean predicate that selects rows where
        ColumnName == Value (or IS NULL when Value is None).
        """
        if getattr(self, "exec_mode", "requirement") != "condition":
            return None

        col = self.params.ColumnName
        v = self.params.Value

        # use base literalizer if present; fall back to a simple one
        _lit = getattr(self, "_lit", None)
        if _lit is None:

            def _lit(x):
                if x is None:
                    return "NULL"
                if isinstance(x, (int, float)) and not isinstance(x, bool):
                    return str(x)
                return "'" + str(x).replace("'", "''") + "'"

        if v is None:
            return f"{col} IS NULL"
        else:
            return f"{col} = {_lit(v)}"


class CheckNotValueGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnName", "Value"}

    def generateSql(self) -> str:
        # Default error message if none provided
        if self.params.Value is None:
            message = self.errorMessage or f"{self.params.ColumnName} MUST NOT be NULL."
            condition = f"{self.params.ColumnName} IS NULL"
        else:
            message = (
                self.errorMessage
                or f"{self.params.ColumnName} MUST NOT be '{self.params.Value}'."
            )
            # escape single quotes in Value for SQL literal safety
            val = str(self.params.Value).replace("'", "''")
            # Fix: Use <> (not equals) and handle NULLs properly for CheckNotValue
            condition = f"({self.params.ColumnName} IS NOT NULL AND {self.params.ColumnName} = '{val}')"

        # Apply conditional logic if present
        condition = self._apply_condition(condition)

        msg_sql = message.replace("'", "''")

        return f"""
        WITH invalid AS (
            SELECT 1
            FROM {{table_name}}
            WHERE {condition}
        )
        SELECT
            COUNT(*) AS violations,
            CASE WHEN COUNT(*) > 0 THEN '{msg_sql}' END AS error_message
        FROM invalid
        """

    def getCheckType(self) -> str:
        return "check_not_value"

    def generatePredicate(self) -> str | None:
        if self.exec_mode != "condition":
            return None
        col = self.params.ColumnName
        val = self.params.Value
        # CONDITION predicate: rows where requirement applies
        # For "CheckNotValue", the natural condition is: col IS NOT NULL (if Value is NULL) or col <> Value
        if val is None:
            return f"{col} IS NOT NULL"
        else:
            return f"({col} IS NOT NULL AND {col} <> {self._lit(val)})"


class CheckSameValueGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnAName", "ColumnBName"}

    def generateSql(self) -> str:
        message = (
            self.errorMessage
            or f"{self.params.ColumnAName} and {self.params.ColumnBName} MUST have the same value."
        )
        msg_sql = message.replace("'", "''")
        col_a = f"{self.params.ColumnAName}"
        col_b = f"{self.params.ColumnBName}"

        condition = (
            f"{col_a} IS NOT NULL AND {col_b} IS NOT NULL AND {col_a} <> {col_b}"
        )
        # Apply conditional logic if present
        condition = self._apply_condition(condition)

        return f"""
        WITH invalid AS (
            SELECT 1
            FROM {{table_name}}
            WHERE {condition}
        )
        SELECT
            COUNT(*) AS violations,
            CASE WHEN COUNT(*) > 0 THEN '{msg_sql}' END AS error_message
        FROM invalid
        """

    def getCheckType(self) -> str:
        return "column_comparison_equals"

    def generatePredicate(self) -> str | None:
        """
        Condition mode: select rows where both columns are non-null and equal.
        """
        if getattr(self, "exec_mode", "requirement") != "condition":
            return None

        col_a = self.params.ColumnAName
        col_b = self.params.ColumnBName
        return f"{col_a} IS NOT NULL AND {col_b} IS NOT NULL AND {col_a} = {col_b}"


class CheckNotSameValueGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnAName", "ColumnBName"}

    def generateSql(self) -> str:
        message = (
            self.errorMessage
            or f"{self.params.ColumnAName} and {self.params.ColumnBName} MUST NOT have the same value."
        )
        msg_sql = message.replace("'", "''")
        col_a = f"{self.params.ColumnAName}"
        col_b = f"{self.params.ColumnBName}"

        condition = f"{col_a} IS NOT NULL AND {col_b} IS NOT NULL AND {col_a} = {col_b}"
        # Apply conditional logic if present
        condition = self._apply_condition(condition)

        return f"""
        WITH invalid AS (
            SELECT 1
            FROM {{table_name}}
            WHERE {condition}
        )
        SELECT
            COUNT(*) AS violations,
            CASE WHEN COUNT(*) > 0 THEN '{msg_sql}' END AS error_message
        FROM invalid
        """

    def getCheckType(self) -> str:
        return "column_comparison_not_equals"

    def generatePredicate(self) -> str | None:
        """
        Condition mode: select rows where both columns are non-null and not equal.
        """
        if getattr(self, "exec_mode", "requirement") != "condition":
            return None

        col_a = self.params.ColumnAName
        col_b = self.params.ColumnBName
        return f"{col_a} IS NOT NULL AND {col_b} IS NOT NULL AND {col_a} <> {col_b}"


class CheckDecimalValueGenerator(SkippedCheck):
    def __init__(self, rule, rule_id: str, **kwargs: Any) -> None:
        super().__init__(rule, rule_id, **kwargs)
        self.errorMessage = "no defined check rule"


class ColumnByColumnEqualsColumnValueGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnAName", "ColumnBName", "ResultColumnName"}

    def generateSql(self) -> str:
        a = self.params.ColumnAName
        b = self.params.ColumnBName
        r = self.params.ResultColumnName
        self.errorMessage = f"Expected {r} = {a} * {b}"

        condition = f"{a} IS NOT NULL AND {b} IS NOT NULL AND {r} IS NOT NULL AND ({a} * {b}) <> {r}"
        # Apply conditional logic if present
        condition = self._apply_condition(condition)

        return f"""
        SELECT
          COUNT(*) FILTER (
            WHERE {condition}
          ) AS violations
        FROM {{table_name}}
        """

    def getCheckType(self) -> str:
        return "column_by_column_equals_column_value"

    def generatePredicate(self) -> str | None:
        """
        Condition mode: select rows where all three columns are non-null
        AND the equality holds: (a * b) = r
        """
        if getattr(self, "exec_mode", "requirement") != "condition":
            return None

        a = self.params.ColumnAName
        b = self.params.ColumnBName
        r = self.params.ResultColumnName

        return f"{a} IS NOT NULL AND {b} IS NOT NULL AND {r} IS NOT NULL AND ({a} * {b}) = {r}"


class CheckGreaterOrEqualGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnName", "Value"}

    def generateSql(self) -> str:
        message = (
            self.errorMessage
            or f"{self.params.ColumnName} MUST be greater than or equal to {self.params.Value}."
        )
        msg_sql = message.replace("'", "''")
        col = f"{self.params.ColumnName}"
        val = self.params.Value

        condition = f"{col} IS NOT NULL AND {col} < {val}"
        # Apply conditional logic if present
        condition = self._apply_condition(condition)

        return f"""
        WITH invalid AS (
            SELECT 1
            FROM {{table_name}}
            WHERE {condition}
        )
        SELECT
            COUNT(*) AS violations,
            CASE WHEN COUNT(*) > 0 THEN '{msg_sql}' END AS error_message
        FROM invalid
        """

    def getCheckType(self) -> str:
        return "check_greater_equal"

    def generatePredicate(self) -> str | None:
        """
        Condition mode: select rows where the requirement applies, i.e., col >= value.
        """
        if getattr(self, "exec_mode", "requirement") != "condition":
            return None

        col = self.params.ColumnName
        v = self.params.Value
        _lit = getattr(self, "_lit", None)
        if _lit is None:

            def _lit(x):
                if x is None:
                    return "NULL"
                if isinstance(x, (int, float)) and not isinstance(x, bool):
                    return str(x)
                return "'" + str(x).replace("'", "''") + "'"

        val_sql = _lit(v)

        # rows satisfying the condition (non-null and >= value)
        return f"{col} IS NOT NULL AND {col} >= {val_sql}"


class CheckDistinctCountGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnAName", "ColumnBName", "ExpectedCount"}

    def generateSql(self) -> str:
        a = f"{self.params.ColumnAName}"
        b = f"{self.params.ColumnBName}"
        n = self.params.ExpectedCount

        message = (
            self.errorMessage
            or f"For each {a}, there MUST be exactly {n} distinct {b} values."
        )
        msg_sql = message.replace("'", "''")

        return f"""
        WITH counts AS (
            SELECT {a} AS grp, COUNT(DISTINCT {b}) AS distinct_count
            FROM {{table_name}}
            GROUP BY {a}
        ),
        invalid AS (
            SELECT grp, distinct_count
            FROM counts
            WHERE distinct_count <> {n}
        )
        SELECT
            COUNT(*) AS violations,
            CASE WHEN COUNT(*) > 0 THEN '{msg_sql}' END AS error_message
        FROM invalid
        """

    def getCheckType(self) -> str:
        return "distinct_count"


class CheckModelRuleGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ModelRuleId"}

    def getCheckType(self) -> str:
        return "model_rule_reference"

    def generateSql(self) -> str:
        # Won’t be executed; we’ll attach a special executor instead.
        self.errorMessage = f"Conformance reference to {self.params.ModelRuleId}"
        return "SELECT 0 AS violations"

    def generateCheck(self):
        # Let the base create the DuckDBColumnCheck (with errorMessage, type, sql)
        chk = super().generateCheck()

        target_id = self.params.ModelRuleId
        plan = self.plan
        # Make a dict {rule_id -> result} out of parent_results_by_idx + plan
        id2res: dict[str, dict] = {}
        if plan:
            for pidx, res in (self.parent_results_by_idx or {}).items():
                rid = getattr(plan.nodes[pidx], "rule_id", None)
                if rid:
                    # normalize shape: we expect {"ok": bool, "details": {...}}
                    id2res[rid] = res

        def _exec_reference(_conn):
            # Try to find the referenced rule’s result among parents
            res = id2res.get(target_id)
            if res is None:
                # Not a direct parent? Try to find in global results registry
                converter = None
                if (
                    hasattr(self, "child_builder")
                    and callable(self.child_builder)
                    and hasattr(self.child_builder, "__closure__")
                    and self.child_builder.__closure__
                ):
                    # Access the converter instance from the child_builder lambda's closure
                    for cell in self.child_builder.__closure__:
                        if hasattr(cell.cell_contents, "_global_results_by_idx"):
                            converter = cell.cell_contents
                            break

                if converter and hasattr(converter, "_global_results_by_idx") and plan:
                    # Look for the target_id in global results by scanning plan nodes
                    for node_idx, result in converter._global_results_by_idx.items():
                        if (
                            node_idx < len(plan.nodes)
                            and plan.nodes[node_idx].rule_id == target_id
                        ):
                            res = result
                            break

                if res is None:
                    # Still not found? Fall back to a clear failure.
                    details = {
                        "violations": 1,
                        "message": f"Referenced rule '{target_id}' not found upstream",
                        "referenced_rule_id": target_id,
                    }
                    return False, details

            ok = bool(res.get("ok", False))
            det = dict(res.get("details") or {})
            violations = det.get("violations", 0 if ok else 1)

            details = {
                "violations": int(violations),
                "message": f"Conformance reference to {target_id} ({'OK' if ok else 'FAIL'})",
                "referenced_rule_id": target_id,
            }
            return ok, details

        # Attach the callable to the check so run_check can use it
        chk.special_executor = _exec_reference
        chk.exec_mode = "reference"
        return chk


class CompositeBaseRuleGenerator(DuckDBCheckGenerator):
    """
    Base for AND/OR composites.
    REQUIRED_KEYS = {"Items"}
    Expects the converter to pass:
      - self.child_builder(requirement_dict, breadcrumb:str) -> DuckDBColumnCheck
      - self.composite_factory not needed; we’ll construct DuckDBColumnCheck directly
      - self.breadcrumb (string path for good error messages)
    """

    REQUIRED_KEYS = {"Items"}
    COMPOSITE_NAME = "COMPOSITE"
    HANDLER = staticmethod(all)  # override in subclasses

    def generateSql(self) -> str:
        if not callable(self.child_builder):
            raise RuntimeError(f"{self.__class__.__name__} requires child_builder")

        items = self.p.get("Items")
        if not isinstance(items, list) or not items:
            raise InvalidRuleException(
                f"{self.rule_id} @ {self.breadcrumb}: {self.COMPOSITE_NAME} needs non-empty 'Items'"
            )

        children = []
        for i, child_req in enumerate(items):
            if not isinstance(child_req, dict) or "CheckFunction" not in child_req:
                raise InvalidRuleException(
                    f"{self.rule_id} @ {self.breadcrumb}: Item[{i}] must be a requirement dict with 'CheckFunction'"
                )
            child_bc = f"{self.breadcrumb} > {self.COMPOSITE_NAME}[{i}]"
            # IMPORTANT: pass the REQUIREMENT DICT here
            child_check = self.child_builder(child_req, child_bc)
            children.append(child_check)

        # --- identify upstream failed deps (excluding Items) ------------------------
        # 1) collect failed parent rule_ids from immediate parents
        failed_parent_rule_ids = set()
        if self.plan:
            for pidx, pres in self.parent_results_by_idx.items():
                if not pres.get("ok", True):
                    failed_parent_rule_ids.add(self.plan.nodes[pidx].rule_id)

            # ENHANCED: Also check ALL previously executed rules for failures
            # This ensures that indirect dependencies (like column presence checks)
            # properly propagate failures to dependent composite rules
            converter = None
            if (
                callable(self.child_builder)
                and hasattr(self.child_builder, "__closure__")
                and self.child_builder.__closure__
            ):
                # Access the converter instance from the child_builder lambda's closure
                for cell in self.child_builder.__closure__:
                    if hasattr(cell.cell_contents, "_global_results_by_idx"):
                        converter = cell.cell_contents
                        break

            if converter and hasattr(converter, "_global_results_by_idx"):
                for node_idx, result in converter._global_results_by_idx.items():
                    if not result.get("ok", True):
                        if node_idx < len(self.plan.nodes):
                            failed_parent_rule_ids.add(
                                self.plan.nodes[node_idx].rule_id
                            )

        # 2) dependencies declared on this rule
        deps = []
        vc = getattr(self.rule, "validation_criteria", None)
        if vc and hasattr(vc, "dependencies"):
            deps = list(vc.dependencies or [])
        elif isinstance(vc, dict):
            deps = list(vc.get("dependencies") or [])

        # 3) rule_ids that appear inside Items (if Items are references with RuleId)
        item_rule_ids = set()
        model_rule_refs = set()  # Track CheckModelRule references
        for item in items:
            if isinstance(item, dict):
                rid = item.get("RuleId") or item.get("rule_id")
                if rid:
                    item_rule_ids.add(rid)
                # Check for CheckModelRule references
                if item.get("CheckFunction") == "CheckModelRule":
                    conf_rule_id = item.get("ModelRuleId")
                    if conf_rule_id:
                        model_rule_refs.add(conf_rule_id)

        # 4) Check if any CheckModelRule references have failed
        failed_conformance_refs = []
        if model_rule_refs and self.plan:
            converter = None
            if (
                callable(self.child_builder)
                and hasattr(self.child_builder, "__closure__")
                and self.child_builder.__closure__
            ):
                for cell in self.child_builder.__closure__:
                    if hasattr(cell.cell_contents, "_global_results_by_idx"):
                        converter = cell.cell_contents
                        break

            if converter and hasattr(converter, "_global_results_by_idx"):
                for node_idx, result in converter._global_results_by_idx.items():
                    if node_idx < len(self.plan.nodes):
                        rule_id = self.plan.nodes[node_idx].rule_id
                        if rule_id in model_rule_refs:
                            is_ok = result.get("ok", True)
                            if not is_ok:
                                # Check if the failed conformance rule is a Dataset entity type
                                failed_rule_entity_type = getattr(
                                    self.plan.nodes[node_idx].rule, "entity_type", None
                                )
                                # Only cascade the failure if it's not a Dataset entity type
                                if failed_rule_entity_type != "Dataset":
                                    failed_conformance_refs.append(rule_id)

        # 5) Check for failed base rules operating on the same column (semantic dependencies)
        # Only apply semantic dependency propagation to Attribute and Column entity types, not Dataset
        failed_same_column_rules = []
        rule_entity_type = (
            getattr(self.rule, "entity_type", None) if hasattr(self, "rule") else None
        )

        if self.plan and rule_entity_type in ["Attribute", "Column"]:
            # Extract the column name from this rule's rule_id (e.g., "CapacityReservationId" from "CapacityReservationId-C-007-C")
            current_rule_column = (
                self.rule_id.split("-")[0] if "-" in self.rule_id else None
            )

            if (
                current_rule_column
                and converter
                and hasattr(converter, "_global_results_by_idx")
            ):
                for node_idx, result in converter._global_results_by_idx.items():
                    if not result.get("ok", True) and node_idx < len(self.plan.nodes):
                        failed_rule_id = self.plan.nodes[node_idx].rule_id
                        # Check if this failed rule operates on the same column
                        # Extract column name from failed rule - handle both direct rules and CostAndUsage-D-* presence checks
                        if (
                            failed_rule_id.startswith("CostAndUsage-D-")
                            and current_rule_column
                        ):
                            # For column presence checks, check if the failure message mentions this column
                            failure_message = str(
                                result.get("details", {}).get("message", "")
                            )
                            failed_rule_column = (
                                current_rule_column
                                if f"Column '{current_rule_column}'" in failure_message
                                else None
                            )
                        else:
                            failed_rule_column = (
                                failed_rule_id.split("-")[0]
                                if "-" in failed_rule_id
                                else None
                            )

                        if (
                            failed_rule_column == current_rule_column
                            and failed_rule_id != self.rule_id
                            and ("-C-000-" in failed_rule_id or "-D-" in failed_rule_id)
                        ):  # Column presence or base validation
                            failed_same_column_rules.append(failed_rule_id)

        # 6) external deps = declared deps minus item rule_ids
        external_deps = set(deps) - item_rule_ids

        # 7) which external deps actually failed upstream?
        external_failed_candidates = sorted(external_deps & failed_parent_rule_ids)

        # Filter out Dataset entity type failures - they should not cascade to child rules
        external_failed = []
        for failed_rule_id in external_failed_candidates:
            # Find the failed rule in the plan to check its entity type
            failed_rule_entity_type = None
            if self.plan:
                for node in self.plan.nodes:
                    if node.rule_id == failed_rule_id:
                        failed_rule_entity_type = getattr(
                            node.rule, "entity_type", None
                        )
                        break

            # Only include the failure if the parent rule is NOT a Dataset entity type
            if failed_rule_entity_type != "Dataset":
                external_failed.append(failed_rule_id)

        # 8) Add failed conformance rule references and same-column failures to external failures
        # Only apply dependency propagation to Attribute and Column entity types, not Dataset
        if rule_entity_type in ["Attribute", "Column"]:
            all_failed = sorted(
                set(external_failed)
                | set(failed_conformance_refs)
                | set(failed_same_column_rules)
            )
        else:
            # For Dataset entity types, only use explicit dependencies, not semantic cascading
            all_failed = sorted(set(external_failed) | set(failed_conformance_refs))

        if all_failed:
            # Tag the composite check to force a short-circuit fail in run_check
            self.force_fail_due_to_upstream = {
                "failed_dependencies": all_failed,
                "reason": "upstream dependency failure",
            }
            # Give a clear message now (executor will reuse it)
            failure_reason = (
                f"external dependencies: {external_failed}" if external_failed else ""
            )
            conformance_reason = (
                f"conformance rules: {failed_conformance_refs}"
                if failed_conformance_refs
                else ""
            )
            same_column_reason = (
                f"same-column rules: {failed_same_column_rules}"
                if failed_same_column_rules
                else ""
            )
            combined_reason = " and ".join(
                filter(None, [failure_reason, conformance_reason, same_column_reason])
            )

            self.errorMessage = (
                self.p.get("Message")
                or f"{self.rule_id}: upstream dependency failure ({combined_reason})"
            )

        self.nestedChecks = children
        self.nestedCheckHandler = (
            self.HANDLER.__func__ if hasattr(self.HANDLER, "__func__") else self.HANDLER
        )
        self.errorMessage = (
            self.p.get("Message") or f"{self.rule_id}: {self.COMPOSITE_NAME} failed"
        )
        return "SELECT 0 AS violations"

    def getCheckType(self) -> str:
        return "composite"


class CompositeANDRuleGenerator(CompositeBaseRuleGenerator):
    COMPOSITE_NAME = "AND"
    HANDLER = staticmethod(all)

    def generatePredicate(self) -> str | None:
        # Only meaningful in condition mode
        if getattr(self, "exec_mode", "requirement") != "condition":
            return None

        if not callable(self.compile_condition):
            # No compiler available; safest is to “no filter”
            return "TRUE"

        items = self.p.get("Items") if hasattr(self, "p") else self.params.Items
        items = items or []

        preds = []
        for i, spec in enumerate(items):
            pred = self.compile_condition(
                spec,
                rule=self.rule,
                rule_id=self.rule_id,
                breadcrumb=f"{self.breadcrumb}>AND[{i}]",
            )
            if pred:
                preds.append(f"({pred})")

        # AND of nothing → TRUE (no additional filtering)
        return " AND ".join(preds) if preds else "TRUE"


class CompositeORRuleGenerator(CompositeBaseRuleGenerator):
    COMPOSITE_NAME = "OR"
    HANDLER = staticmethod(any)

    def generatePredicate(self) -> str | None:
        if getattr(self, "exec_mode", "requirement") != "condition":
            return None

        if not callable(self.compile_condition):
            return "FALSE"  # OR without a compiler → safest to match nothing

        items = self.p.get("Items") if hasattr(self, "p") else self.params.Items
        items = items or []

        preds = []
        for i, spec in enumerate(items):
            pred = self.compile_condition(
                spec,
                rule=self.rule,
                rule_id=self.rule_id,
                breadcrumb=f"{self.breadcrumb}>OR[{i}]",
            )
            if pred:
                preds.append(f"({pred})")

        # OR of nothing → FALSE (no rows match)
        return " OR ".join(preds) if preds else "FALSE"


class FocusToDuckDBSchemaConverter:
    # Central registry for all check types with both generators and check object factories
    CHECK_GENERATORS: dict[str, Dict[str, Any]] = {
        "ColumnPresent": {
            "generator": ColumnPresentCheckGenerator,
            "factory": lambda args: "ColumnName",
        },
        "TypeString": {
            "generator": TypeStringCheckGenerator,
            "factory": lambda args: "ColumnName",
        },
        "TypeDecimal": {
            "generator": TypeDecimalCheckGenerator,
            "factory": lambda args: "ColumnName",
        },
        "TypeDateTime": {
            "generator": TypeDateTimeGenerator,
            "factory": lambda args: "ColumnName",
        },
        "FormatNumeric": {
            "generator": FormatNumericGenerator,
            "factory": lambda args: "ColumnName",
        },
        "FormatString": {
            "generator": FormatStringGenerator,
            "factory": lambda args: "ColumnName",
        },
        "FormatDateTime": {
            "generator": FormatDateTimeGenerator,
            "factory": lambda args: "ColumnName",
        },
        "FormatBillingCurrencyCode": {
            "generator": FormatBillingCurrencyCodeGenerator,
            "factory": lambda args: "ColumnName",
        },
        "FormatKeyValue": {
            "generator": FormatKeyValueGenerator,
            "factory": lambda args: "ColumnName",
        },
        "FormatCurrency": {
            "generator": FormatCurrencyGenerator,
            "factory": lambda args: "ColumnName",
        },
        "CheckNationalCurrency": {
            "generator": FormatCurrencyGenerator,
            "factory": lambda args: "ColumnName",
        },
        "FormatUnit": {
            "generator": FormatUnitGenerator,
            "factory": lambda args: "ColumnName",
        },
        "CheckValue": {
            "generator": CheckValueGenerator,
            "factory": lambda args: "ColumnName",
        },
        "CheckNotValue": {
            "generator": CheckNotValueGenerator,
            "factory": lambda args: "ColumnName",
        },
        "CheckSameValue": {
            "generator": CheckSameValueGenerator,
            "factory": lambda args: "ColumnAName",
        },
        "CheckNotSameValue": {
            "generator": CheckNotSameValueGenerator,
            "factory": lambda args: "ColumnAName",
        },
        "CheckDecimalValue": {
            "generator": CheckDecimalValueGenerator,
            "factory": lambda args: "ColumnName",
        },
        "CheckGreaterOrEqualThanValue": {
            "generator": CheckGreaterOrEqualGenerator,
            "factory": lambda args: "ColumnName",
        },
        "CheckDistinctCount": {
            "generator": CheckDistinctCountGenerator,
            "factory": lambda args: "ColumnAName",
        },
        "CheckModelRule": {
            "generator": CheckModelRuleGenerator,
            "factory": lambda args: "ModelRuleId",
        },
        "AND": {
            "generator": CompositeANDRuleGenerator,
            "factory": lambda args: "Items",
        },
        "OR": {"generator": CompositeORRuleGenerator, "factory": lambda args: "Items"},
        "ColumnByColumnEqualsColumnValue": {
            "generator": ColumnByColumnEqualsColumnValueGenerator,
            "factory": lambda args: "ColumnAName",
        },
    }

    def __init__(
        self,
        *,
        focus_data: Any,
        focus_table_name: str = "focus_data",
        pragma_threads: int | None = None,
        explain_mode: bool = False,
        validated_applicability_criteria: Optional[List[str]] = None,
    ) -> None:
        self.log = logging.getLogger(f"{__name__}.{self.__class__.__qualname__}")
        self.conn: duckdb.DuckDBPyConnection | None = None
        self.plan: ValidationPlan | None = None
        self.pragma_threads = pragma_threads
        self.focus_data = focus_data
        self.table_name = focus_table_name
        self.validated_applicability_criteria = validated_applicability_criteria or []
        # Example caches (optional)
        self._prepared: Dict[str, Any] = {}
        self._views: Dict[str, str] = {}  # rule_id -> temp view name
        self.explain_mode = explain_mode
        # Global results registry for dependency failure propagation
        self._global_results_by_idx: Dict[int, Dict[str, Any]] = {}

    def _should_include_rule(
        self, rule: Any, parent_edges: Optional[Tuple[Any, ...]] = None
    ) -> bool:
        """Check if a rule should be included based on applicability criteria.

        Performs hierarchical check:
        1. Check this rule's applicability criteria
        2. Check all parent dependencies up to the root
        """
        # First check this rule's own applicability criteria
        if not self._check_rule_applicability(rule):
            return False

        # Then check all parent dependencies recursively
        if parent_edges:
            for parent_rule in self._parent_rules_from_edges(parent_edges):
                if parent_rule and not self._check_rule_applicability(parent_rule):
                    self.log.debug(
                        "Excluding rule %s due to parent %s applicability criteria mismatch",
                        getattr(rule, "rule_id", "<unknown>"),
                        getattr(parent_rule, "rule_id", "<unknown>"),
                    )
                    return False
                # Recursively check parent's parents if available
                parent_rule_id = getattr(parent_rule, "rule_id", None)
                if parent_rule_id is None:
                    continue
                parent_node = self._node_by_rule_id(parent_rule_id)
                if parent_node and hasattr(parent_node, "parent_edges"):
                    if not self._should_include_rule(
                        parent_rule, parent_node.parent_edges
                    ):
                        return False

        return True

    def _check_rule_applicability(self, rule: Any) -> bool:
        """Check a single rule's applicability criteria without checking parents."""
        # Check if rule has applicability criteria
        rule_criteria = (
            rule.applicability_criteria
            if hasattr(rule, "applicability_criteria") and rule.applicability_criteria
            else []
        )

        # If rule has no applicability criteria, always include it (backwards compatibility)
        if not rule_criteria:
            return True

        # If no criteria were provided by user, skip rules that have applicability criteria
        if not self.validated_applicability_criteria:
            return False

        # Include rule if any of its criteria match our validated list
        return any(
            criteria in self.validated_applicability_criteria
            for criteria in rule_criteria
        )

    # -- lifecycle ------------------------------------------------------------
    def prepare(self, *, conn: duckdb.DuckDBPyConnection, plan: ValidationPlan) -> None:
        """Initialize connection, create temp schema/tables/UDFs, register sources."""
        self.conn = conn
        self.plan = plan
        if self.conn is None:
            self.log.debug("Creating in-memory DuckDB connection")
            self.conn = duckdb.connect(":memory:")
        else:
            self.log.debug("Using provided DuckDB connection")

        # Register the focus data with DuckDB regardless of connection source
        self.conn.register(self.table_name, self.focus_data)

        if self.pragma_threads:
            self.conn.execute(f"PRAGMA threads={int(self.pragma_threads)}")

        # TODO:
        # - register pandas/arrow tables or file paths
        # - create temp schema or set search_path if needed
        # - compile reusable SQL fragments; register any UDFs (from CheckFunctions)

    def finalize(
        self, *, success: bool, results_by_idx: Dict[int, Dict[str, Any]]
    ) -> None:
        """Optional cleanup: drop temps, emit summaries, etc."""
        # e.g., self.conn.execute("DROP VIEW IF EXISTS ...")
        pass

    # -- check build/execute --------------------------------------------------
    def build_check(
        self,
        *,
        rule: Any,
        parent_results_by_idx: Dict[int, Dict[str, Any]],
        parent_edges: Tuple[EdgeCtx, ...],
        rule_id: str,
        node_idx: int,
    ) -> Any:
        """
        Build a runnable DuckDBColumnCheck (leaf or composite) for a rule.
        Parent results/edges are available if your generators ever need them;
        for now, we keep the API symmetric and future-proof.
        """
        if self.conn is None or self.plan is None:
            raise RuntimeError(
                "Converter not prepared. Call prepare(conn=..., plan=...) first."
            )

        # If your generators need access to parents, you can stash them on self for this node
        # or extend DuckDBCheckGenerator to accept them. Keeping it simple for now.
        if rule.is_dynamic():
            return SkippedDynamicCheck(rule=rule, rule_id=rule_id)

        # Check if rule should be skipped due to applicability criteria (including parent chain)
        if not self._should_include_rule(rule, parent_edges):
            return SkippedNonApplicableCheck(rule=rule, rule_id=rule_id)

        requirement = self.__requirement_for_rule__(rule)
        check_obj = self.__generate_duckdb_check__(
            rule,
            rule_id,
            requirement,
            breadcrumb=rule_id,
            parent_results_by_idx=parent_results_by_idx,
            parent_edges=parent_edges,
        )
        return check_obj

    def run_check(self, check: Any) -> Tuple[bool, Dict[str, Any]]:
        """
        Execute a DuckDBColumnCheck (leaf or composite) or a SkippedCheck.
        Ensures details always include: violations:int, message:str.
        """

        def _msg_for_outcome(
            obj, ok: bool, fallback_fail: str, fallback_ok: str | None = None
        ) -> str | None:
            if ok:
                # Prefer an explicit successMessage if a generator sets it; else no message or a tiny 'OK'
                sm = getattr(obj, "successMessage", None)
                return (
                    sm if isinstance(sm, str) and sm.strip() else (fallback_ok or None)
                )
            else:
                em = getattr(obj, "errorMessage", None)
                return em if isinstance(em, str) and em.strip() else fallback_fail

        if self.conn is None:
            raise RuntimeError("Converter not prepared. No DuckDB connection.")

        # ---- helpers ------------------------------------------------------------
        def _msg_for(obj, fallback: str) -> str:
            msg = getattr(obj, "errorMessage", None)
            return msg if (isinstance(msg, str) and msg.strip()) else fallback

        def _sub_table(sql: str) -> str:
            # support both {table_name} and {{table_name}} templates
            if not hasattr(self, "table_name") or not self.table_name:
                raise RuntimeError(
                    "FocusToDuckDBSchemaConverter.table_name is not set."
                )
            return sql.replace("{{table_name}}", self.table_name).replace(
                "{table_name}", self.table_name
            )

        def _extract_missing_columns(err_msg: str) -> list[str]:
            pats = [
                r'Column with name ([A-Za-z0-9_"]+) does not exist',
                r'Binder Error: .*? column ([A-Za-z0-9_"]+)',
                r'"([A-Za-z0-9_]+)" not found',
            ]
            found = set()
            for pat in pats:
                for m in re.finditer(pat, err_msg):
                    col = m.group(1).strip('"')
                    if col:
                        found.add(col)
            return sorted(found)

        # ---- skipped (dynamic) --------------------------------------------------
        if (
            isinstance(check, SkippedCheck)
            or getattr(check, "checkType", "") == "skipped_check"
        ):
            ok, details = check.run(self.conn)
            details.setdefault("violations", 0)
            details.setdefault(
                "message",
                _msg_for(check, f"{getattr(check, 'rule_id', '<rule>')}: skipped"),
            )
            return ok, details

        # ---- composite (AND/OR) -------------------------------------------------
        nested = getattr(check, "nestedChecks", None) or []
        handler = getattr(check, "nestedCheckHandler", None)
        if nested and handler:
            # Upstream dependency short-circuit (tag set by composite generator)
            upstream = getattr(check, "force_fail_due_to_upstream", None)
            if upstream:
                reason = upstream.get("reason", "upstream dependency failure")
                failed_deps = upstream.get("failed_dependencies", [])
                upstream_child_details: List[Dict[str, Any]] = []
                for child in nested:
                    upstream_child_details.append(
                        {
                            "rule_id": getattr(child, "rule_id", None),
                            "ok": False,
                            "violations": 1,
                            "message": f"{getattr(child, 'rule_id', '<child>')}: {reason}",
                            "reason": reason,
                        }
                    )
                details = {
                    "children": upstream_child_details,
                    "aggregated": handler.__name__,
                    "message": _msg_for(
                        check, f"{getattr(check, 'rule_id', '<rule>')}: {reason}"
                    ),
                    "reason": reason,
                    "failed_dependencies": failed_deps,
                    "violations": 1,
                    "check_type": getattr(check, "checkType", None)
                    or getattr(check, "check_type", None),
                }
                return False, details

            # Normal composite: run children and aggregate
            oks: List[bool] = []
            normal_child_details: List[Dict[str, Any]] = []
            for child in nested:
                ok_i, det_i = self.run_check(child)
                oks.append(ok_i)
                det_i.setdefault("violations", 0 if ok_i else 1)
                det_i.setdefault(
                    "message",
                    _msg_for(
                        child, f"{getattr(child, 'rule_id', '<child>')}: check failed"
                    ),
                )
                normal_child_details.append(
                    {"rule_id": getattr(child, "rule_id", None), **det_i}
                )

            agg_ok = bool(handler(oks))
            normal_details = {
                "children": normal_child_details,
                "aggregated": handler.__name__,
                "message": _msg_for_outcome(
                    check,
                    agg_ok,
                    fallback_fail=f"{getattr(check, 'rule_id', '<rule>')}: composite failed",
                    fallback_ok=None,  # or f"{getattr(check,'rule_id','<rule>')}: OK"
                ),
                "violations": 0 if agg_ok else 1,
                "check_type": getattr(check, "checkType", None)
                or getattr(check, "check_type", None),
            }
            return agg_ok, normal_details

        # ---- leaf ---------------------------------------------------------------
        # Special executor path (e.g., conformance rule reference)
        special = getattr(check, "special_executor", None)
        if callable(special):
            ok, details = special(self.conn)
            details.setdefault("violations", 0 if ok else 1)
            details.setdefault(
                "message",
                getattr(check, "errorMessage", None)
                or f"{getattr(check, 'rule_id', '<rule>')}: reference evaluation",
            )
            details.setdefault(
                "check_type",
                getattr(check, "checkType", None) or getattr(check, "check_type", None),
            )
            return ok, details
        sql = getattr(check, "checkSql", None)
        if not sql:
            raise InvalidRuleException(
                f"Leaf check has no SQL to execute (rule_id={getattr(check, 'rule_id', None)})"
            )
        sql_final = _sub_table(sql)

        t0 = time.perf_counter()
        try:
            df = self.conn.execute(sql_final).fetchdf()
        except (
            duckdb.CatalogException,
            duckdb.BinderException,
            duckdb.ParserException,
        ) as e:
            # Convert schema/binder errors into a clean failure
            msg = str(e)
            missing = _extract_missing_columns(msg)
            reason = (
                f"Missing columns: {', '.join(missing)}"
                if missing
                else "Missing required column(s)"
            )
            details = {
                "violations": 1,
                "message": _msg_for(
                    check, f"{getattr(check, 'rule_id', '<rule>')}: {reason}"
                ),
                "error": msg,
                "missing_columns": missing or None,
                "timing_ms": (time.perf_counter() - t0) * 1000.0,
                "check_type": getattr(check, "checkType", None)
                or getattr(check, "check_type", None),
            }
            return False, details

        elapsed_ms = (time.perf_counter() - t0) * 1000.0

        if df.empty:
            raise RuntimeError(
                f"Validation query returned no rows for {getattr(check, 'rule_id', '<rule>')}.\nSQL:\n{sql_final}"
            )

        # Prefer explicit 'violations' column; fall back to first cell if needed.
        if "violations" in df.columns:
            raw = df.at[0, "violations"]
        else:
            raw = df.iloc[0, 0]

        if raw is None:
            raise RuntimeError(
                f"'violations' returned NULL for {getattr(check, 'rule_id', '<rule>')}.\nSQL:\n{sql_final}"
            )

        try:
            # Handle numpy/pandas types by converting to Python native type first
            if hasattr(raw, 'item'):
                # numpy scalar types have .item() method to get Python native type
                raw_native = raw.item()
            else:
                raw_native = raw
            violations = int(raw_native)
        except Exception:
            raise RuntimeError(
                f"'violations' is not an integer for {getattr(check, 'rule_id', '<rule>')} (got {type(raw).__name__}: {raw!r}).\nSQL:\n{sql_final}"
            )

        ok = violations == 0
        leaf_details: Dict[str, Any] = {
            "violations": violations,
            "message": _msg_for_outcome(
                check,
                ok,
                fallback_fail=f"{getattr(check, 'rule_id', '<rule>')}: check failed",
                fallback_ok=None,  # or f"{getattr(check,'rule_id','<rule>')}: OK"
            ),
            "timing_ms": elapsed_ms,
            "check_type": getattr(check, "checkType", None)
            or getattr(check, "check_type", None),
        }

        # Optional: sample rows if provided by the generator and the check failed
        sample_sql = getattr(check, "sample_sql", None)
        sample_limit = getattr(check, "sample_limit", 50)
        if (not ok) and sample_sql:
            try:
                sql_sample = _sub_table(sample_sql) + f" LIMIT {int(sample_limit)}"
                leaf_details["failure_cases"] = self.conn.execute(sql_sample).fetchdf()
            except Exception as e:
                leaf_details["sample_error"] = str(e)

        return ok, leaf_details

    def __requirement_for_rule__(self, rule: Any) -> dict:
        """
        Return the normalized Requirement dict for this rule.
        Expects rule.validation_criteria.requirement to be a dict that contains either:
        - {"CheckFunction": "<LeafType>", ...params...}
        - {"CheckFunction": "AND"|"OR", "Items": [ ... child requirements ... ]}
        """
        vc = getattr(rule, "validation_criteria", None)
        if not vc:
            raise InvalidRuleException(
                f"{getattr(rule, 'rule_id', '<unknown>')} has no validation_criteria"
            )
        req = getattr(vc, "requirement", None)
        if not isinstance(req, dict):
            raise InvalidRuleException(
                f"{getattr(rule, 'rule_id', '<unknown>')} requirement must be a dict"
            )
        return req

    def __make_generator__(
        self,
        rule: Any,
        rule_id: str,
        requirement: dict,
        breadcrumb: str,
        parent_results_by_idx: Optional[dict] = None,
        parent_edges: Optional[dict] = None,
        row_condition_sql=None,
    ) -> DuckDBCheckGenerator:
        if not isinstance(requirement, dict):
            raise InvalidRuleException(
                f"{rule_id} @ {breadcrumb}: expected requirement dict, got {type(requirement).__name__}"
            )
        check_fn = requirement.get("CheckFunction")
        if not check_fn or not isinstance(check_fn, str):
            raise InvalidRuleException(
                textwrap.dedent(
                    f"""
                Rule {rule_id} @ {breadcrumb}: Requirement missing 'CheckFunction'.
                Requirement:
                {_compact_json(requirement)}
                """
                ).strip()
            )

        reg = self.CHECK_GENERATORS.get(check_fn)
        if not reg or "generator" not in reg:
            raise InvalidRuleException(
                textwrap.dedent(
                    f"""
                Rule {rule_id} @ {breadcrumb}: No generator registered for CheckFunction='{check_fn}'.
                Available generators: {sorted(self.CHECK_GENERATORS.keys())}
                Requirement:
                {_compact_json(requirement)}
                """
                ).strip()
            )

        gen_cls = reg["generator"]

        # Strip reserved + 'CheckFunction' and pass as-is (no aliasing)
        reserved = getattr(DuckDBCheckGenerator, "RESERVED", set()) or set()
        params = {
            k: v
            for k, v in requirement.items()
            if k not in reserved and k != "CheckFunction"
        }

        # Let the generator’s REQUIRED_KEYS drive validation
        required = set(getattr(gen_cls, "REQUIRED_KEYS", set()) or set())
        missing = [rk for rk in sorted(required) if rk not in params]

        if missing:
            # Optional: capture parent context summary for composite trees (best-effort)
            parent_summary = ""
            try:
                parents = getattr(
                    rule, "_plan_parents_", None
                )  # if you want, attach this earlier
                if parents:
                    parent_status = ", ".join(
                        f"{pid}={'FAIL' if not pres.get('ok', True) else 'OK'}"
                        for pid, pres in parents.items()
                    )
                    parent_summary = f"\nParent status: {parent_status}"
            except Exception:
                pass

            message = (
                textwrap.dedent(
                    f"""
            Rule {rule_id} @ {breadcrumb}: Missing required parameter(s) for '{check_fn}'.
            Required: {sorted(required)}
            Provided: {sorted(params.keys())}
            Requirement (snippet):
            {_compact_json(requirement)}
            """
                ).rstrip()
                + parent_summary
            )

            # Log full requirement once (helps when stdout truncates exceptions)
            log.error("Generator args missing: %s", message)
            raise InvalidRuleException(message)

        # Instantiate with *exactly* what was provided (plus defaults if your gen applies them)
        return gen_cls(
            rule=rule,
            rule_id=rule_id,
            plan=self.plan,
            conn=self.conn,
            parent_results_by_idx=parent_results_by_idx or {},
            parent_edges=parent_edges or (),
            row_condition_sql=row_condition_sql,
            compile_condition=self._compile_condition_with_generators,
            child_builder=lambda child_req, child_bc: self.__generate_duckdb_check__(
                rule,
                rule_id,
                child_req,
                breadcrumb=child_bc,
                parent_results_by_idx=parent_results_by_idx or {},
                parent_edges=parent_edges or (),
            ),
            breadcrumb=breadcrumb,
            **params,
        )

    def __generate_duckdb_check__(
        self,
        rule: Any,
        rule_id: str,
        requirement: dict,
        breadcrumb: str,
        parent_results_by_idx,
        parent_edges,
    ) -> Union["DuckDBColumnCheck", SkippedCheck]:
        """
        Build a DuckDBColumnCheck for this requirement.
        For composites (AND/OR), the Composite* generators will recursively call back here
        to build child checks and set `nestedChecks` + `nestedCheckHandler`.
        """
        if not isinstance(requirement, dict):
            raise InvalidRuleException(
                f"{rule_id} @ {breadcrumb}: expected requirement dict, got {type(requirement).__name__}"
            )
        eff_cond = self._build_effective_condition(rule, parent_edges)
        gen = self.__make_generator__(
            rule,
            rule_id,
            requirement,
            breadcrumb=breadcrumb,
            parent_results_by_idx=parent_results_by_idx,
            parent_edges=parent_edges,
            row_condition_sql=eff_cond,
        )
        # NOTE: Composite generators in your file already call __generate_duckdb_check__ for children
        # and set self.nestedChecks + self.nestedCheckHandler before returning.
        if isinstance(gen, SkippedCheck):
            return gen
        return gen.generateCheck()

    # --- condition helpers -------------------------------------------------------
    def _extract_condition_sql_from_rule(self, rule) -> str | None:
        """
        Return a SQL predicate string for this rule's Condition, or None if not present.
        Supports both Pydantic models and dict-shaped rules.
        We treat these keys as likely carriers of a raw SQL predicate:
        - ValidationCriteria.ConditionSql
        - ValidationCriteria.Condition.SQL / Sql / Expression / Where / Predicate
        - ValidationCriteria.Condition (if it's already a string)
        """
        vc = getattr(rule, "validation_criteria", None)
        if vc is None and isinstance(rule, dict):
            vc = rule.get("ValidationCriteria")

        cond = None
        if vc is None:
            return None

        # direct fields
        for key in ("ConditionSql", "ConditionSQL", "Condition_Sql"):
            cond = getattr(vc, key, None) if not isinstance(vc, dict) else vc.get(key)
            if isinstance(cond, str) and cond.strip():
                return cond.strip()

        # nested "Condition"
        c = (
            getattr(vc, "Condition", None)
            if not isinstance(vc, dict)
            else vc.get("Condition")
        )
        if isinstance(c, str) and c.strip():
            return c.strip()
        if isinstance(c, dict):
            # try common keys for raw SQL
            for key in ("SQL", "Sql", "Expression", "Expr", "Where", "Predicate"):
                v = c.get(key)
                if isinstance(v, str) and v.strip():
                    return v.strip()
            # If your Condition is a structured object, compile here (out of scope for now)

        return None

    def _node_by_rule_id(self, rid: str):
        # Try fast path if your plan exposes a mapping; otherwise fallback scan.
        plan = getattr(self, "plan", None)
        if not plan:
            return None
        # common names people use:
        for attr in ("nodes_by_rule_id", "by_rule_id", "id2node"):
            ndict = getattr(plan, attr, None)
            if isinstance(ndict, dict):
                return ndict.get(rid)
        # fallback: linear scan
        nodes = getattr(plan, "nodes", None)
        if isinstance(nodes, (list, tuple)):
            for n in nodes:
                if getattr(n, "rule_id", None) == rid:
                    return n
        return None

    def _parent_rules_from_edges(self, parent_edges):
        """
        Yield parent rule objects from a variety of parent_edges shapes:
        - iterable[EdgeCtx] (with .parent_idx / .src_idx / .parent_rule_id / .src_rule_id)
        - dict[rule_id, EdgeCtx]
        - iterable[int] (node indices)
        - iterable[str] (rule_ids)
        - iterable[PlanNode]
        """
        plan = getattr(self, "plan", None)
        if not plan:
            return

        # Normalize to an iterable of edges/ids/indices/nodes
        if isinstance(parent_edges, dict):
            for e in parent_edges.values():
                # Handle each edge case for dict values
                # 1) direct index
                if isinstance(e, int):
                    try:
                        node = plan.nodes[e]
                        yield getattr(node, "rule", None)
                    except (IndexError, AttributeError):
                        pass
                # 2) EdgeCtx-like object
                elif hasattr(e, "parent_idx") or hasattr(e, "src_idx"):
                    idx = getattr(e, "parent_idx", None) or getattr(e, "src_idx", None)
                    if isinstance(idx, int):
                        try:
                            node = plan.nodes[idx]
                            yield getattr(node, "rule", None)
                        except (IndexError, AttributeError):
                            pass
                # 3) direct rule_id string
                elif isinstance(e, str):
                    node = self._node_by_rule_id(e)
                    if node:
                        yield getattr(node, "rule", None)
                # 4) PlanNode directly
                elif hasattr(e, "rule"):
                    yield getattr(e, "rule", None)
            return

        # Handle non-dict parent_edges
        edge_items = parent_edges or ()
        for e in edge_items:
            # 1) direct index
            if isinstance(e, int):
                try:
                    node = plan.nodes[e]
                    yield getattr(node, "rule", None)
                except Exception:
                    continue
                continue

            # 2) rule_id string
            if isinstance(e, str):
                node = self._node_by_rule_id(e)
                if node is not None:
                    yield getattr(node, "rule", None)
                continue

            # 3) plan node object
            rid = getattr(e, "rule_id", None)
            rule = getattr(e, "rule", None)
            if rule is not None and rid is not None:
                yield rule
                continue

            # 4) EdgeCtx-like: try to pull an index, then rule_id
            idx = None
            for attr in ("parent_idx", "src_idx", "from_idx", "u", "parent_node_idx"):
                v = getattr(e, attr, None)
                if isinstance(v, int):
                    idx = v
                    break
            if idx is not None:
                try:
                    node = plan.nodes[idx]
                    yield getattr(node, "rule", None)
                except Exception:
                    pass
                continue

            for attr in ("parent_rule_id", "src_rule_id", "rule_id"):
                rid = getattr(e, attr, None)
                if isinstance(rid, str):
                    node = self._node_by_rule_id(rid)
                    if node is not None:
                        yield getattr(node, "rule", None)
                    break

    def _compile_condition_with_generators(
        self, spec: dict | str | None, *, rule, rule_id: str, breadcrumb: str = ""
    ) -> str | None:
        if not spec:
            return None
        if isinstance(spec, str):
            s = spec.strip()
            return s or None
        if not isinstance(spec, dict):
            return None

        fn = spec.get("CheckFunction")
        if not fn:
            return None

        # Composites (reuse your composite names)
        if fn == "AND":
            items = spec.get("Items") or []
            parts = []
            for i, it in enumerate(items):
                pred = self._compile_condition_with_generators(
                    it, rule=rule, rule_id=rule_id, breadcrumb=f"{breadcrumb}>AND[{i}]"
                )
                if pred:
                    parts.append(f"({pred})")
            if not parts:
                return "TRUE"  # AND of nothing = TRUE (no filter)
            return " AND ".join(parts)

        if fn == "OR":
            items = spec.get("Items") or []
            parts = []
            for i, it in enumerate(items):
                pred = self._compile_condition_with_generators(
                    it, rule=rule, rule_id=rule_id, breadcrumb=f"{breadcrumb}>OR[{i}]"
                )
                if pred:
                    parts.append(f"({pred})")
            if not parts:
                return "FALSE"  # OR of nothing = FALSE (no rows)
            return " OR ".join(parts)

        # Leaf: reuse the CHECK_GENERATORS registry
        reg = self.CHECK_GENERATORS.get(fn)
        if not reg:
            # Unknown function name → no filter (or raise if you want strict)
            return None

        gen_cls = reg["generator"]

        # Basic required-key validation (optional)
        required = getattr(gen_cls, "REQUIRED_KEYS", set()) or set()
        missing = [k for k in required if k not in spec]
        if missing:
            # For conditions, you can choose to return None or raise
            # raise ValueError(f"{rule_id} @ {breadcrumb}: Condition {fn} missing keys {missing}")
            return None

        # Build params (exclude CheckFunction)
        params = {k: v for k, v in spec.items() if k != "CheckFunction"}

        # Instantiate with exec_mode="condition"
        gen = gen_cls(
            rule=rule,
            rule_id=rule_id,
            exec_mode="condition",
            breadcrumb=breadcrumb or rule_id,
            **params,
        )

        pred = gen.generatePredicate()
        return pred.strip() if pred else None

    def _extract_condition_spec(self, rule):
        vc = getattr(rule, "validation_criteria", None)
        if vc is None and isinstance(rule, dict):
            vc = rule.get("ValidationCriteria")
        if not vc:
            return None
        cond = (
            getattr(vc, "condition", None)  # Fixed: use lowercase attribute name
            if not isinstance(vc, dict)
            else vc.get("Condition")
        )
        if cond is None:
            for k in ("ConditionSql", "ConditionSQL", "Condition_Sql"):
                v = getattr(vc, k, None) if not isinstance(vc, dict) else vc.get(k)
                if v is not None:
                    return v
        return cond

    def _build_effective_condition(self, rule, parent_edges) -> str | None:
        parts = []

        me = self._extract_condition_spec(rule)
        me_sql = self._compile_condition_with_generators(
            me,
            rule=rule,
            rule_id=getattr(rule, "rule_id", None)
            or getattr(rule, "RuleId", None)
            or "<rule>",
            breadcrumb="Condition",
        )
        if me_sql:
            parts.append(f"({me_sql})")

        for prule in self._parent_rules_from_edges(parent_edges):
            if prule is None:
                continue
            pspec = self._extract_condition_spec(prule)
            psql = self._compile_condition_with_generators(
                pspec,
                rule=prule,
                rule_id=getattr(prule, "rule_id", None)
                or getattr(prule, "RuleId", None)
                or "<parent>",
                breadcrumb="ParentCondition",
            )
            if psql:
                parts.append(f"({psql})")

        if not parts:
            return None
        return " AND ".join(parts)

    def emit_sql_map(self) -> dict:
        """
        Build (but do not execute) every check for the current plan and
        return {rule_id: explanation_dict}.
        The explanation includes final SQL (with table name substituted) for leaves,
        effective row_condition_sql, and composite/reference/skip shapes.
        """
        if not getattr(self, "plan", None):
            raise RuntimeError("emit_sql_map() requires an attached plan")

        out = {}
        # however you iterate in validate(): use plan.schedule (list of node indices), or plan.nodes
        if self.plan is None:
            raise RuntimeError("emit_sql_map() requires an attached plan")
        schedule = getattr(self.plan, "schedule", None) or range(len(self.plan.nodes))

        for idx in schedule:
            node = self.plan.nodes[idx]
            rid = node.rule_id
            rule = node.rule

            # Gather the same parent context you use in validate()
            parent_results_by_idx = getattr(node, "parent_results_by_idx", {}) or {}
            parent_edges = getattr(node, "parent_edges", ()) or ()

            # Build (don’t run) the check
            check = self.build_check(
                rule=rule,
                rule_id=rid,
                node_idx=idx,
                parent_results_by_idx=parent_results_by_idx,
                parent_edges=parent_edges,
            )

            out[rid] = self._explain_check_sql(check)

        return out

    def explain(self) -> dict:
        return self.emit_sql_map()

    def _explain_check_sql(self, check) -> dict:
        """
        Produce a pure-data explanation for a single built check object, without executing it.
        """
        rid = getattr(check, "rule_id", None)
        ctype = getattr(check, "checkType", None) or getattr(check, "check_type", None)
        meta = getattr(check, "meta", {}) or {}

        # Skipped / dynamic
        if (
            getattr(check, "checkType", "") == "skipped_check"
            or hasattr(check, "is_skip")
            and check.is_skip
        ):
            return {
                "rule_id": rid,
                "type": "skipped",
                "check_type": ctype,
                "reason": getattr(check, "reason", None) or "dynamic rule",
                "sql": None,
                "row_condition_sql": None,
                "generator": meta.get("generator"),
            }

        # Composite (AND / OR)
        nested = getattr(check, "nestedChecks", None) or []
        handler = getattr(check, "nestedCheckHandler", None)
        if nested and handler:
            agg = (
                "all"
                if handler is all
                else (
                    "any"
                    if handler is any
                    else getattr(handler, "__name__", "aggregate")
                )
            )
            children = [self._explain_check_sql(ch) for ch in nested]
            return {
                "rule_id": rid,
                "type": "composite",
                "aggregate": agg,  # "all" (AND) or "any" (OR)
                "check_type": ctype,
                "generator": meta.get("generator"),
                "row_condition_sql": meta.get("row_condition_sql"),
                "children": children,
                # Many composite generators return "SELECT 0 AS violations"—not meaningful alone
                "sql": None,
            }

        # Conformance reference / special executor (no SQL)
        special = getattr(check, "special_executor", None)
        if callable(special):
            return {
                "rule_id": rid,
                "type": "reference",
                "check_type": ctype,
                "generator": meta.get("generator"),
                "row_condition_sql": meta.get("row_condition_sql"),
                "referenced": getattr(check, "referenced_rule_id", None),
                "sql": None,  # executed by reference, not SQL
                "note": "mirrors referenced rule outcome (no SQL)",
            }

        # Leaf with SQL
        sql = getattr(check, "checkSql", None) or getattr(check, "check_sql", None)
        return {
            "rule_id": rid,
            "type": "leaf",
            "check_type": ctype,
            "generator": meta.get("generator"),
            "row_condition_sql": meta.get("row_condition_sql"),
            "sql": self._subst_table(sql) if sql else None,
            "message": getattr(check, "errorMessage", None),
        }

    def _subst_table(self, sql: str) -> str:
        if not hasattr(self, "table_name") or not self.table_name:
            return sql
        return sql.replace("{{table_name}}", self.table_name).replace(
            "{table_name}", self.table_name
        )

    def print_sql_map(self, sql_map: dict):
        for rid, info in sql_map.items():
            t = info.get("type")
            print(f"\n=== {rid} [{t}] ===")
            rc = info.get("row_condition_sql")
            if rc:
                print(f"Condition: {rc}")
            if t == "leaf" and info.get("sql"):
                print(info["sql"])
            elif t == "composite":
                print(
                    f"Composite: {info.get('aggregate')} with {len(info.get('children', []))} items"
                )
            elif t == "reference":
                print(f"Reference to: {info.get('referenced')}")
            elif t == "skipped":
                print(f"Skipped: {info.get('reason')}")

    def update_global_results(
        self, node_idx: int, ok: bool, details: Dict[str, Any]
    ) -> None:
        """Update the global results registry for dependency propagation."""
        self._global_results_by_idx[node_idx] = {
            "ok": ok,
            "details": details,
            "rule_id": self.plan.nodes[node_idx].rule_id
            if self.plan and node_idx < len(self.plan.nodes)
            else None,
        }
