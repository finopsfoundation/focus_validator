import inspect
import json
import logging
import re
import textwrap
import time
from abc import ABC, abstractmethod
from types import MappingProxyType, SimpleNamespace
from typing import Any, Callable, ClassVar, Dict, List, Optional, Set, Tuple, Union

import duckdb  # type: ignore[import-untyped]
import sqlglot  # type: ignore[import-untyped]
import sqlglot.expressions as exp  # type: ignore[import-untyped]

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


# --- SQLGlot Integration -------------------------------------------------


class SQLQuery:
    """
    Enhanced SQL wrapper supporting both requirement and predicate modes with cross-database transpilation.

    This class bridges the existing dual-mode architecture (generateSql/generatePredicate) with SQLGlot's
    cross-database capabilities, maintaining full backward compatibility while enabling transpilation.
    """

    def __init__(self, requirement_sql: str, predicate_sql: Optional[str] = None):
        """
        Initialize with requirement SQL and optional predicate SQL.

        Args:
            requirement_sql: SQL for finding violations (SELECT ... AS violations)
            predicate_sql: Boolean predicate for WHERE clause filtering (optional)
        """
        self.requirement_sql = requirement_sql.strip() if requirement_sql else ""
        self.predicate_sql = predicate_sql.strip() if predicate_sql else None

        # Lazy parsing - only parse when transpilation is needed
        self._requirement_parsed = None
        self._predicate_parsed = None
        self._parse_error = None

    @property
    def requirement_parsed(self):
        """Lazily parse the requirement SQL"""
        if self._requirement_parsed is None and self.requirement_sql:
            try:
                # Replace template placeholders with dummy values for parsing
                sql_for_parsing = self.requirement_sql.replace(
                    "{table_name}", "dummy_table"
                )
                self._requirement_parsed = sqlglot.parse_one(
                    sql_for_parsing, dialect="duckdb"
                )
            except Exception as e:
                self._parse_error = f"Failed to parse requirement SQL: {e}"
                log.warning(f"SQLGlot parsing failed for requirement SQL: {e}")
        return self._requirement_parsed

    @property
    def predicate_parsed(self):
        """Lazily parse the predicate SQL if available"""
        if self.predicate_sql and self._predicate_parsed is None:
            try:
                # Parse as a conditional expression by wrapping in a SELECT
                wrapper_sql = f"SELECT * FROM dummy WHERE {self.predicate_sql}"
                full_parsed = sqlglot.parse_one(wrapper_sql, dialect="duckdb")
                where_clause = full_parsed.find(exp.Where)
                if where_clause:
                    self._predicate_parsed = where_clause.this
            except Exception as e:
                self._parse_error = f"Failed to parse predicate SQL: {e}"
                log.warning(f"SQLGlot parsing failed for predicate SQL: {e}")
        return self._predicate_parsed

    def transpile_requirement(self, target_dialect: str) -> str:
        """
        Transpile the requirement SQL to target dialect.
        Falls back to original SQL if transpilation fails.
        """
        if not self.requirement_sql:
            return ""

        if target_dialect.lower() == "duckdb":
            return self.requirement_sql

        parsed = self.requirement_parsed
        if parsed is None:
            log.warning(
                f"Cannot transpile requirement SQL to {target_dialect}, using original"
            )
            return self.requirement_sql

        try:
            # Transpile and restore template placeholders
            transpiled = parsed.sql(dialect=target_dialect)
            transpiled = transpiled.replace("dummy_table", "{table_name}")
            return transpiled
        except Exception as e:
            log.warning(f"Failed to transpile requirement SQL to {target_dialect}: {e}")
            return self.requirement_sql

    def transpile_predicate(self, target_dialect: str) -> Optional[str]:
        """
        Transpile the predicate SQL to target dialect.
        Falls back to original predicate if transpilation fails.
        """
        if not self.predicate_sql:
            return None

        if target_dialect.lower() == "duckdb":
            return self.predicate_sql

        parsed = self.predicate_parsed
        if parsed is None:
            log.warning(
                f"Cannot transpile predicate SQL to {target_dialect}, using original"
            )
            return self.predicate_sql

        try:
            return parsed.sql(dialect=target_dialect)
        except Exception as e:
            log.warning(f"Failed to transpile predicate SQL to {target_dialect}: {e}")
            return self.predicate_sql

    def get_requirement_sql(self, dialect: str = "duckdb") -> str:
        """Get requirement SQL for the specified dialect"""
        return self.transpile_requirement(dialect)

    def get_predicate_sql(self, dialect: str = "duckdb") -> Optional[str]:
        """Get predicate SQL for the specified dialect"""
        return self.transpile_predicate(dialect)

    @property
    def has_parsing_error(self) -> bool:
        """Check if there were any parsing errors"""
        return self._parse_error is not None

    @property
    def parsing_error(self) -> Optional[str]:
        """Get the parsing error message if any"""
        return self._parse_error


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
        self.force_fail_due_to_upstream: Optional[Dict[str, Any]] = (
            None  # For upstream dependency failures
        )
        self.sample_sql: Optional[str] = None  # For --show-violations feature

        # Attributes for dependency tracking and rule composition
        self._dependencies: Optional[Set[str]] = None
        self._child_rule_ids: Optional[List[str]] = None
        self._non_applicable: bool = False
        self._non_applicable_reason: Optional[str] = None


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
    def generateSql(self) -> Union[str, SQLQuery]:
        # Generate the SQL query for this check type
        # Can return either a string (backward compatibility) or SQLQuery object
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
        sql_result = self.generateSql()

        # Handle both SQLQuery and string returns for backward compatibility
        if isinstance(sql_result, SQLQuery):
            # Extract the requirement SQL for execution
            sql = sql_result.get_requirement_sql()
            # Store the SQLQuery object for potential transpilation
            setattr(self, "_sql_query", sql_result)
        else:
            # Legacy string return
            sql = sql_result
            setattr(self, "_sql_query", None)

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
                        or f"Validation rule {getattr(chk, 'rule_id', 'unknown')} failed",
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
        # For composite rules (with nested checks), don't set fallback error message
        # to allow runtime detailed message generation
        has_nested_checks = bool(
            child_checks or getattr(self, "nestedCheckHandler", None)
        )
        error_msg = getattr(self, "errorMessage", None)
        if not error_msg and not has_nested_checks:
            error_msg = f"Validation rule {self.rule_id} failed"
        elif not error_msg:
            # For composite rules, provide a fallback but allow runtime override
            error_msg = f"Validation rule {self.rule_id} failed"

        chk = DuckDBColumnCheck(
            rule_id=self.rule_id,
            rule=self.rule,
            check_type=self.getCheckType(),
            check_sql=sql,
            error_message=error_msg,
            nested_checks=child_checks or None,
            nested_check_handler=getattr(self, "nestedCheckHandler", None),
            meta=meta,
            special_executor=getattr(self, "special_executor", None),
            exec_mode=getattr(self, "exec_mode", None),
            referenced_rule_id=getattr(self, "referenced_rule_id", None),
        )

        # 5) Transfer SQLQuery object to the check if available
        if hasattr(self, "_sql_query"):
            setattr(chk, "_sql_query", getattr(self, "_sql_query"))

        # 6) Transfer generator-specific attributes to the check object
        if hasattr(self, "force_fail_due_to_upstream"):
            chk.force_fail_due_to_upstream = self.force_fail_due_to_upstream

        # Transfer dependencies for runtime checking of skipped dependencies
        if hasattr(self, "_dependencies"):
            chk._dependencies = self._dependencies

        # Transfer child rule IDs for composites
        if hasattr(self, "_child_rule_ids"):
            chk._child_rule_ids = self._child_rule_ids

        # Transfer non-applicable flags for three-scenario handling
        if hasattr(self, "_non_applicable"):
            chk._non_applicable = self._non_applicable
        if hasattr(self, "_non_applicable_reason"):
            chk._non_applicable_reason = self._non_applicable_reason

        # Transfer sample_sql for --show-violations feature
        # Note: sample_limit is now centralized in FocusToDuckDBSchemaConverter.DEFAULT_SAMPLE_LIMIT
        if hasattr(self, "sample_sql"):
            chk.sample_sql = self.sample_sql

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

    def _get_validation_keyword(self) -> str:
        """
        Extract the validation keyword (MUST, SHOULD, MAY, RECOMMENDED, etc.)
        from the rule's ValidationCriteria.

        Returns:
            str: The validation keyword, defaulting to "MUST" if not specified
        """
        if hasattr(self.rule, "validation_criteria"):
            criteria = self.rule.validation_criteria
            if hasattr(criteria, "keyword"):
                return criteria.keyword
        return "MUST"  # Default fallback

    def generatePredicate(self) -> str | None:
        """
        Return a SQL boolean expression (no SELECT), suitable for WHERE filters.

        Enhanced to work with SQLQuery objects:
        1. If generator returns SQLQuery with predicate_sql, use that
        2. Otherwise, fall back to subclass override (existing behavior)
        3. Default: None (generator not usable as a condition)
        """
        # Check if we have a SQLQuery object with predicate SQL
        sql_result = self.generateSql()
        if isinstance(sql_result, SQLQuery) and sql_result.predicate_sql:
            return sql_result.get_predicate_sql()

        # Fall back to existing behavior - subclasses can still override
        return None


class SkippedCheck(DuckDBCheckGenerator):
    REQUIRED_KEYS = set()

    def run(self, _conn) -> tuple[bool, dict]:
        return True, {"skipped": True, "reason": self.errorMessage, "violations": 0}

    def generateSql(self):
        self.errorMessage = (
            self.errorMessage or "Rule skipped - cannot be validated statically"
        )
        return None

    def getCheckType(self) -> str:
        return "skipped_check"


class SkippedDynamicCheck(SkippedCheck):
    def __init__(self, rule, rule_id: str, **kwargs: Any) -> None:
        super().__init__(rule, rule_id, **kwargs)
        self.errorMessage = (
            "Rule skipped - validation is dynamic and cannot be pre-generated"
        )


class SkippedOptionalCheck(SkippedCheck):
    def __init__(self, rule, rule_id: str, **kwargs: Any) -> None:
        super().__init__(rule, rule_id, **kwargs)
        self.errorMessage = "Rule skipped - marked as MAY/OPTIONAL and not enforced"


class SkippedNonApplicableCheck(SkippedCheck):
    def __init__(self, rule, rule_id: str, **kwargs: Any) -> None:
        super().__init__(rule, rule_id, **kwargs)
        self.errorMessage = (
            "Rule skipped - not applicable to current dataset or configuration"
        )


class ColumnPresentCheckGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnName"}

    def generateSql(self) -> SQLQuery:
        col = self.params.ColumnName
        keyword = self._get_validation_keyword()
        message = (
            self.errorMessage or f"Column '{col}' {keyword} be present in the table."
        )
        self.errorMessage = message  # <-- make sure run_check can see it
        msg_sql = message.replace("'", "''")

        # Requirement SQL (finds violations)
        requirement_sql = f"""
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

        # Note: Column presence checks don't have meaningful predicates for row-level filtering
        # since they operate at the schema level, not the data level
        predicate_sql = None

        return SQLQuery(
            requirement_sql=requirement_sql.strip(), predicate_sql=predicate_sql
        )

    def getCheckType(self) -> str:
        return "column_presence"


class TypeStringCheckGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnName"}

    # Generate type string validation check
    def generateSql(self) -> SQLQuery:
        col = self.params.ColumnName
        keyword = self._get_validation_keyword()
        message = self.errorMessage or f"{col} {keyword} be of type VARCHAR (string)."
        msg_sql = message.replace("'", "''")

        # Requirement SQL (finds violations)
        condition = f"{col} IS NOT NULL AND typeof({col}) != 'VARCHAR'"
        condition = self._apply_condition(condition)

        requirement_sql = f"""
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

        # Predicate SQL (for condition mode)
        predicate_sql = f"{col} IS NOT NULL AND typeof({col}) = 'VARCHAR'"

        return SQLQuery(
            requirement_sql=requirement_sql.strip(), predicate_sql=predicate_sql
        )

    def getCheckType(self) -> str:
        return "type_string"


class TypeDecimalCheckGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnName"}

    # Generate type decimal validation check
    def generateSql(self) -> SQLQuery:
        col = self.params.ColumnName
        keyword = self._get_validation_keyword()
        message = (
            self.errorMessage
            or f"{col} {keyword} be of type DECIMAL, DOUBLE, or FLOAT."
        )
        msg_sql = message.replace("'", "''")

        # Requirement SQL (finds violations)
        condition = (
            f"{col} IS NOT NULL AND typeof({col}) NOT IN ('DECIMAL', 'DOUBLE', 'FLOAT')"
        )
        condition = self._apply_condition(condition)

        requirement_sql = f"""
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

        # Predicate SQL (for condition mode)
        predicate_sql = (
            f"{col} IS NOT NULL AND typeof({col}) IN ('DECIMAL', 'DOUBLE', 'FLOAT')"
        )

        return SQLQuery(
            requirement_sql=requirement_sql.strip(), predicate_sql=predicate_sql
        )

    def getCheckType(self) -> str:
        return "type_decimal"


class TypeDateTimeGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnName"}

    # Validate the *type* is datetime-like:
    # - Accept native DATE, TIMESTAMP, TIMESTAMP_NS, TIMESTAMP WITH TIME ZONE
    # - Also accept ISO 8601 UTC text: YYYY-MM-DDTHH:mm:ssZ
    def generateSql(self) -> SQLQuery:
        col = self.params.ColumnName
        keyword = self._get_validation_keyword()
        message = (
            self.errorMessage
            or f"{col} {keyword} be a DATE/TIMESTAMP (with/without TZ) "
            f"or an ISO 8601 UTC string (YYYY-MM-DDTHH:mm:ssZ)."
        )
        msg_sql = message.replace("'", "''")

        # Requirement SQL (finds violations)
        condition = (
            f"{col} IS NOT NULL "
            f"AND typeof({col}) NOT IN ('TIMESTAMP', 'TIMESTAMP_NS', 'TIMESTAMP WITH TIME ZONE', 'DATE') "
            f"AND NOT ({col}::TEXT ~ '^[0-9]{{4}}-[0-1][0-9]-[0-3][0-9]T[0-2][0-9]:[0-5][0-9]:[0-5][0-9]Z$')"
        )
        condition = self._apply_condition(condition)

        requirement_sql = f"""
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

        # Predicate SQL (for condition mode)
        predicate_sql = (
            f"{col} IS NOT NULL "
            f"AND (typeof({col}) IN ('TIMESTAMP', 'TIMESTAMP_NS', 'TIMESTAMP WITH TIME ZONE', 'DATE') "
            f"OR ({col}::TEXT ~ '^[0-9]{{4}}-[0-1][0-9]-[0-3][0-9]T[0-2][0-9]:[0-5][0-9]:[0-5][0-9]Z$'))"
        )

        return SQLQuery(
            requirement_sql=requirement_sql.strip(), predicate_sql=predicate_sql
        )

    def getCheckType(self) -> str:
        return "type_datetime"


class FormatNumericGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnName"}

    # Generate numeric format validation check
    def generateSql(self) -> SQLQuery:
        col = self.params.ColumnName
        keyword = self._get_validation_keyword()
        message = (
            self.errorMessage
            or f"{col} {keyword} be a numeric value (optional +/- sign, optional decimal, optional scientific notation)."
        )
        msg_sql = message.replace("'", "''")

        # Requirement SQL (finds violations)
        # Pattern supports: 123, -123, 1.23, -1.23, 1.23e10, 1.23e-10, 1.23E+10, etc.
        condition = f"{col} IS NOT NULL AND NOT (TRIM({col}::TEXT) ~ '^[+-]?([0-9]*[.])?[0-9]+([eE][+-]?[0-9]+)?$')"
        condition = self._apply_condition(condition)

        requirement_sql = f"""
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

        # Predicate SQL (for condition mode)
        predicate_sql = f"{col} IS NOT NULL AND (TRIM({col}::TEXT) ~ '^[+-]?([0-9]*[.])?[0-9]+([eE][+-]?[0-9]+)?$')"

        return SQLQuery(
            requirement_sql=requirement_sql.strip(), predicate_sql=predicate_sql
        )

    def get_sample_sql(self) -> str:
        """Return SQL to fetch sample violating rows for display"""
        col = self.params.ColumnName

        # Build condition to find violating rows
        # Pattern supports: 123, -123, 1.23, -1.23, 1.23e10, 1.23e-10, 1.23E+10, etc.
        condition = f"{col} IS NOT NULL AND NOT (TRIM({col}::TEXT) ~ '^[+-]?([0-9]*[.])?[0-9]+([eE][+-]?[0-9]+)?$')"
        condition = self._apply_condition(condition)

        return f"""
        SELECT {col}
        FROM {{table_name}}
        WHERE {condition}
        """

    # Make sample_sql accessible as a property for the infrastructure
    @property
    def sample_sql(self) -> str:
        return self.get_sample_sql()

    def getCheckType(self) -> str:
        return "format_numeric"


class FormatStringGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnName"}

    # Generate string format validation check for ASCII characters
    def generateSql(self) -> SQLQuery:
        col = self.params.ColumnName
        keyword = self._get_validation_keyword()
        message = self.errorMessage or f"{col} {keyword} contain only ASCII characters."
        msg_sql = message.replace("'", "''")

        # Requirement SQL (finds violations)
        condition = f"{col} IS NOT NULL AND NOT ({col}::TEXT ~ '^[\\x00-\\x7F]*$')"
        condition = self._apply_condition(condition)

        requirement_sql = f"""
        WITH invalid AS (
            SELECT {col}::TEXT AS value
            FROM {{table_name}}
            WHERE {condition}
        )
        SELECT
            COUNT(*) AS violations,
            CASE WHEN COUNT(*) > 0 THEN '{msg_sql}' END AS error_message
        FROM invalid
        """

        # Predicate SQL (for condition mode)
        predicate_sql = f"{col} IS NOT NULL AND ({col}::TEXT ~ '^[\\x00-\\x7F]*$')"

        return SQLQuery(
            requirement_sql=requirement_sql.strip(), predicate_sql=predicate_sql
        )

    def get_sample_sql(self) -> str:
        """Return SQL to fetch sample violating rows for display"""
        col = self.params.ColumnName

        # Build condition to find violating rows (non-ASCII characters)
        condition = f"{col} IS NOT NULL AND NOT ({col}::TEXT ~ '^[\\x00-\\x7F]*$')"
        condition = self._apply_condition(condition)

        return f"""
        SELECT {col}
        FROM {{table_name}}
        WHERE {condition}
        """

    # Make sample_sql accessible as a property for the infrastructure
    @property
    def sample_sql(self) -> str:
        return self.get_sample_sql()

    def getCheckType(self) -> str:
        return "format_string"


class FormatDateTimeGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnName"}

    # Generate datetime validation check for valid UTC datetime values
    def generateSql(self) -> SQLQuery:
        col = self.params.ColumnName
        keyword = self._get_validation_keyword()
        message = (
            self.errorMessage or f"{col} {keyword} be a valid DateTime in UTC format"
        )
        msg_sql = message.replace("'", "''")

        # Requirement SQL (finds violations)
        # Check for valid datetime types (TIMESTAMP, TIMESTAMP_NS, TIMESTAMP WITH TIME ZONE, DATE)
        # or valid ISO 8601 UTC strings that can be parsed as datetime
        condition = (
            f"{col} IS NOT NULL "
            f"AND typeof({col}) NOT IN ('TIMESTAMP', 'TIMESTAMP_NS', 'TIMESTAMP WITH TIME ZONE', 'DATE') "
            f"AND NOT (typeof({col}) = 'VARCHAR' AND {col}::TEXT ~ '^[0-9]{{4}}-[0-1][0-9]-[0-3][0-9]T[0-2][0-9]:[0-5][0-9]:[0-5][0-9]Z?$' "
            f"AND TRY_CAST({col} AS TIMESTAMP) IS NOT NULL)"
        )
        condition = self._apply_condition(condition)

        requirement_sql = f"""
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

        # Predicate SQL (for condition mode)
        predicate_sql = (
            f"{col} IS NOT NULL "
            f"AND (typeof({col}) IN ('TIMESTAMP', 'TIMESTAMP_NS', 'TIMESTAMP WITH TIME ZONE', 'DATE') "
            f"OR (typeof({col}) = 'VARCHAR' AND {col}::TEXT ~ '^[0-9]{{4}}-[0-1][0-9]-[0-3][0-9]T[0-2][0-9]:[0-5][0-9]:[0-5][0-9]Z?$' "
            f"AND TRY_CAST({col} AS TIMESTAMP) IS NOT NULL))"
        )

        return SQLQuery(
            requirement_sql=requirement_sql.strip(), predicate_sql=predicate_sql
        )

    def getCheckType(self) -> str:
        return "format_datetime"


class FormatBillingCurrencyCodeGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnName"}

    def generateSql(self) -> SQLQuery:
        col = self.params.ColumnName
        keyword = self._get_validation_keyword()
        message = (
            self.errorMessage
            or f"{col} {keyword} be a valid ISO 4217 currency code (e.g., USD, EUR)."
        )
        msg_sql = message.replace("'", "''")

        # Get valid currency codes from CSV file
        valid_codes = get_currency_codes(
            code_file="focus_validator/rules/currency_codes.csv"
        )
        # Create SQL IN clause with properly quoted currency codes
        codes_list = "', '".join(sorted(valid_codes))

        # Requirement SQL (finds violations)
        condition = f"{col} IS NOT NULL AND TRIM({col}::TEXT) NOT IN ('{codes_list}')"
        condition = self._apply_condition(condition)

        requirement_sql = f"""
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

        # Predicate SQL (for condition mode)
        predicate_sql = f"{col} IS NOT NULL AND TRIM({col}::TEXT) IN ('{codes_list}')"

        return SQLQuery(
            requirement_sql=requirement_sql.strip(), predicate_sql=predicate_sql
        )

    def getCheckType(self) -> str:
        return "format_currency_code"


class FormatCurrencyGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnName"}

    # Generate national currency code validation check (ISO 4217)
    def generateSql(self) -> SQLQuery:
        col = self.params.ColumnName
        keyword = self._get_validation_keyword()
        message = (
            self.errorMessage
            or f"{col} {keyword} be a valid ISO 4217 currency code (3 uppercase letters, e.g. USD, EUR)."
        )
        msg_sql = message.replace("'", "''")

        # Requirement SQL (finds violations)
        condition = f"{col} IS NOT NULL AND NOT (TRIM({col}::TEXT) ~ '^[A-Z]{{3}}$')"
        condition = self._apply_condition(condition)

        requirement_sql = f"""
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

        # Predicate SQL (for condition mode)
        predicate_sql = f"{col} IS NOT NULL AND (TRIM({col}::TEXT) ~ '^[A-Z]{{3}}$')"

        return SQLQuery(
            requirement_sql=requirement_sql.strip(), predicate_sql=predicate_sql
        )

    def getCheckType(self) -> str:
        return "national_currency"


class FormatUnitGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnName"}

    def _generate_unit_format_regex(self) -> str:
        """
        Generate the complete regex pattern for FOCUS Unit Format validation.

        Returns:
            str: Combined regex pattern for all valid FOCUS unit formats
        """
        # Data Size Unit Names (both decimal and binary) - these are standardized
        data_size_units = [
            # Bits (decimal)
            "b",
            "Kb",
            "Mb",
            "Gb",
            "Tb",
            "Pb",
            "Eb",
            # Bytes (decimal)
            "B",
            "KB",
            "MB",
            "GB",
            "TB",
            "PB",
            "EB",
            # Bits (binary)
            "Kib",
            "Mib",
            "Gib",
            "Tib",
            "Pib",
            "Eib",
            # Bytes (binary)
            "KiB",
            "MiB",
            "GiB",
            "TiB",
            "PiB",
            "EiB",
        ]

        # Time-based Unit Names (must match exactly) - these are standardized
        time_units_singular = ["Year", "Month", "Day", "Hour", "Minute", "Second"]
        time_units_plural = ["Years", "Months", "Days", "Hours", "Minutes", "Seconds"]

        # Build regex patterns for each valid format according to FOCUS
        patterns = []

        # Data size unit pattern (exact match for standardized units)
        data_size_pattern = "|".join(data_size_units)

        # Time unit patterns (exact match for standardized units)
        time_singular_pattern = "|".join(time_units_singular)
        time_plural_pattern = "|".join(time_units_plural)

        # Count-based units pattern (flexible - matches alphanumeric words with optional spaces)
        # This captures any reasonable count unit like "Request", "API Request", "vCPU", "WriteCapacityUnit", etc.
        count_unit_pattern = r"[A-Za-z][A-Za-z0-9]*(?:\s+[A-Za-z][A-Za-z0-9]*)*"

        # Pattern 1: Standalone units
        # - Data size units: "GB", "KB", etc.
        # - Time units: "Year", "Hours", etc.
        # - Count units: "Request", "API Request", "vCPU", etc.
        patterns.append(
            f"^({data_size_pattern}|{time_singular_pattern}|{time_plural_pattern}|{count_unit_pattern})$"
        )

        # Pattern 2: <unit>-<plural-time-units> – "GB-Hours", "Request-Days", "API Request-Months"
        # Any unit (data size or count) combined with plural time units
        patterns.append(
            f"^({data_size_pattern}|{count_unit_pattern})-({time_plural_pattern})$"
        )

        # Pattern 3: <unit>/<singular-time-unit> – "GB/Hour", "Request/Day", "API Request/Second"
        # Any unit (data size or count) as a rate per time unit
        patterns.append(
            f"^({data_size_pattern}|{count_unit_pattern}|{time_plural_pattern})/({time_singular_pattern})$"
        )

        # Pattern 4: <quantity> <units> – "1000 Requests", "5000 API Requests"
        # Numeric quantity followed by any unit
        patterns.append(
            f"^[0-9]+ ({data_size_pattern}|{time_singular_pattern}|{time_plural_pattern}|{count_unit_pattern})$"
        )

        # Pattern 5: <units>/<interval> <plural-time-units> – "Requests/3 Months", "API Requests/5 Days"
        # Units per interval of time
        patterns.append(
            f"^({data_size_pattern}|{count_unit_pattern}|{time_plural_pattern})/[0-9]+ ({time_plural_pattern})$"
        )

        # Combine all patterns with OR
        return "|".join(f"({pattern})" for pattern in patterns)

    def generateSql(self) -> SQLQuery:
        col = self.params.ColumnName
        message = (
            self.errorMessage
            or f"Column '{col}' values SHOULD follow the FOCUS Unit Format specification."
        )
        self.errorMessage = message

        # Get the combined regex pattern
        combined_pattern = self._generate_unit_format_regex()

        # SQL for checking format compliance
        # Structure similar to other format generators - return violation count
        condition = (
            f"{col} IS NOT NULL AND NOT regexp_matches({col}, '{combined_pattern}')"
        )
        condition = self._apply_condition(condition)

        requirement_sql = f"""
        WITH invalid AS (
            SELECT 1
            FROM {{table_name}}
            WHERE {condition}
        )
        SELECT
            COUNT(*) AS violations,
            CASE WHEN COUNT(*) > 0 THEN 'Column ''{col}'' contains values that do not match FOCUS Unit Format specification' END AS error_message
        FROM invalid
        """

        # Predicate SQL (for condition mode)
        predicate_sql = (
            f"{col} IS NOT NULL AND regexp_matches({col}, '{combined_pattern}')"
        )

        return SQLQuery(
            requirement_sql=requirement_sql.strip(), predicate_sql=predicate_sql
        )

    def get_sample_sql(self) -> str:
        """Return SQL to fetch sample violating rows for display"""
        col = self.params.ColumnName

        # Use the same centralized regex pattern generation
        combined_pattern = self._generate_unit_format_regex()

        # SQL to return violating rows with the column value
        condition = (
            f"{col} IS NOT NULL AND NOT regexp_matches({col}, '{combined_pattern}')"
        )
        condition = self._apply_condition(condition)

        return f"""
        SELECT {col}
        FROM {{table_name}}
        WHERE {condition}
        """

    # Make sample_sql accessible as a property for the infrastructure
    @property
    def sample_sql(self) -> str:
        return self.get_sample_sql()

    def getCheckType(self) -> str:
        return "format_unit"


class FormatJSONGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnName"}

    # Generate JSON format validation check for valid JSON structures
    def generateSql(self) -> SQLQuery:
        col = self.params.ColumnName
        keyword = self._get_validation_keyword()
        message = self.errorMessage or f"{col} {keyword} be valid JSON format"
        msg_sql = message.replace("'", "''")

        # Requirement SQL (finds violations)
        # Check if column is not null and either:
        # 1. Cannot be cast to JSON, or
        # 2. Is not a valid JSON string when treated as text
        condition = (
            f"{col} IS NOT NULL "
            f"AND (TRY_CAST({col} AS JSON) IS NULL "
            f"OR (typeof({col}) = 'VARCHAR' AND NOT json_valid({col}::TEXT)))"
        )
        condition = self._apply_condition(condition)

        requirement_sql = f"""
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

        # Predicate SQL (for condition mode)
        predicate_sql = (
            f"{col} IS NOT NULL "
            f"AND (TRY_CAST({col} AS JSON) IS NOT NULL "
            f"OR (typeof({col}) = 'VARCHAR' AND json_valid({col}::TEXT)))"
        )

        return SQLQuery(
            requirement_sql=requirement_sql.strip(), predicate_sql=predicate_sql
        )

    def getCheckType(self) -> str:
        return "format_json"


class CheckValueGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnName", "Value"}

    def generateSql(self) -> SQLQuery:
        col = self.params.ColumnName
        value = self.params.Value
        keyword = self._get_validation_keyword()

        # Build requirement SQL (finds violations)
        if value is None:
            message = self.errorMessage or f"{col} {keyword} be NULL."
            condition = f"{col} IS NOT NULL"
            predicate = f"{col} IS NULL"  # Condition: rows where requirement applies
        else:
            val_escaped = str(value).replace("'", "''")
            message = self.errorMessage or f"{col} {keyword} equal '{value}'."
            condition = f"{col} != '{val_escaped}'"
            predicate = (
                f"{col} = '{val_escaped}'"  # Condition: rows where requirement applies
            )

        # Apply conditional logic if present (for requirement SQL)
        condition = self._apply_condition(condition)
        msg_sql = message.replace("'", "''")

        requirement_sql = f"""
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

        return SQLQuery(
            requirement_sql=requirement_sql.strip(), predicate_sql=predicate
        )

    def get_sample_sql(self) -> str:
        """Return SQL to fetch sample violating rows for display"""
        col = self.params.ColumnName
        value = self.params.Value

        # Build condition to find violating rows
        if value is None:
            condition = f"{col} IS NOT NULL"
        else:
            val_escaped = str(value).replace("'", "''")
            condition = f"{col} != '{val_escaped}'"

        # Apply conditional logic if present
        condition = self._apply_condition(condition)

        return f"""
        SELECT {col}
        FROM {{table_name}}
        WHERE {condition}
        """

    # Make sample_sql accessible as a property for the infrastructure
    @property
    def sample_sql(self) -> str:
        return self.get_sample_sql()

    def getCheckType(self) -> str:
        return "check_value"

    def generatePredicate(self) -> str | None:
        """
        Backward compatibility: extract predicate from SQLQuery
        """
        if getattr(self, "exec_mode", "requirement") != "condition":
            return None

        sql_query = self.generateSql()
        return sql_query.get_predicate_sql()


class CheckNotValueGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnName", "Value"}

    def generateSql(self) -> SQLQuery:
        col = self.params.ColumnName
        value = self.params.Value
        keyword = self._get_validation_keyword()

        # Build requirement SQL (finds violations)
        if value is None:
            # Handle keywords that already contain "NOT" (e.g., "MUST NOT")
            if "NOT" in keyword.upper():
                message = self.errorMessage or f"{col} {keyword} be NULL."
            else:
                message = self.errorMessage or f"{col} {keyword} NOT be NULL."
            condition = f"{col} IS NULL"
            predicate = (
                f"{col} IS NOT NULL"  # Condition: rows where requirement applies
            )
        else:
            val_escaped = str(value).replace("'", "''")
            # Handle keywords that already contain "NOT" (e.g., "MUST NOT")
            if "NOT" in keyword.upper():
                message = self.errorMessage or f"{col} {keyword} be '{value}'."
            else:
                message = self.errorMessage or f"{col} {keyword} NOT be '{value}'."
            condition = f"({col} IS NOT NULL AND {col} = '{val_escaped}')"
            predicate = f"({col} IS NOT NULL AND {col} <> '{val_escaped}')"

        # Apply conditional logic if present
        condition = self._apply_condition(condition)
        msg_sql = message.replace("'", "''")

        requirement_sql = f"""
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

        return SQLQuery(
            requirement_sql=requirement_sql.strip(), predicate_sql=predicate
        )

    def get_sample_sql(self) -> str:
        """Return SQL to fetch sample violating rows for display"""
        col = self.params.ColumnName
        value = self.params.Value

        # Build condition to find violating rows
        if value is None:
            condition = f"{col} IS NULL"
        else:
            val_escaped = str(value).replace("'", "''")
            condition = f"({col} IS NOT NULL AND {col} = '{val_escaped}')"

        # Apply conditional logic if present
        condition = self._apply_condition(condition)

        return f"""
        SELECT {col}
        FROM {{table_name}}
        WHERE {condition}
        """

    # Make sample_sql accessible as a property for the infrastructure
    @property
    def sample_sql(self) -> str:
        return self.get_sample_sql()

    def getCheckType(self) -> str:
        return "check_not_value"

    def generatePredicate(self) -> str | None:
        """Backward compatibility wrapper"""
        if self.exec_mode != "condition":
            return None
        sql_query = self.generateSql()
        return sql_query.get_predicate_sql()


class CheckSameValueGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnAName", "ColumnBName"}

    def generateSql(self) -> SQLQuery:
        col_a = self.params.ColumnAName
        col_b = self.params.ColumnBName
        keyword = self._get_validation_keyword()
        message = (
            self.errorMessage or f"{col_a} and {col_b} {keyword} have the same value."
        )
        msg_sql = message.replace("'", "''")

        # Requirement SQL (finds violations)
        condition = (
            f"{col_a} IS NOT NULL AND {col_b} IS NOT NULL AND {col_a} <> {col_b}"
        )
        condition = self._apply_condition(condition)

        requirement_sql = f"""
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

        # Predicate SQL (for condition mode)
        predicate_sql = (
            f"{col_a} IS NOT NULL AND {col_b} IS NOT NULL AND {col_a} = {col_b}"
        )

        return SQLQuery(
            requirement_sql=requirement_sql.strip(), predicate_sql=predicate_sql
        )

    def get_sample_sql(self) -> str:
        """Return SQL to fetch sample violating rows for display"""
        col_a = self.params.ColumnAName
        col_b = self.params.ColumnBName

        # Build condition to find violating rows (where columns are NOT the same)
        condition = (
            f"{col_a} IS NOT NULL AND {col_b} IS NOT NULL AND {col_a} <> {col_b}"
        )
        condition = self._apply_condition(condition)

        return f"""
        SELECT {col_a}, {col_b}
        FROM {{table_name}}
        WHERE {condition}
        """

    # Make sample_sql accessible as a property for the infrastructure
    @property
    def sample_sql(self) -> str:
        return self.get_sample_sql()

    def getCheckType(self) -> str:
        return "column_comparison_equals"

    def generatePredicate(self) -> str | None:
        """Backward compatibility wrapper"""
        if getattr(self, "exec_mode", "requirement") != "condition":
            return None
        sql_query = self.generateSql()
        return sql_query.get_predicate_sql()


class CheckNotSameValueGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnAName", "ColumnBName"}

    def generateSql(self) -> SQLQuery:
        col_a = self.params.ColumnAName
        col_b = self.params.ColumnBName
        keyword = self._get_validation_keyword()
        # Handle keywords that already contain "NOT" (e.g., "MUST NOT")
        if "NOT" in keyword.upper():
            message = (
                self.errorMessage
                or f"{col_a} and {col_b} {keyword} have the same value."
            )
        else:
            message = (
                self.errorMessage
                or f"{col_a} and {col_b} {keyword} NOT have the same value."
            )
        msg_sql = message.replace("'", "''")

        # Requirement SQL (finds violations)
        condition = f"{col_a} IS NOT NULL AND {col_b} IS NOT NULL AND {col_a} = {col_b}"
        condition = self._apply_condition(condition)

        requirement_sql = f"""
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

        # Predicate SQL (for condition mode)
        predicate_sql = (
            f"{col_a} IS NOT NULL AND {col_b} IS NOT NULL AND {col_a} <> {col_b}"
        )

        return SQLQuery(
            requirement_sql=requirement_sql.strip(), predicate_sql=predicate_sql
        )

    def get_sample_sql(self) -> str:
        """Return SQL to fetch sample violating rows for display"""
        col_a = self.params.ColumnAName
        col_b = self.params.ColumnBName

        # Build condition to find violating rows (where columns ARE the same but shouldn't be)
        condition = f"{col_a} IS NOT NULL AND {col_b} IS NOT NULL AND {col_a} = {col_b}"
        condition = self._apply_condition(condition)

        return f"""
        SELECT {col_a}, {col_b}
        FROM {{table_name}}
        WHERE {condition}
        """

    # Make sample_sql accessible as a property for the infrastructure
    @property
    def sample_sql(self) -> str:
        return self.get_sample_sql()

    def getCheckType(self) -> str:
        return "column_comparison_not_equals"

    def generatePredicate(self) -> str | None:
        """Backward compatibility wrapper"""
        if getattr(self, "exec_mode", "requirement") != "condition":
            return None
        sql_query = self.generateSql()
        return sql_query.get_predicate_sql()


class CheckDecimalValueGenerator(SkippedCheck):
    def __init__(self, rule, rule_id: str, **kwargs: Any) -> None:
        super().__init__(rule, rule_id, **kwargs)
        self.errorMessage = (
            "Rule skipped - CheckDecimalValue validation is not yet implemented"
        )


class ColumnByColumnEqualsColumnValueGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnAName", "ColumnBName", "ResultColumnName"}

    def generateSql(self) -> SQLQuery:
        a = self.params.ColumnAName
        b = self.params.ColumnBName
        r = self.params.ResultColumnName
        message = self.errorMessage or f"Expected {r} = {a} * {b}"
        msg_sql = message.replace("'", "''")

        # Requirement SQL (finds violations)
        condition = f"{a} IS NOT NULL AND {b} IS NOT NULL AND {r} IS NOT NULL AND ({a} * {b}) <> {r}"
        condition = self._apply_condition(condition)

        requirement_sql = f"""
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

        # Predicate SQL (for condition mode)
        predicate_sql = f"{a} IS NOT NULL AND {b} IS NOT NULL AND {r} IS NOT NULL AND ({a} * {b}) = {r}"

        return SQLQuery(
            requirement_sql=requirement_sql.strip(), predicate_sql=predicate_sql
        )

    def getCheckType(self) -> str:
        return "column_by_column_equals_column_value"

    def generatePredicate(self) -> str | None:
        """Backward compatibility wrapper"""
        if getattr(self, "exec_mode", "requirement") != "condition":
            return None
        sql_query = self.generateSql()
        return sql_query.get_predicate_sql()


class CheckGreaterOrEqualGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnName", "Value"}

    def generateSql(self) -> SQLQuery:
        col = self.params.ColumnName
        val = self.params.Value
        keyword = self._get_validation_keyword()
        message = (
            self.errorMessage or f"{col} {keyword} be greater than or equal to {val}."
        )
        msg_sql = message.replace("'", "''")

        # Requirement SQL (finds violations)
        condition = f"{col} IS NOT NULL AND {col} < {val}"
        condition = self._apply_condition(condition)

        requirement_sql = f"""
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

        # Predicate SQL (for condition mode)
        predicate_sql = f"{col} IS NOT NULL AND {col} >= {self._lit(val)}"

        return SQLQuery(
            requirement_sql=requirement_sql.strip(), predicate_sql=predicate_sql
        )

    def get_sample_sql(self) -> str:
        """Return SQL to fetch sample violating rows for display"""
        col = self.params.ColumnName
        val = self.params.Value

        # Build condition to find violating rows (values less than the required minimum)
        condition = f"{col} IS NOT NULL AND {col} < {val}"
        condition = self._apply_condition(condition)

        return f"""
        SELECT {col}
        FROM {{table_name}}
        WHERE {condition}
        """

    # Make sample_sql accessible as a property for the infrastructure
    @property
    def sample_sql(self) -> str:
        return self.get_sample_sql()

    def getCheckType(self) -> str:
        return "check_greater_equal"

    def generatePredicate(self) -> str | None:
        """Backward compatibility wrapper"""
        if getattr(self, "exec_mode", "requirement") != "condition":
            return None
        sql_query = self.generateSql()
        return sql_query.get_predicate_sql()


class CheckDistinctCountGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnAName", "ColumnBName", "ExpectedCount"}

    def generateSql(self) -> SQLQuery:
        a = self.params.ColumnAName
        b = self.params.ColumnBName
        n = self.params.ExpectedCount
        keyword = self._get_validation_keyword()

        message = (
            self.errorMessage
            or f"For each {a}, there {keyword} be exactly {n} distinct {b} values."
        )
        msg_sql = message.replace("'", "''")

        # Build WHERE clause for row-level filtering before aggregation
        # This applies parent conditions (e.g., "SkuPriceId IS NOT NULL") before GROUP BY
        where_clause = ""
        if self.row_condition_sql and self.row_condition_sql.strip():
            where_clause = f"WHERE {self.row_condition_sql}"

        # Requirement SQL (finds violations)
        # IMPORTANT: Apply row_condition_sql BEFORE GROUP BY to filter groups themselves
        requirement_sql = f"""
        WITH counts AS (
            SELECT {a} AS grp, COUNT(DISTINCT {b}) AS distinct_count
            FROM {{table_name}}
            {where_clause}
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

        # Note: This is a complex aggregation check that doesn't naturally translate
        # to a simple predicate for row-level filtering. Setting predicate_sql to None.
        predicate_sql = None

        return SQLQuery(
            requirement_sql=requirement_sql.strip(), predicate_sql=predicate_sql
        )

    def getCheckType(self) -> str:
        return "distinct_count"


class CheckModelRuleGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ModelRuleId"}

    def getCheckType(self) -> str:
        return "model_rule_reference"

    def generateSql(self) -> SQLQuery:
        # Won’t be executed; we’ll attach a special executor instead.
        self.errorMessage = f"Conformance reference to {self.params.ModelRuleId}"
        requirement_sql = "SELECT 0 AS violations"
        return SQLQuery(requirement_sql=requirement_sql.strip(), predicate_sql=None)

    def generateCheck(self) -> DuckDBColumnCheck:
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
        chk.referenced_rule_id = target_id
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

    def generateSql(self) -> SQLQuery:  # noqa: C901
        if not callable(self.child_builder):
            raise RuntimeError(f"{self.__class__.__name__} requires child_builder")

        items = self.p.get("Items")
        if not isinstance(items, list) or not items:
            raise InvalidRuleException(
                f"{self.rule_id} @ {self.breadcrumb}: {self.COMPOSITE_NAME} needs non-empty 'Items'"
            )

        children = []
        child_rule_ids = []  # Store child rule IDs for later reference
        for i, child_req in enumerate(items):
            if not isinstance(child_req, dict) or "CheckFunction" not in child_req:
                raise InvalidRuleException(
                    f"{self.rule_id} @ {self.breadcrumb}: Item[{i}] must be a requirement dict with 'CheckFunction'"
                )
            child_bc = f"{self.breadcrumb} > {self.COMPOSITE_NAME}[{i}]"
            # IMPORTANT: pass the REQUIREMENT DICT here
            child_check = self.child_builder(child_req, child_bc)
            children.append(child_check)

            # Extract child rule ID for reference
            if child_req.get("CheckFunction") == "CheckModelRule":
                child_rule_id = child_req.get("ModelRuleId")
                if child_rule_id:
                    child_rule_ids.append(child_rule_id)
            # For other check functions, we might not have a clear rule ID
            # In that case, we'll use the breadcrumb or a generated ID

        # Store child rule IDs in the generator for later transfer to check object
        self._child_rule_ids = child_rule_ids

        # --- identify upstream failed deps (excluding Items) ------------------------
        # 1) collect failed parent rule_ids from immediate parents
        failed_parent_rule_ids = set()
        skipped_parent_rule_ids = set()
        if self.plan:
            for pidx, pres in self.parent_results_by_idx.items():
                if not pres.get("ok", True):
                    failed_parent_rule_ids.add(self.plan.nodes[pidx].rule_id)
                # Track skipped parents too
                if pres.get("details", {}).get("skipped", False):
                    skipped_parent_rule_ids.add(self.plan.nodes[pidx].rule_id)

            # ENHANCED: Also check ALL previously executed rules for failures and skips
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
                    # Track skipped rules
                    if result.get("details", {}).get("skipped", False):
                        if node_idx < len(self.plan.nodes):
                            skipped_parent_rule_ids.add(
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

        # 4) Check if any CheckModelRule references have failed or been skipped
        failed_conformance_refs = []
        skipped_conformance_refs = []
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
                            is_skipped = result.get("details", {}).get("skipped", False)

                            if is_skipped:
                                # If dependency was skipped, mark this composite as skipped too
                                skipped_conformance_refs.append(rule_id)
                            elif not is_ok:
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

        # Check for skipped dependencies (scenario 2: column absent when criteria not met)
        # If a dependency was skipped, all dependent rules should also be skipped
        all_deps_skipped = []
        if self.plan and deps:
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
                for dep_rule_id in deps:
                    for node_idx, result in converter._global_results_by_idx.items():
                        if node_idx < len(self.plan.nodes):
                            if self.plan.nodes[node_idx].rule_id == dep_rule_id:
                                # If dependency was skipped, this composite should also be skipped
                                if result.get("details", {}).get("skipped", False):
                                    all_deps_skipped.append(dep_rule_id)
                                break

        external_skipped_candidates = sorted(set(deps) & skipped_parent_rule_ids)
        all_skipped = sorted(
            set(external_skipped_candidates)
            | set(skipped_conformance_refs)
            | set(all_deps_skipped)
        )

        # If any dependency was skipped, mark this composite to be skipped
        if all_skipped:
            self.force_skip_due_to_upstream = {
                "skipped_dependencies": all_skipped,
                "reason": "upstream dependency was skipped",
            }
            skip_reason = f"Rule skipped - dependent rule(s) were skipped: {', '.join(all_skipped)}"
            self.errorMessage = skip_reason

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

        # Store dependencies for runtime checking
        # This is crucial for detecting skipped dependencies at execution time
        self._dependencies = []

        if deps:
            # Convert dependency rule IDs to check objects by finding them in the plan
            if self.plan:
                for dep_id in deps:
                    # Find the node with this rule_id in the plan
                    for node in self.plan.nodes:
                        if node.rule_id == dep_id:
                            # Store a reference to the rule with its idx
                            dep_ref = type(
                                "DependencyRef",
                                (),
                                {
                                    "rule_id": dep_id,
                                    "rule_global_idx": node.idx,
                                    "referenced_rule_id": dep_id,
                                },
                            )()
                            self._dependencies.append(dep_ref)
                            break

        # Don't set a static errorMessage for composites - let runtime logic provide detailed failure info
        # Only set errorMessage if explicitly provided in the rule specification
        if self.p.get("Message"):
            self.errorMessage = self.p.get("Message")
        # Otherwise, errorMessage will be None and _msg_for_outcome will use the detailed fallback_fail message

        # For composites, requirement SQL is typically trivial since logic is in nested checks
        requirement_sql = "SELECT 0 AS violations"

        # Predicate SQL is built by combining child predicates (handled in generatePredicate)
        predicate_sql = (
            self.generatePredicate() if self.exec_mode == "condition" else None
        )

        return SQLQuery(requirement_sql=requirement_sql, predicate_sql=predicate_sql)

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

    def generateCheck(self) -> DuckDBColumnCheck:
        """
        Override to create a custom OR check that handles the semantic flip.
        In OR context, we want "at least one condition passes" rather than "all conditions pass".
        """
        # Let the base class do most of the work
        check = super().generateCheck()

        # Add a special executor that handles OR semantics correctly
        if hasattr(check, "nestedChecks") and check.nestedChecks:
            original_nested_checks = check.nestedChecks
            original_handler = check.nestedCheckHandler

            def _or_special_executor(conn):
                """
                Custom executor for OR that handles the semantic interpretation correctly.
                For OR composites, each child check should pass if it finds ANY matching rows,
                not if ALL rows match.
                """
                # We need access to the converter's run_check method
                # This is a bit hacky, but we'll get it from the closure or context
                converter = None

                frame = inspect.currentframe()
                try:
                    while frame:
                        if "self" in frame.f_locals and hasattr(
                            frame.f_locals["self"], "run_check"
                        ):
                            converter = frame.f_locals["self"]
                            break
                        frame = frame.f_back
                finally:
                    del frame

                if not converter:
                    # Fallback to original behavior if we can't find converter
                    return (
                        original_handler([False] * len(original_nested_checks))
                        if original_handler
                        else (False, {"error": "No converter found"})
                    )

                # Run each child and collect results with OR semantics
                child_oks = []
                child_details = []
                total_rows = None
                composite_rule_id = getattr(check, "rule_id", None)

                for i, child in enumerate(original_nested_checks):
                    ok_i, det_i = converter.run_check(child)
                    violations = det_i.get("violations", 1)

                    # Get total row count if we don't have it yet
                    if total_rows is None:
                        try:
                            # Quick way to get row count
                            result = conn.execute(
                                "SELECT COUNT(*) as total FROM {table_name}".format(
                                    table_name=converter.table_name
                                )
                            ).fetchone()
                            total_rows = result[0] if result else 1
                        except Exception:
                            total_rows = 1  # Fallback

                    # Debug logging for ServiceCategory
                    child_check_type = getattr(child, "checkType", None) or getattr(
                        child, "check_type", None
                    )

                    # OR semantics: child passes if it has fewer violations than total rows
                    # (meaning at least one row matched the condition)
                    or_child_ok = violations < total_rows

                    child_oks.append(or_child_ok)

                    # Update the details to reflect OR semantics
                    det_i["ok"] = or_child_ok  # Update ok status to match OR semantics
                    det_i["violations"] = 0 if or_child_ok else 1
                    det_i["or_adjusted"] = True  # Mark that we adjusted this

                    # Generate unique child ID
                    child_rule_id = getattr(child, "rule_id", None)
                    child_check_type = getattr(child, "checkType", None) or getattr(
                        child, "check_type", None
                    )

                    if child_rule_id and child_rule_id != composite_rule_id:
                        unique_child_id = child_rule_id
                    elif child_check_type:
                        unique_child_id = f"{child_check_type}#{i + 1}"
                    else:
                        unique_child_id = f"child#{i + 1}"

                    child_details.append({**det_i, "rule_id": unique_child_id})

                # OR passes if ANY child passes
                overall_ok = any(child_oks)

                # Collect information about failed rule IDs from child_details
                failed_rule_ids = []
                passed_rule_ids = []
                for child_detail, child_ok in zip(child_details, child_oks):
                    child_desc = child_detail.get("rule_id", "<child>")
                    if child_ok:
                        passed_rule_ids.append(child_desc)
                    else:
                        failed_rule_ids.append(child_desc)

                # For the composite level, if OR fails, report the actual number of failing rows
                # This is the original violation count from any child (they should all be the same)
                composite_violations = 0
                if not overall_ok:
                    # Find the original violation count from the first child that was adjusted
                    for child_detail in child_details:
                        if (
                            child_detail.get("or_adjusted")
                            and "violations" in child_detail
                        ):
                            # We need to recover the original violation count
                            # Since all children should have had the same violation count (total_rows)
                            # when none of the conditions matched, use total_rows
                            composite_violations = total_rows
                            break
                    if composite_violations == 0:  # Fallback
                        composite_violations = 1

                # Build detailed message for OR composite
                rule_id = getattr(check, "rule_id", "<rule>")
                if overall_ok:
                    message = f"{rule_id}: OR passed - satisfied by rules: [{', '.join(passed_rule_ids)}]"
                else:
                    message = f"{rule_id}: OR failed - all child rules failed: [{', '.join(failed_rule_ids)}]"

                details = {
                    "children": child_details,
                    "aggregated": "any",
                    "message": message,
                    "violations": composite_violations,
                    "check_type": "composite",
                    "or_semantic_adjustment": True,
                    "failed_rule_ids": failed_rule_ids,
                    "passed_rule_ids": passed_rule_ids,
                }

                return overall_ok, details

            check.special_executor = _or_special_executor

        return check

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
    # Central configuration for sample violation data collection
    DEFAULT_SAMPLE_LIMIT = 2  # Number of sample violation rows to collect when --show-violations is enabled

    # Default registry for all check types with both generators and check object factories
    # This serves as the base mapping that all versions inherit from
    _DEFAULT_CHECK_GENERATORS: dict[str, Dict[str, Any]] = {
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
            "generator": FormatJSONGenerator,
            "factory": lambda args: "ColumnName",
        },
        "FormatCurrency": {
            "generator": FormatCurrencyGenerator,
            "factory": lambda args: "ColumnName",
        },
        "CheckNationalCurrency": {
            "generator": FormatBillingCurrencyCodeGenerator,
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

    # Version-specific overrides: each version only defines what changes from previous versions
    # Format: {"version": {"CheckFunction": {"generator": ..., "factory": ...}}}
    _VERSION_OVERRIDES: dict[str, dict[str, Dict[str, Any]]] = {
        # Future versions can be added here with only the changes needed
        # Example:
        # "1.3": {
        #     "NewCheckFunction": {
        #         "generator": NewCheckGenerator,
        #         "factory": lambda args: "ColumnName",
        #     },
        #     "FormatUnit": {
        #         "generator": FormatUnitV13Generator,  # hypothetical updated generator
        #         "factory": lambda args: "ColumnName",
        #     },
        # },
    }

    @classmethod
    def _parse_version(cls, version_str: str) -> Tuple[int, ...]:
        """Parse a version string like '1.2' into a tuple of integers for comparison."""
        try:
            return tuple(int(x) for x in version_str.split("."))
        except ValueError:
            # If parsing fails, treat as a very low version
            return (0,)

    @classmethod
    def _find_best_version(cls, target_version: Optional[str]) -> Optional[str]:
        """
        Find the best available version for the given target version.
        Returns exact match if available, otherwise the highest version that's <= target_version.
        If no target_version is provided, returns None (use defaults).
        """
        if not target_version:
            return None

        available_versions = list(cls._VERSION_OVERRIDES.keys())
        if not available_versions:
            return None

        target_parsed = cls._parse_version(target_version)

        # Check for exact match first
        if target_version in available_versions:
            return target_version

        # Find the highest version that's <= target_version
        candidates = []
        for v in available_versions:
            v_parsed = cls._parse_version(v)
            if v_parsed <= target_parsed:
                candidates.append((v_parsed, v))

        if candidates:
            # Sort by version tuple and return the highest
            candidates.sort(key=lambda x: x[0])
            return candidates[-1][1]

        return None

    @classmethod
    def _build_check_generators_for_version(
        cls, version: Optional[str]
    ) -> dict[str, Dict[str, Any]]:
        """
        Build the effective CHECK_GENERATORS mapping for the given version.
        Uses defaultdict to inherit from defaults and applies version-specific overrides.
        """
        # Start with defaults
        effective_generators: Dict[str, Dict[str, Any]] = {}
        effective_generators.update(cls._DEFAULT_CHECK_GENERATORS)

        if not version:
            # No version specified, use defaults only
            return effective_generators

        best_version = cls._find_best_version(version)
        if not best_version:
            # No suitable version found, use defaults
            return effective_generators

        # Apply overrides for the best version and all versions leading up to it
        available_versions = list(cls._VERSION_OVERRIDES.keys())
        best_parsed = cls._parse_version(best_version)

        # Get all versions <= best_version, sorted
        applicable_versions = []
        for v in available_versions:
            v_parsed = cls._parse_version(v)
            if v_parsed <= best_parsed:
                applicable_versions.append((v_parsed, v))

        # Sort by version and apply overrides in order
        applicable_versions.sort(key=lambda x: x[0])

        for _, v in applicable_versions:
            overrides = cls._VERSION_OVERRIDES.get(v, {})
            for check_function, config in overrides.items():
                effective_generators[check_function] = config

        return effective_generators

    def __init__(
        self,
        *,
        focus_data: Any,
        focus_table_name: str = "focus_data",
        pragma_threads: int | None = None,
        explain_mode: bool = False,
        validated_applicability_criteria: Optional[List[str]] = None,
        transpile_dialect: Optional[str] = None,
        show_violations: bool = False,
        rules_version: Optional[str] = None,
    ) -> None:
        self.log = logging.getLogger(f"{__name__}.{self.__class__.__qualname__}")
        self.conn: duckdb.DuckDBPyConnection | None = None
        self.plan: ValidationPlan | None = None
        self.pragma_threads = pragma_threads
        self.focus_data = focus_data
        self.table_name = focus_table_name
        self.validated_applicability_criteria = validated_applicability_criteria or []
        self.transpile_dialect = (
            transpile_dialect  # Target dialect for SQL transpilation
        )
        self.show_violations = show_violations
        self.rules_version = rules_version

        # Build the effective CHECK_GENERATORS mapping for this version
        self.CHECK_GENERATORS = self._build_check_generators_for_version(rules_version)

        # Example caches (optional)
        self._prepared: Dict[str, Any] = {}
        self._views: Dict[str, str] = {}  # rule_id -> temp view name
        self.explain_mode = explain_mode
        # Global results registry for dependency failure propagation
        self._global_results_by_idx: Dict[int, Dict[str, Any]] = {}

    def get_rules_version(self) -> Optional[str]:
        """Get the FOCUS rules version being used for validation.

        Returns:
            The validation version (e.g., "1.2", "1.3") or None if not set
        """
        return self.rules_version

    def _should_include_rule(
        self, rule: Any, parent_edges: Optional[Tuple[Any, ...]] = None
    ) -> bool:
        """Check if a rule should be included based on applicability criteria.

        A rule is included if:
        1. It has no applicability criteria (always included)
        2. It has applicability criteria that match the provided criteria

        Note: Rules with empty applicability criteria are ALWAYS included,
        regardless of parent applicability. Parent applicability is only
        checked for rules that themselves have applicability criteria.
        """
        # Check if rule has applicability criteria
        rule_criteria = (
            rule.applicability_criteria
            if hasattr(rule, "applicability_criteria") and rule.applicability_criteria
            else []
        )

        # If rule has no applicability criteria, always include it
        # Do NOT check parent applicability for such rules
        if not rule_criteria:
            return True

        # Rule has applicability criteria - check if it matches
        if not self._check_rule_applicability(rule):
            return False

        # For rules WITH applicability criteria, also check parent dependencies
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

        # Register the focus data with DuckDB (skip if None in explain mode)
        if self.focus_data is not None:
            self.conn.register(self.table_name, self.focus_data)
        elif self.explain_mode:
            # In explain mode, create a dummy table with no rows for SQL generation
            self.log.debug("Explain mode: creating empty dummy table")
            self.conn.execute(
                f"CREATE TABLE {self.table_name} AS SELECT 1 AS dummy_col WHERE FALSE"
            )
        else:
            raise ValueError("focus_data cannot be None outside of explain mode")

        if self.pragma_threads:
            self.conn.execute(f"PRAGMA threads={int(self.pragma_threads)}")

        # Log the validation version for reference
        if self.rules_version:
            best_version = self._find_best_version(self.rules_version)
            if best_version == self.rules_version:
                self.log.debug(
                    "FocusToDuckDBSchemaConverter initialized with FOCUS version: %s (exact match)",
                    self.rules_version,
                )
            elif best_version:
                self.log.debug(
                    "FocusToDuckDBSchemaConverter initialized with FOCUS version: %s (using fallback version %s)",
                    self.rules_version,
                    best_version,
                )
            else:
                self.log.debug(
                    "FocusToDuckDBSchemaConverter initialized with FOCUS version: %s (using default mappings)",
                    self.rules_version,
                )
        else:
            self.log.debug(
                "FocusToDuckDBSchemaConverter initialized with default CHECK_GENERATORS (no version specified)"
            )

        # TODO:
        # - register pandas/arrow tables or file paths
        # - create temp schema or set search_path if needed
        # - compile reusable SQL fragments; register any UDFs (from CheckFunctions)

    def finalize(
        self, *, success: bool, results_by_idx: Dict[int, Dict[str, Any]]
    ) -> None:
        """Optional cleanup: drop temps, emit summaries, etc."""
        # e.g., self.conn.execute("DROP VIEW IF EXISTS ...")
        # Close DuckDB connection to prevent hanging in CI environments
        if hasattr(self, "conn") and self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                # Ignore errors during cleanup
                pass
            finally:
                self.conn = None

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

        if rule.is_optional():
            return SkippedOptionalCheck(rule=rule, rule_id=rule_id)

        # Build the actual check object
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

    def run_check(self, check: Any) -> Tuple[bool, Dict[str, Any]]:  # noqa: C901
        """
        Execute a DuckDBColumnCheck (leaf or composite) or a SkippedCheck.
        Ensures details always include: violations:int, message:str.

        NOTE: This method runs checks NORMALLY without any pre-filtering.
        Post-processing (apply_result_overrides) handles non-applicable rules,
        composite aggregation, and dependency skipping.
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

        # Check for special executor on composite (e.g., custom OR logic)
        special = getattr(check, "special_executor", None)

        if callable(special):
            ok, details = special(self.conn)
            details.setdefault("violations", 0 if ok else 1)
            details.setdefault(
                "message",
                getattr(check, "errorMessage", None)
                or f"{getattr(check, 'rule_id', '<rule>')}: composite evaluation",
            )
            details.setdefault(
                "check_type",
                getattr(check, "checkType", None) or getattr(check, "check_type", None),
            )

            return ok, details

        if nested and handler:
            # SIMPLE: Just run all children and aggregate their results
            # Post-processing will handle skipping, non-applicable rules, etc.
            oks: List[bool] = []
            child_details: List[Dict[str, Any]] = []
            composite_rule_id = getattr(check, "rule_id", None)

            for i, child in enumerate(nested):
                ok_i, det_i = self.run_check(child)
                oks.append(ok_i)
                det_i.setdefault("violations", 0 if ok_i else 1)
                det_i.setdefault(
                    "message",
                    _msg_for(
                        child, f"{getattr(child, 'rule_id', '<child>')}: check failed"
                    ),
                )

                # Generate a unique identifier for each child
                child_rule_id = getattr(child, "rule_id", None)
                child_check_type = getattr(child, "checkType", None) or getattr(
                    child, "check_type", None
                )

                # For model_rule_reference checks, use the referenced rule ID
                if child_check_type == "model_rule_reference":
                    # Extract the referenced rule ID from the check object
                    referenced_rule_id = getattr(child, "referenced_rule_id", None)
                    if referenced_rule_id:
                        unique_child_id = referenced_rule_id
                    else:
                        # Fallback to using the check's details if available
                        unique_child_id = (
                            det_i.get("referenced_rule_id")
                            or f"model_rule_reference#{i + 1}"
                        )
                # Check if child has a unique rule_id (different from parent)
                # If child's rule_id matches parent or is missing, create descriptive ID
                elif child_rule_id and child_rule_id != composite_rule_id:
                    # Child has its own unique rule_id
                    unique_child_id = child_rule_id
                elif child_check_type:
                    # Use check type with index for children without unique IDs
                    unique_child_id = f"{child_check_type}#{i + 1}"
                else:
                    # Fallback to generic child identifier
                    unique_child_id = f"child#{i + 1}"

                # Put rule_id AFTER the spread to ensure it overrides any existing rule_id
                child_detail_entry = {**det_i, "rule_id": unique_child_id}
                child_details.append(child_detail_entry)

            # Aggregate the children results normally
            agg_ok = bool(handler(oks))

            # Build descriptive message using the unique child IDs from child_details
            composite_rule_id = getattr(check, "rule_id", "<rule>")
            composite_type = handler.__name__ if handler else "composite"

            # Use the unique IDs from child_details, not from check objects
            failed_child_ids = [
                child_detail["rule_id"]
                for child_detail, ok_i in zip(child_details, oks)
                if not ok_i
            ]
            passed_child_ids = [
                child_detail["rule_id"]
                for child_detail, ok_i in zip(child_details, oks)
                if ok_i
            ]

            if agg_ok:
                if composite_type == "all":  # AND composite
                    fallback_message = f"{composite_rule_id}: AND passed - all child rules succeeded: [{', '.join(passed_child_ids)}]"
                elif composite_type == "any":  # OR composite
                    fallback_message = f"{composite_rule_id}: OR passed - satisfied by rules: [{', '.join(passed_child_ids)}]"
                else:
                    fallback_message = f"{composite_rule_id}: {composite_type} passed"
            else:
                if composite_type == "all":  # AND composite
                    fallback_message = f"{composite_rule_id}: AND failed - failed child rules: [{', '.join(failed_child_ids)}]"
                elif composite_type == "any":  # OR composite
                    fallback_message = f"{composite_rule_id}: OR failed - all child rules failed: [{', '.join(failed_child_ids)}]"
                else:
                    fallback_message = f"{composite_rule_id}: {composite_type} failed - failed children: [{', '.join(failed_child_ids)}]"

            details = {
                "children": child_details,
                "aggregated": handler.__name__,
                "message": _msg_for_outcome(
                    check,
                    agg_ok,
                    fallback_fail=fallback_message,
                    fallback_ok=fallback_message if agg_ok else None,
                ),
                "violations": 0 if agg_ok else 1,
                "check_type": getattr(check, "checkType", None)
                or getattr(check, "check_type", None),
                "failed_child_ids": failed_child_ids,
                "passed_child_ids": passed_child_ids,
            }
            return agg_ok, details

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
        # ---- leaf SQL execution ------------------------------------------------
        sql = getattr(check, "checkSql", None)
        if not sql:
            raise InvalidRuleException(
                f"Leaf check has no SQL to execute (rule_id={getattr(check, 'rule_id', None)})"
            )

        # Handle SQLQuery objects with transpilation support
        sql_query = getattr(check, "_sql_query", None)
        if sql_query and isinstance(sql_query, SQLQuery):
            # Use transpiled SQL if target dialect is specified
            target_dialect = getattr(self, "transpile_dialect", None) or "duckdb"
            sql_to_execute = sql_query.get_requirement_sql(target_dialect)
        else:
            # Legacy string SQL
            sql_to_execute = sql

        sql_final = _sub_table(sql_to_execute)

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
            if hasattr(raw, "item"):
                # numpy scalar types have .item() method to get Python native type
                raw_native = raw.item()
            else:
                raw_native = raw

            # Ensure we have a type that int() can handle
            if isinstance(raw_native, (int, float, str)):
                violations = int(raw_native)
            else:
                # Try to convert whatever type we have
                violations = int(raw_native)  # type: ignore[arg-type]
        except Exception:
            raise RuntimeError(
                f"'violations' is not an integer for {getattr(check, 'rule_id', '<rule>')} (got {type(raw).__name__}: {raw!r}).\nSQL:\n{sql_final}"
            )

        ok = violations == 0

        # Extract error message from SQL result if available
        sql_error_message = None
        if "error_message" in df.columns and not ok:
            sql_error_msg = df.at[0, "error_message"]
            if sql_error_msg is not None and str(sql_error_msg).strip():
                sql_error_message = str(sql_error_msg)

        # Determine final message with preference for SQL-embedded error messages
        if ok:
            # Success case: check for successMessage on generator
            success_msg = getattr(check, "successMessage", None)
            final_message = (
                success_msg
                if isinstance(success_msg, str) and success_msg.strip()
                else None
            )
        else:
            # Failure case: prefer SQL error message, then generator errorMessage, then fallback
            if sql_error_message:
                final_message = sql_error_message
            else:
                generator_error_msg = getattr(check, "errorMessage", None)
                if isinstance(generator_error_msg, str) and generator_error_msg.strip():
                    final_message = generator_error_msg
                else:
                    final_message = (
                        f"{getattr(check, 'rule_id', '<rule>')}: check failed"
                    )

        leaf_details: Dict[str, Any] = {
            "violations": violations,
            "message": final_message,
            "timing_ms": elapsed_ms,
            "check_type": getattr(check, "checkType", None)
            or getattr(check, "check_type", None),
        }

        # Optional: sample rows if provided by the generator and the check failed
        # Only execute sample SQL when --show-violations is enabled for performance
        sample_sql = getattr(check, "sample_sql", None)

        if (not ok) and sample_sql and self.show_violations:
            try:
                sql_sample = (
                    _sub_table(sample_sql) + f" LIMIT {self.DEFAULT_SAMPLE_LIMIT}"
                )
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
        # For composite rules (AND/OR), we need to extend parent_edges to include the composite itself
        # so that children can inherit the composite's condition
        is_composite = check_fn in ("AND", "OR")
        child_parent_edges = parent_edges or ()

        if is_composite:
            # Find the node index for this composite rule in the plan so children can reference it
            composite_node_idx = None
            if self.plan:
                for idx, node in enumerate(self.plan.nodes):
                    if node.rule_id == rule_id:
                        composite_node_idx = idx
                        break

            # Extend parent_edges to include this composite's rule_id
            # The _parent_rules_from_edges method can handle rule_id strings directly
            if composite_node_idx is not None:
                # Add the composite's rule_id to parent_edges so children can find it
                child_parent_edges = tuple(list(parent_edges or ()) + [rule_id])

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
                parent_edges=child_parent_edges,
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

        # Build effective condition from parent_edges AND from downstream composite consumers
        eff_cond = self._build_effective_condition(rule, parent_edges)

        # ENHANCEMENT: Also check if this rule is referenced by composite rules with conditions
        # This handles the case where a rule like PricingQuantity-C-008-M is referenced by
        # a composite like PricingQuantity-C-007-C that has a condition.
        if self.plan and hasattr(self.plan, "plan_graph"):
            graph = self.plan.plan_graph
            # Find composite rules that reference this rule
            downstream_composites = graph.children.get(rule_id, set())

            for composite_rid in downstream_composites:
                # Get the composite rule object
                composite_node = graph.nodes.get(composite_rid)

                if composite_node and composite_node.rule:
                    composite_rule = composite_node.rule
                    composite_function = getattr(composite_rule, "function", None)

                    # Check if it's a composite with a condition
                    if composite_function == "Composite":
                        composite_cond_spec = self._extract_condition_spec(
                            composite_rule
                        )

                        if composite_cond_spec:
                            composite_cond_sql = (
                                self._compile_condition_with_generators(
                                    composite_cond_spec,
                                    rule=composite_rule,
                                    rule_id=composite_rid,
                                    breadcrumb=f"{composite_rid}_condition",
                                )
                            )

                            if composite_cond_sql:
                                # Combine with existing condition
                                if eff_cond:
                                    eff_cond = (
                                        f"({eff_cond}) AND ({composite_cond_sql})"
                                    )
                                else:
                                    eff_cond = composite_cond_sql

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

        # Get the rule_id for potential debugging
        current_rule_id = (
            getattr(rule, "rule_id", None) or getattr(rule, "RuleId", None) or "<rule>"
        )

        me = self._extract_condition_spec(rule)
        me_sql = self._compile_condition_with_generators(
            me,
            rule=rule,
            rule_id=current_rule_id,
            breadcrumb="Condition",
        )
        if me_sql:
            parts.append(f"({me_sql})")

        for prule in self._parent_rules_from_edges(parent_edges):
            if prule is None:
                continue
            parent_rule_id = (
                getattr(prule, "rule_id", None)
                or getattr(prule, "RuleId", None)
                or "<parent>"
            )

            pspec = self._extract_condition_spec(prule)
            psql = self._compile_condition_with_generators(
                pspec,
                rule=prule,
                rule_id=parent_rule_id,
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

        # Get MustSatisfy from the rule object
        rule = getattr(check, "rule", None)
        must_satisfy = None
        if (
            rule
            and hasattr(rule, "validation_criteria")
            and hasattr(rule.validation_criteria, "must_satisfy")
        ):
            must_satisfy = rule.validation_criteria.must_satisfy

        # Skipped / dynamic
        check_type_attr = getattr(check, "checkType", "")
        check_type_method = getattr(check, "getCheckType", lambda: "")()

        if (
            check_type_attr == "skipped_check"
            or check_type_method == "skipped_check"
            or hasattr(check, "is_skip")
            and check.is_skip
        ):
            # Check if this is a dynamic rule specifically
            reason = (
                getattr(check, "reason", None)
                or getattr(check, "errorMessage", None)
                or "skipped rule"
            )
            generator_name = meta.get("generator")

            # Check the entity type of the rule to determine if it's dynamic
            rule_entity_type = getattr(
                getattr(check, "rule", None), "entity_type", None
            )

            # For different types of skipped rules, provide appropriate generator names
            if rule_entity_type == "Dynamic":
                generator_name = "None due to dynamic rule"
            elif reason == "dynamic rule" or isinstance(check, SkippedDynamicCheck):
                generator_name = "None due to dynamic rule"
            elif reason == "non applicable rule" or isinstance(
                check, SkippedNonApplicableCheck
            ):
                generator_name = "None due to non-applicable rule"
            elif reason in [
                "no defined check rule",
                "FormatUnit rule is dynamic",
                "FormatUnit check is dynamic",
            ]:
                generator_name = "None due to dynamic rule"
            elif generator_name is None:
                # For other skipped checks, use the class name or a generic message
                generator_name = (
                    getattr(check, "__class__", type(check)).__name__
                    if hasattr(check, "__class__")
                    else "None"
                )

            return {
                "rule_id": rid,
                "type": "skipped",
                "check_type": ctype,
                "reason": reason,
                "sql": None,
                "row_condition_sql": None,
                "generator": generator_name,
                "must_satisfy": must_satisfy,
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
                "must_satisfy": must_satisfy,
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
                "must_satisfy": must_satisfy,
            }

        # Leaf with SQL
        sql = getattr(check, "checkSql", None) or getattr(check, "check_sql", None)

        # Handle SQLQuery objects with transpilation support
        sql_query = getattr(check, "_sql_query", None)
        transpilation_info = {}

        if sql_query and isinstance(sql_query, SQLQuery):
            # Get SQL for current dialect
            target_dialect = getattr(self, "transpile_dialect", None) or "duckdb"
            sql_to_show = sql_query.get_requirement_sql(target_dialect)

            # Add transpilation information for explain mode
            transpilation_info = {
                "original_dialect": "duckdb",
                "target_dialect": target_dialect,
                "predicate_sql": sql_query.get_predicate_sql(target_dialect),
                "supports_transpilation": True,
                "parsing_error": sql_query.parsing_error,
            }

            # Show transpilation examples if not already in target dialect
            if target_dialect.lower() != "duckdb":
                transpilation_info["duckdb_sql"] = self._subst_table(
                    sql_query.get_requirement_sql("duckdb")
                )
        else:
            # Legacy string SQL
            sql_to_show = sql
            transpilation_info = {
                "supports_transpilation": False,
                "note": "Legacy string SQL - SQLQuery migration needed for transpilation",
            }

        # Check if this is a Dynamic entity rule that ended up in the leaf branch
        rule_entity_type = getattr(getattr(check, "rule", None), "entity_type", None)
        generator_name = meta.get("generator")

        if rule_entity_type == "Dynamic" and generator_name is None:
            generator_name = "None due to dynamic rule"

        result = {
            "rule_id": rid,
            "type": "leaf",
            "check_type": ctype,
            "generator": generator_name,
            "row_condition_sql": meta.get("row_condition_sql"),
            "sql": self._subst_table(sql_to_show) if sql_to_show else None,
            "message": getattr(check, "errorMessage", None),
            "must_satisfy": must_satisfy,
        }

        # Add transpilation info
        result.update(transpilation_info)

        return result

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

    def apply_result_overrides(self, results_by_idx: Dict[int, Dict[str, Any]]) -> None:
        """
        POST-PROCESSING: Apply all result overrides after checks have run.

        This is the single location where we handle:
        1. Non-applicable rules: Skip rules that don't meet applicability criteria
        2. Composite aggregation: Update composites based on child results
        3. Dependency skips: Skip rules whose dependencies failed/skipped

        This runs AFTER all checks have executed normally, making the logic
        simple, clear, and maintainable.
        """
        if not self.plan:
            return

        # Phase 1: Mark non-applicable rules and their descendants as skipped
        non_applicable_rule_ids = self._apply_non_applicable_skips(results_by_idx)

        # Phase 2: Propagate skipped dependencies BEFORE composite aggregation
        # This ensures composites see correct child skip states
        self._apply_dependency_skips(results_by_idx)

        # Phase 3: Update composite results based on child results
        # This must run AFTER dependency skips so it sees the final child states
        self._apply_composite_aggregation(results_by_idx)

        # Phase 4: Apply non-applicable marking to nested children
        # This must run AFTER composite aggregation so synced skip states are preserved
        self._apply_nested_child_non_applicable_marking(
            results_by_idx, non_applicable_rule_ids
        )

    def _apply_non_applicable_skips(
        self, results_by_idx: Dict[int, Dict[str, Any]]
    ) -> tuple[Set[str], Set[str]]:
        """Mark non-applicable rules and all their descendants as skipped.

        This handles both separate plan nodes AND nested children within composites.
        Also handles column-family rules: when a column presence check is non-applicable,
        ALL rules for that column are marked non-applicable.

        Returns:
            Tuple of (non_applicable_nested_rule_ids, non_applicable_column_prefixes)
            for use in Phase 4.
        """
        if not self.plan:
            return (set(), set())

        # Identify all non-applicable rules (both top-level nodes and nested children)
        non_applicable_rules: Set[int] = set()
        non_applicable_nested_rule_ids: Set[str] = set()
        non_applicable_column_prefixes: Set[str] = set()

        for idx, node in enumerate(self.plan.nodes):
            if not node or not hasattr(node, "rule"):
                continue

            rule = node.rule
            parent_edges = node.parent_edges if hasattr(node, "parent_edges") else ()

            # Check if this rule should be included
            if not self._should_include_rule(rule, parent_edges):
                non_applicable_rules.add(idx)
                rule_id = getattr(rule, "rule_id", None)
                if rule_id:
                    non_applicable_nested_rule_ids.add(rule_id)
                    # Extract column prefix ONLY for Presence checks with EntityType="Dataset"
                    # that reference a COLUMN (not dataset rules starting with CostAndUsage-D-)
                    # This ensures all rules for a non-applicable column presence check are skipped
                    rule_function = getattr(rule, "function", None)
                    rule_entity_type = getattr(rule, "entity_type", None)
                    if (
                        rule_function == "Presence"
                        and rule_entity_type == "Dataset"
                        and "-" in rule_id
                    ):
                        # Dataset presence rules for columns are like "CostAndUsage-D-NNN-X"
                        # We need to look at the "Reference" field to get the actual column name
                        column_name = getattr(rule, "reference", None)
                        if column_name and column_name != "CostAndUsage":
                            non_applicable_column_prefixes.add(column_name)

        # Collect all descendants of non-applicable rules
        rules_to_skip = self._collect_all_descendants(non_applicable_rules)

        # Also add all rules that share a column prefix with non-applicable rules
        for idx, node in enumerate(self.plan.nodes):
            if idx not in rules_to_skip:  # Don't re-process already marked rules
                rule_id = (
                    getattr(node.rule, "rule_id", None)
                    if hasattr(node, "rule")
                    else None
                )
                if rule_id and "-" in rule_id:
                    column_prefix = rule_id.split("-")[0]
                    if column_prefix in non_applicable_column_prefixes:
                        rules_to_skip.add(idx)
                        if rule_id:
                            non_applicable_nested_rule_ids.add(rule_id)

        # Mark them all as skipped
        for idx in rules_to_skip:
            if idx in results_by_idx:
                result = results_by_idx[idx]
                details = result.get("details", {})
                rule_id = result.get("rule_id", "")

                # Only update skip reason if not already skipped for another reason
                # This preserves more specific skip reasons like "dynamic rule" or "optional rule"
                if not details.get("skipped", False):
                    # Update result to skipped
                    result["ok"] = True
                    details["skipped"] = True
                    details["reason"] = "rule not applicable"
                    details["message"] = (
                        "Rule skipped - not applicable to current dataset or configuration"
                    )
                    details["violations"] = 0

                # Mark nested children as skipped if composite
                # This handles children that are part of the composite's nestedChecks
                if "children" in details:
                    for child in details["children"]:
                        # Only update if not already skipped
                        if not child.get("skipped", False):
                            child["ok"] = True
                            child["skipped"] = True
                            child["reason"] = "rule not applicable"
                            child["violations"] = 0
                            child_rule_id = child.get("rule_id", "<child>")
                            child["message"] = f"{child_rule_id}: rule not applicable"

        # Return the sets for use in Phase 4
        return (non_applicable_nested_rule_ids, non_applicable_column_prefixes)

    def _apply_nested_child_non_applicable_marking(
        self,
        results_by_idx: Dict[int, Dict[str, Any]],
        non_applicable_rule_ids: tuple[Set[str], Set[str]],
    ) -> None:
        """Mark nested children as non-applicable AFTER composite aggregation.

        This phase runs after composite aggregation so that any skip states
        synced from child execution results are preserved. Only marks children
        as "not applicable" if they aren't already skipped for another reason
        (like being dynamic).

        Args:
            results_by_idx: Results dictionary
            non_applicable_rule_ids: Tuple of (rule_ids, column_prefixes) from Phase 1
        """
        # Unpack the sets from Phase 1
        non_applicable_nested_rule_ids, non_applicable_column_prefixes = (
            non_applicable_rule_ids
        )

        # Mark any nested children that are non-applicable
        # These are children embedded in composites, not separate plan nodes
        for idx in results_by_idx:
            result = results_by_idx[idx]
            details = result.get("details", {})

            if "children" in details:
                for child in details["children"]:
                    child_rule_id = child.get("rule_id")
                    if child_rule_id:
                        # Check if child rule ID matches non-applicable rule
                        if child_rule_id in non_applicable_nested_rule_ids:
                            # Only update if not already skipped
                            if not child.get("skipped", False):
                                child["ok"] = True
                                child["skipped"] = True
                                child["reason"] = "rule not applicable"
                                child["violations"] = 0
                                child["message"] = (
                                    f"{child_rule_id}: rule not applicable"
                                )
                        # Also check if child rule shares column prefix with non-applicable column
                        elif "-" in child_rule_id:
                            child_column_prefix = child_rule_id.split("-")[0]
                            if child_column_prefix in non_applicable_column_prefixes:
                                # Only update if not already skipped
                                if not child.get("skipped", False):
                                    child["ok"] = True
                                    child["skipped"] = True
                                    child["reason"] = "rule not applicable"
                                    child["violations"] = 0
                                    child["message"] = (
                                        f"{child_rule_id}: rule not applicable"
                                    )

    def _apply_composite_aggregation(
        self, results_by_idx: Dict[int, Dict[str, Any]]
    ) -> None:
        """Update composite results based on actual child results.

        This must run AFTER dependency skips so it sees the final child states.
        """
        if not self.plan:
            return

        for idx, node in enumerate(self.plan.nodes):
            if idx not in results_by_idx:
                continue

            result = results_by_idx[idx]
            details = result.get("details", {})

            # Only process composites with children
            if "children" not in details or "aggregated" not in details:
                continue

            children = details["children"]
            aggregator = details["aggregated"]

            # IMPORTANT: Update children with their current states from results_by_idx
            # The children array was populated during initial execution, but child rules
            # may have been updated by earlier post-processing phases (non-applicable, dependencies)
            # We need to sync the child states so aggregation sees the latest data
            if self.plan and hasattr(node, "rule"):
                rule = node.rule
                # Get the dependencies list from the rule's validation criteria
                dependencies = []
                vc = getattr(rule, "validation_criteria", None)
                if vc and hasattr(vc, "dependencies"):
                    dependencies = list(vc.dependencies or [])
                elif isinstance(vc, dict):
                    dependencies = list(vc.get("dependencies") or [])

                # Dependencies list often includes a Dataset presence check as the first entry
                # (e.g., "CostAndUsage-D-010-M") which is NOT part of the composite's nested children
                # Skip Dataset dependencies (those starting with "CostAndUsage-D-" or other dataset prefixes)
                child_dependencies = [
                    dep
                    for dep in dependencies
                    if not dep.endswith("-D-") and "-D-" not in dep
                ]

                # Match children to their dependency rules - but only if lengths match
                if child_dependencies and len(child_dependencies) == len(children):
                    for i, dep_rule_id in enumerate(child_dependencies):
                        child = children[i]

                        # Find the result for this dependency rule
                        for dep_idx, dep_node in enumerate(self.plan.nodes):
                            if dep_idx in results_by_idx:
                                dep_node_rule_id = (
                                    getattr(dep_node.rule, "rule_id", None)
                                    if hasattr(dep_node, "rule")
                                    else None
                                )

                                if dep_node_rule_id == dep_rule_id:
                                    # Found the node for this dependency - update child state
                                    dep_result = results_by_idx[dep_idx]
                                    dep_details = dep_result.get("details", {})

                                    # Sync the key fields - prioritize skipped status from details
                                    child["ok"] = dep_result.get("ok", False)
                                    # Check both top-level and details for skipped status
                                    child["skipped"] = dep_details.get(
                                        "skipped", False
                                    ) or dep_result.get("skipped", False)
                                    child["violations"] = dep_details.get(
                                        "violations", 0
                                    )
                                    if "reason" in dep_details:
                                        child["reason"] = dep_details["reason"]
                                    # Also check for message to carry forward
                                    if "message" in dep_details:
                                        child["message"] = dep_details["message"]

                                    break
                else:
                    # If we can't match by dependencies, try to match by rule_id directly
                    for child in children:
                        child_rule_id = child.get("rule_id")
                        if child_rule_id:
                            # Search for this rule_id in results_by_idx
                            for dep_idx, dep_node in enumerate(self.plan.nodes):
                                if dep_idx in results_by_idx:
                                    dep_node_rule_id = (
                                        getattr(dep_node.rule, "rule_id", None)
                                        if hasattr(dep_node, "rule")
                                        else None
                                    )

                                    if dep_node_rule_id == child_rule_id:
                                        # Found the node - update child state
                                        dep_result = results_by_idx[dep_idx]
                                        dep_details = dep_result.get("details", {})

                                        # Sync the key fields
                                        child["ok"] = dep_result.get("ok", False)
                                        child["skipped"] = dep_details.get(
                                            "skipped", False
                                        ) or dep_result.get("skipped", False)
                                        child["violations"] = dep_details.get(
                                            "violations", 0
                                        )
                                        if "reason" in dep_details:
                                            child["reason"] = dep_details["reason"]
                                        if "message" in dep_details:
                                            child["message"] = dep_details["message"]
                                        break

            # Check if ALL children were skipped
            all_children_skipped = children and all(
                child.get("skipped", False) for child in children
            )

            if all_children_skipped:
                # If ALL children skipped, mark composite as skipped too
                result["ok"] = True
                details["skipped"] = True
                details["reason"] = "Rule skipped - all child rules were skipped"
                details["message"] = "Rule skipped - all child rules were skipped"
                details["violations"] = 0
                continue

            # Normal aggregation: only consider non-skipped children
            # Skipped children should not affect the composite result
            non_skipped_children = [
                child for child in children if not child.get("skipped", False)
            ]

            # If there are no non-skipped children, this should have been caught above
            # But as a safety check, if all were skipped, mark as skipped
            if not non_skipped_children:
                result["ok"] = True
                details["skipped"] = True
                details["reason"] = "Rule skipped - all child rules were skipped"
                details["message"] = "Rule skipped - all child rules were skipped"
                details["violations"] = 0
                continue

            # Aggregate only non-skipped children
            child_oks = [child.get("ok", False) for child in non_skipped_children]

            if aggregator == "all":
                # AND: all non-skipped children must pass
                composite_ok = all(child_oks)
            elif aggregator == "any":
                # OR: at least one non-skipped child must pass
                composite_ok = any(child_oks)
            else:
                # Unknown aggregator, keep current result
                continue

            # Update composite result
            result["ok"] = composite_ok

            # For violations, use the actual violation count from children
            # For OR composites, we want the violation count from the composite's own SQL execution
            # which represents rows that don't match ANY of the allowed values
            # The initial execution already set this correctly, so only update if we're changing pass/fail status
            if "violations" in details:
                # Keep the original violation count from execution
                # Only force to 0 if composite is now passing
                if composite_ok:
                    details["violations"] = 0
                # If failing, keep the original violation count from the composite's SQL execution
            else:
                # Fallback if violations wasn't set (shouldn't happen)
                details["violations"] = 0 if composite_ok else 1

            # Build descriptive message using CHILD DETAILS not rule IDs from check objects
            rule_id = result.get("rule_id", "<composite>")

            # Collect child rule IDs from non-skipped children only
            failed_children = []
            passed_children = []
            skipped_children = []

            for child in children:
                child_rule_id = child.get("rule_id", "<child>")
                child_skipped = child.get("skipped", False)
                child_ok = child.get("ok", False)

                if child_skipped:
                    skipped_children.append(child_rule_id)
                elif child_ok:
                    passed_children.append(child_rule_id)
                else:
                    failed_children.append(child_rule_id)

            if composite_ok:
                if aggregator == "all":
                    details["message"] = (
                        f"{rule_id}: AND passed - all child rules succeeded: "
                        f"[{', '.join(passed_children)}]"
                    )
                else:  # any
                    details["message"] = (
                        f"{rule_id}: OR passed - satisfied by rules: "
                        f"[{', '.join(passed_children)}]"
                    )
            else:
                if aggregator == "all":
                    details["message"] = (
                        f"{rule_id}: AND failed - failed child rules: "
                        f"[{', '.join(failed_children)}]"
                    )
                else:  # any
                    details["message"] = (
                        f"{rule_id}: OR failed - all child rules failed: "
                        f"[{', '.join(failed_children)}]"
                    )

    def _apply_dependency_skips(
        self, results_by_idx: Dict[int, Dict[str, Any]]
    ) -> None:
        """Skip rules whose dependencies were skipped or failed.

        Also handles retroactive skipping: when a composite is skipped due to failed
        dependencies, its child rules (referenced via CheckModelRule) are also marked
        as skipped in results_by_idx so they appear correctly in the final output.
        """
        if not self.plan:
            return

        # Build dependency map
        for idx, node in enumerate(self.plan.nodes):
            if idx not in results_by_idx:
                continue

            result = results_by_idx[idx]
            details = result.get("details", {})

            # Check if this rule has dependencies
            parent_idxs = node.parent_idxs if hasattr(node, "parent_idxs") else []

            rule_id = result.get("rule_id", "")

            # Check if any parent was skipped or failed (for column presence checks)
            skipped_parents = []
            failed_presence_checks = []

            for parent_idx in parent_idxs:
                if parent_idx in results_by_idx:
                    parent_result = results_by_idx[parent_idx]
                    parent_details = parent_result.get("details", {})
                    parent_node = self.plan.nodes[parent_idx]
                    parent_rule = (
                        parent_node.rule if hasattr(parent_node, "rule") else None
                    )
                    parent_rule_id = parent_result.get("rule_id", f"idx_{parent_idx}")

                    # Skip if parent was skipped
                    if parent_details.get("skipped", False):
                        skipped_parents.append(parent_rule_id)

                    # Also skip if parent is a column presence check that failed
                    # Column presence checks are critical dependencies - if they fail, dependent rules can't run
                    elif not parent_result.get("ok", True):
                        # Check if this is a column presence check
                        parent_function = (
                            getattr(parent_rule, "function", None)
                            if parent_rule
                            else None
                        )
                        parent_check_type = parent_details.get("check_type")

                        if (
                            parent_function == "Presence"
                            or parent_check_type == "column_presence"
                        ):
                            failed_presence_checks.append(parent_rule_id)

            rule_function = (
                getattr(node.rule, "function", None) if hasattr(node, "rule") else None
            )
            is_composite = rule_function == "Composite"

            # For composite rules, check if ALL children are skipped before skipping the composite
            # A composite with some passing and some skipped children should aggregate normally
            if is_composite and skipped_parents:
                # Check if ALL children in the composite are skipped
                all_children_skipped = True
                if "children" in details:
                    for child in details["children"]:
                        if not child.get("skipped", False):
                            all_children_skipped = False
                            break

                # Only skip the composite if all children are skipped
                if all_children_skipped:
                    result["ok"] = True
                    details["skipped"] = True
                    details["reason"] = "upstream dependency was skipped"
                    details["message"] = (
                        f"Rule skipped - dependent rule(s) were skipped: "
                        f"{', '.join(skipped_parents)}"
                    )
                    details["violations"] = 0
                    details["skipped_dependencies"] = skipped_parents
                else:
                    # Some children passed, so don't skip the composite
                    # Just continue with normal processing
                    pass
            # For non-composite rules, skip if parent was skipped
            elif skipped_parents:
                # Skip this rule because an upstream dependency was skipped
                result["ok"] = True
                details["skipped"] = True
                details["reason"] = "upstream dependency was skipped"
                details["message"] = (
                    f"Rule skipped - dependent rule(s) were skipped: "
                    f"{', '.join(skipped_parents)}"
                )
                details["violations"] = 0
                details["skipped_dependencies"] = skipped_parents

            # If the rule has failed presence checks, annotate the failure message
            # (but don't skip - let it fail with context)
            elif failed_presence_checks and not result.get("ok", True):
                current_message = details.get("message", "")
                dependency_context = f" [Upstream dependency failed: {', '.join(failed_presence_checks)} - required column not present]"

                if current_message:
                    details["message"] = current_message + dependency_context
                else:
                    details["message"] = f"Rule execution failed{dependency_context}"

                # For composite rules, also annotate their children
                if is_composite and "children" in details:
                    for child in details["children"]:
                        child_rule_id = child.get("rule_id")
                        if child_rule_id:
                            # Find this child rule in results_by_idx and annotate it
                            for child_idx, child_node in enumerate(self.plan.nodes):
                                if child_idx in results_by_idx:
                                    child_node_rule_id = (
                                        getattr(child_node.rule, "rule_id", None)
                                        if hasattr(child_node, "rule")
                                        else None
                                    )
                                    if child_node_rule_id == child_rule_id:
                                        child_result = results_by_idx[child_idx]
                                        child_details = child_result.get("details", {})

                                        # Only annotate if child also failed
                                        if not child_result.get("ok", True):
                                            child_current_message = child_details.get(
                                                "message", ""
                                            )
                                            child_context = f" [Upstream dependency failed: {', '.join(failed_presence_checks)} - required column not present]"

                                            if child_current_message:
                                                child_details["message"] = (
                                                    child_current_message
                                                    + child_context
                                                )
                                            else:
                                                child_details["message"] = (
                                                    f"Rule execution failed{child_context}"
                                                )
                                        break

        # Second pass: Find rules that failed but are children of composites with failed dependencies
        # (Handle "grandparent" dependency failures where child -> composite -> failed dependency)
        for idx, node in enumerate(self.plan.nodes):
            if idx not in results_by_idx:
                continue

            result = results_by_idx[idx]
            details = result.get("details", {})

            # Only process rules that failed (not skipped, not passed)
            if result.get("ok", True) or details.get("skipped", False):
                continue

            # Check if this rule's message already has upstream dependency context
            current_message = details.get("message", "")
            if "[Upstream dependency failed:" in current_message:
                continue  # Already annotated

            rule_id = result.get("rule_id", "")

            # Find composites that reference this rule as a child
            for comp_idx, comp_node in enumerate(self.plan.nodes):
                if comp_idx not in results_by_idx:
                    continue

                comp_rule = comp_node.rule if hasattr(comp_node, "rule") else None
                comp_function = (
                    getattr(comp_rule, "function", None) if comp_rule else None
                )

                if comp_function != "Composite":
                    continue

                comp_details = results_by_idx[comp_idx].get("details", {})

                # Check if this composite references our rule as a child
                children = comp_details.get("children", [])
                is_child_of_composite = any(
                    c.get("rule_id") == rule_id for c in children
                )

                if not is_child_of_composite:
                    continue

                # Check if the composite has failed dependencies (grandparent failures)
                comp_parent_idxs = (
                    comp_node.parent_idxs if hasattr(comp_node, "parent_idxs") else []
                )
                grandparent_failed_presence = []

                for gp_idx in comp_parent_idxs:
                    if gp_idx not in results_by_idx:
                        continue

                    gp_result = results_by_idx[gp_idx]
                    gp_details = gp_result.get("details", {})
                    gp_node = self.plan.nodes[gp_idx]
                    gp_rule = gp_node.rule if hasattr(gp_node, "rule") else None
                    gp_rule_id = gp_result.get("rule_id", f"idx_{gp_idx}")

                    # Check if grandparent failed
                    if not gp_result.get("ok", True):
                        gp_function = (
                            getattr(gp_rule, "function", None) if gp_rule else None
                        )
                        gp_check_type = gp_details.get("check_type")

                        # Skip if this is the same rule (avoid circular reference in message)
                        if gp_rule_id == rule_id:
                            continue

                        # Include any failed dependency, but note if it's a column presence check
                        if (
                            gp_function == "Presence"
                            or gp_check_type == "column_presence"
                        ):
                            grandparent_failed_presence.append(
                                f"{gp_rule_id} (missing column)"
                            )
                        else:
                            # For other failures (like composites), just include the rule ID
                            grandparent_failed_presence.append(gp_rule_id)

                # If we found failed dependencies in grandparents, annotate this rule
                if grandparent_failed_presence:
                    dependency_context = f" [Upstream dependency failed: {', '.join(grandparent_failed_presence)}]"

                    if current_message:
                        details["message"] = current_message + dependency_context
                    else:
                        details["message"] = (
                            f"Rule execution failed{dependency_context}"
                        )

                    break  # Found the context, no need to check other composites

    def _collect_all_descendants(self, rule_indices: Set[int]) -> Set[int]:
        """Recursively collect all descendants of the given rules."""
        if not self.plan:
            return set()

        descendants = set(rule_indices)
        to_process = list(rule_indices)

        while to_process:
            current_idx = to_process.pop()

            # Find all nodes that depend on this one
            for idx, node in enumerate(self.plan.nodes):
                if not hasattr(node, "parent_idxs"):
                    continue

                if current_idx in node.parent_idxs:
                    if idx not in descendants:
                        descendants.add(idx)
                        to_process.append(idx)

            # Also check nested children in composite checks
            if current_idx < len(self.plan.nodes):
                node = self.plan.nodes[current_idx]
                if hasattr(node, "rule") and hasattr(node.rule, "validation_criteria"):
                    vc = node.rule.validation_criteria
                    if hasattr(vc, "requirement") and isinstance(vc.requirement, dict):
                        # Items would be child requirements in composites
                        # These are already handled as separate nodes with dependencies
                        pass

        return descendants

    def update_global_results(
        self, node_idx: int, ok: bool, details: Dict[str, Any]
    ) -> None:
        """Update the global results registry for dependency propagation."""
        self._global_results_by_idx[node_idx] = {
            "ok": ok,
            "details": details,
            "rule_id": (
                self.plan.nodes[node_idx].rule_id
                if self.plan and node_idx < len(self.plan.nodes)
                else None
            ),
        }
