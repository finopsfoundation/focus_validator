"""Edge-case coverage for DuckDB check generators.

Unlike ``test_focus_to_duckdb_generators.py`` (which mocks ``duckdb`` and
asserts on generated SQL strings), this module executes the generated SQL
against a real in-memory DuckDB so the behavioural edge cases the happy-path
tests miss are actually exercised:

  * ``CheckColumnComparison`` with one or both columns NULL (null handling)
  * ``CheckStringEndsWith`` with multi-byte suffixes
  * ``CheckJSONSchema`` with malformed paths and with ``jsonschema`` missing
  * ``CheckNoDuplicates`` combined with a row-filter condition
"""

import builtins
import sys
import unittest
from unittest.mock import Mock, patch

# A sibling test module (test_focus_to_duckdb_generators.py) replaces ``duckdb``
# in sys.modules with a MagicMock at import time. These tests execute SQL, so we
# need the real engine: drop any installed mock and import the genuine package.
# Binding it to this module's global means later sys.modules changes can't affect
# us, regardless of test collection order.
sys.modules.pop("duckdb", None)
import duckdb  # noqa: E402

from focus_validator.config_objects.focus_to_duckdb_converter import (  # noqa: E402
    CheckColumnComparisonGenerator,
    CheckJSONSchemaGenerator,
    CheckNoDuplicatesGenerator,
    CheckStringEndsWithGenerator,
)
from focus_validator.config_objects.rule import ModelRule  # noqa: E402
from focus_validator.exceptions import InvalidRuleException  # noqa: E402


def _rule(rule_id="EDGE-CASE"):
    """A ModelRule mock.

    ``Mock(spec=ModelRule)`` does not expose pydantic fields, so
    ``_get_validation_keyword`` falls back to its "MUST" default. That keeps the
    generated error-message SQL literal free of Mock reprs, which matters
    because we execute the SQL here.
    """
    rule = Mock(spec=ModelRule)
    rule.rule_id = rule_id
    return rule


def _violations(conn, generator, table="focus_data"):
    """Execute a generator's requirement SQL and return the violation count."""
    requirement_sql = generator.generateSql().requirement_sql
    sql = requirement_sql.replace("{table_name}", table)
    return conn.execute(sql).fetchone()[0]


class TestCheckColumnComparisonNullHandling(unittest.TestCase):
    """CheckColumnComparison must skip rows where either column is NULL."""

    def test_null_rows_are_not_flagged(self):
        conn = duckdb.connect()
        conn.execute("CREATE TABLE focus_data (EffectiveCost DOUBLE, BilledCost DOUBLE)")
        conn.executemany(
            "INSERT INTO focus_data VALUES (?, ?)",
            [
                (10.0, 10.0),  # equal, both present -> passes
                (10.0, 20.0),  # not equal, both present -> the only violation
                (None, 10.0),  # A null -> skipped
                (10.0, None),  # B null -> skipped
                (None, None),  # both null -> skipped
            ],
        )

        generator = CheckColumnComparisonGenerator(
            rule=_rule("CMP-001"),
            rule_id="CMP-001",
            ColumnAName="EffectiveCost",
            ColumnBName="BilledCost",
            Comparator="=",
        )

        # Only the (10, 20) row violates. With the old
        # ``NOT (A IS NOT NULL AND B IS NOT NULL AND A = B)`` form the three
        # NULL rows would also be flagged (4 violations).
        self.assertEqual(_violations(conn, generator), 1)

    def test_null_handling_matches_check_same_value_convention(self):
        """Both generators skip NULL rows, so they agree on the same data."""
        conn = duckdb.connect()
        conn.execute("CREATE TABLE focus_data (ColA VARCHAR, ColB VARCHAR)")
        conn.executemany(
            "INSERT INTO focus_data VALUES (?, ?)",
            [("x", "x"), ("x", "y"), (None, "y"), ("x", None), (None, None)],
        )

        comparison = CheckColumnComparisonGenerator(
            rule=_rule(),
            rule_id="CMP-002",
            ColumnAName="ColA",
            ColumnBName="ColB",
            Comparator="=",
        )
        self.assertEqual(_violations(conn, comparison), 1)


class TestCheckStringEndsWithMultiByte(unittest.TestCase):
    """ends_with must handle multi-byte (non-ASCII) suffixes correctly."""

    def setUp(self):
        self.conn = duckdb.connect()
        self.conn.execute("CREATE TABLE focus_data (Name VARCHAR)")
        self.conn.executemany(
            "INSERT INTO focus_data VALUES (?)",
            [("café",), ("nope",), ("日本語",), (None,)],
        )

    def test_accented_suffix(self):
        generator = CheckStringEndsWithGenerator(
            rule=_rule("EW-1"), rule_id="EW-1", ColumnName="Name", Value="é"
        )
        # "café" ends with "é"; "nope" and "日本語" do not; NULL is skipped.
        self.assertEqual(_violations(self.conn, generator), 2)

    def test_cjk_suffix(self):
        generator = CheckStringEndsWithGenerator(
            rule=_rule("EW-2"), rule_id="EW-2", ColumnName="Name", Value="語"
        )
        # "日本語" ends with "語"; "café" and "nope" do not; NULL is skipped.
        self.assertEqual(_violations(self.conn, generator), 2)

    def test_multibyte_suffix_appears_in_sql(self):
        generator = CheckStringEndsWithGenerator(
            rule=_rule("EW-3"), rule_id="EW-3", ColumnName="Name", Value="café"
        )
        sql = generator.generateSql().requirement_sql
        self.assertIn("NOT ends_with(CAST(Name AS VARCHAR), 'café')", sql)


class TestCheckJSONSchemaEdgeCases(unittest.TestCase):
    """Malformed JSON paths and a missing jsonschema dependency."""

    def _generator(self):
        return CheckJSONSchemaGenerator(
            rule=_rule("JS-1"),
            rule_id="JS-1",
            ColumnName="Payload",
            SchemaId="TEST-SCHEMA",
            Path="$",
            schemas={"TEST-SCHEMA": {"Schema": {"type": "object"}}},
        )

    def test_path_without_dollar_prefix_raises(self):
        generator = self._generator()
        with self.assertRaises(InvalidRuleException):
            generator._extract_path_value({"a": 1}, "no-dollar")

    def test_malformed_segment_raises(self):
        generator = self._generator()
        # A segment must start with a letter or underscore.
        with self.assertRaises(InvalidRuleException):
            generator._extract_path_value({"1bad": 1}, "$.1bad")

    def test_valid_paths_still_resolve(self):
        generator = self._generator()
        payload = {"a": {"b": 5}, "items": [{"x": 1}, {"x": 2}]}
        self.assertEqual(generator._extract_path_value(payload, "$"), payload)
        self.assertEqual(generator._extract_path_value(payload, "$.a.b"), 5)
        self.assertEqual(generator._extract_path_value(payload, "$.items[1]"), {"x": 2})
        # Missing keys resolve to None rather than raising.
        self.assertIsNone(generator._extract_path_value(payload, "$.a.missing"))

    def test_missing_jsonschema_dependency_raises_runtime_error(self):
        generator = self._generator()
        check = generator.generateCheck()

        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "jsonschema":
                raise ModuleNotFoundError("No module named 'jsonschema'")
            return real_import(name, *args, **kwargs)

        conn = duckdb.connect()
        with patch("builtins.__import__", side_effect=fake_import):
            with self.assertRaises(RuntimeError) as ctx:
                check.special_executor(conn)

        self.assertIn("jsonschema", str(ctx.exception))


class TestCheckNoDuplicatesWithRowFilter(unittest.TestCase):
    """CheckNoDuplicates must apply the row-filter before counting duplicates."""

    def setUp(self):
        self.conn = duckdb.connect()
        self.conn.execute(
            "CREATE TABLE focus_data (ResourceId VARCHAR, ProviderName VARCHAR)"
        )
        self.conn.executemany(
            "INSERT INTO focus_data VALUES (?, ?)",
            [
                ("r1", "AWS"),
                ("r1", "AWS"),   # duplicate within the AWS filter
                ("r2", "Azure"),
                ("r2", "Azure"),  # duplicate, but excluded by the filter
                (None, "AWS"),    # NULL never counts
            ],
        )

    def test_duplicates_outside_filter_are_ignored(self):
        generator = CheckNoDuplicatesGenerator(
            rule=_rule("DUP-1"),
            rule_id="DUP-1",
            ColumnName="ResourceId",
            row_condition_sql="ProviderName = 'AWS'",
        )
        # Only the duplicated AWS "r1" counts; the Azure "r2" pair is filtered out.
        self.assertEqual(_violations(self.conn, generator), 1)

    def test_without_filter_all_duplicates_counted(self):
        generator = CheckNoDuplicatesGenerator(
            rule=_rule("DUP-2"),
            rule_id="DUP-2",
            ColumnName="ResourceId",
        )
        # Both "r1" and "r2" are duplicated when no filter is applied.
        self.assertEqual(_violations(self.conn, generator), 2)

    def test_row_filter_appears_in_sql(self):
        generator = CheckNoDuplicatesGenerator(
            rule=_rule("DUP-3"),
            rule_id="DUP-3",
            ColumnName="ResourceId",
            row_condition_sql="ProviderName = 'AWS'",
        )
        sql = generator.generateSql().requirement_sql
        self.assertIn("(ResourceId IS NOT NULL) AND (ProviderName = 'AWS')", sql)


if __name__ == "__main__":
    unittest.main()
