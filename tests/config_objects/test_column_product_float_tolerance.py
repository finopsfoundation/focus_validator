"""Regression tests for float64 tolerance in ColumnByColumnEqualsColumnValue.

FOCUS defines relationships like ListCost = ListUnitPrice * PricingQuantity as
exact. The CSV loader infers numeric columns as Float64/DOUBLE, so the product
is computed in IEEE-754 double precision. A zero-tolerance ``<>`` comparison
flags rows whose exact-decimal product equals the result but whose float64
product differs by a rounding bit. These tests execute the generated SQL against
a real DuckDB instance to confirm such rows are no longer reported as
violations, while genuine mismatches still are.
"""

import unittest
from unittest.mock import Mock

import duckdb

from focus_validator.config_objects.focus_to_duckdb_converter import (
    ColumnByColumnEqualsColumnValueGenerator,
)
from focus_validator.config_objects.rule import ModelRule


def _requirement_sql(generator, table_name="t"):
    return generator.generateSql().get_requirement_sql().replace(
        "{table_name}", table_name
    )


def _predicate_sql(generator):
    return generator.generateSql().get_predicate_sql()


class TestColumnProductFloatTolerance(unittest.TestCase):
    def setUp(self):
        mock_rule = Mock(spec=ModelRule)
        mock_rule.rule_id = "CAU-ListCost-C-011-C"
        self.generator = ColumnByColumnEqualsColumnValueGenerator(
            rule=mock_rule,
            rule_id="CAU-ListCost-C-011-C",
            ColumnAName="ListUnitPrice",
            ColumnBName="PricingQuantity",
            ResultColumnName="ListCost",
        )
        self.con = duckdb.connect()
        self.con.execute(
            "CREATE TABLE t ("
            "ListUnitPrice DOUBLE, PricingQuantity DOUBLE, ListCost DOUBLE)"
        )

    def tearDown(self):
        self.con.close()

    def _violations(self):
        sql = _requirement_sql(self.generator)
        return self.con.execute(sql).fetchone()[0]

    def test_float_rounding_is_not_a_violation(self):
        # 0.000015 * 20 == 0.000300 in decimal, but float64 yields
        # 0.00030000000000000003. Must not be flagged.
        self.con.execute("INSERT INTO t VALUES (0.000015, 20, 0.000300)")
        self.assertEqual(self._violations(), 0)

    def test_genuine_mismatch_is_a_violation(self):
        self.con.execute("INSERT INTO t VALUES (2.0, 3.0, 10.0)")
        self.assertEqual(self._violations(), 1)

    def test_exact_match_is_not_a_violation(self):
        self.con.execute("INSERT INTO t VALUES (2.0, 3.0, 6.0)")
        self.assertEqual(self._violations(), 0)

    def test_zero_values_are_not_a_violation(self):
        self.con.execute("INSERT INTO t VALUES (0.0, 0.0, 0.0)")
        self.assertEqual(self._violations(), 0)

    def test_nulls_are_skipped(self):
        self.con.execute("INSERT INTO t VALUES (NULL, 20, 0.0003)")
        self.assertEqual(self._violations(), 0)

    def test_predicate_passes_on_float_rounding(self):
        self.con.execute("INSERT INTO t VALUES (0.000015, 20, 0.000300)")
        passing = self.con.execute(
            f"SELECT COUNT(*) FROM t WHERE {_predicate_sql(self.generator)}"
        ).fetchone()[0]
        self.assertEqual(passing, 1)


if __name__ == "__main__":
    unittest.main()
