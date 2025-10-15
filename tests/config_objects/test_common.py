"""Comprehensive tests for config_objects.common module."""

import unittest
from unittest.mock import Mock, patch
import json
from enum import Enum
from pydantic import ValidationError

# We'll mock sqlglot since it's not available in the environment
import sys
from unittest.mock import MagicMock
sys.modules['sqlglot'] = MagicMock()
sys.modules['sqlglot.exp'] = MagicMock()

from focus_validator.config_objects.common import (
    ChecklistObjectStatus,
    DataTypes,
    AllowNullsCheck,
    ValueInCheck,
    SQLQueryCheck,
    ValueComparisonCheck,
    FormatCheck,
    DistinctCountCheck,
    ModelRuleCheck,
    ColumnComparisonCheck,
    DataTypeCheck,
    generate_check_friendly_name,
)


class TestChecklistObjectStatus(unittest.TestCase):
    """Test the ChecklistObjectStatus enum."""

    def test_enum_values(self):
        """Test that all expected enum values are present."""
        expected_values = {
            "errored", "passed", "failed", "skipped", "pending"
        }
        actual_values = {status.value for status in ChecklistObjectStatus}
        self.assertEqual(actual_values, expected_values)

    def test_enum_access_by_name(self):
        """Test accessing enum values by name."""
        self.assertEqual(ChecklistObjectStatus.ERRORED.value, "errored")
        self.assertEqual(ChecklistObjectStatus.PASSED.value, "passed")
        self.assertEqual(ChecklistObjectStatus.FAILED.value, "failed")
        self.assertEqual(ChecklistObjectStatus.SKIPPED.value, "skipped")
        self.assertEqual(ChecklistObjectStatus.PENDING.value, "pending")

    def test_enum_comparison(self):
        """Test enum comparison operations."""
        self.assertEqual(ChecklistObjectStatus.PASSED, ChecklistObjectStatus.PASSED)
        self.assertNotEqual(ChecklistObjectStatus.PASSED, ChecklistObjectStatus.FAILED)

    def test_enum_is_enum(self):
        """Test that ChecklistObjectStatus is properly an enum."""
        self.assertTrue(issubclass(ChecklistObjectStatus, Enum))


class TestDataTypes(unittest.TestCase):
    """Test the DataTypes enum."""

    def test_enum_values(self):
        """Test that all expected data type values are present."""
        expected_values = {
            "string", "decimal", "datetime", "currency-code", "stringified-json-object"
        }
        actual_values = {dtype.value for dtype in DataTypes}
        self.assertEqual(actual_values, expected_values)

    def test_enum_access_by_name(self):
        """Test accessing enum values by name."""
        self.assertEqual(DataTypes.STRING.value, "string")
        self.assertEqual(DataTypes.DECIMAL.value, "decimal")
        self.assertEqual(DataTypes.DATETIME.value, "datetime")
        self.assertEqual(DataTypes.CURRENCY_CODE.value, "currency-code")
        self.assertEqual(DataTypes.STRINGIFIED_JSON_OBJECT.value, "stringified-json-object")


class TestAllowNullsCheck(unittest.TestCase):
    """Test the AllowNullsCheck model."""

    def test_valid_creation(self):
        """Test creating AllowNullsCheck with valid data."""
        check = AllowNullsCheck(allow_nulls=True)
        self.assertTrue(check.allow_nulls)

        check = AllowNullsCheck(allow_nulls=False)
        self.assertFalse(check.allow_nulls)

    def test_dict_creation(self):
        """Test creating AllowNullsCheck from dictionary."""
        check = AllowNullsCheck(**{"allow_nulls": True})
        self.assertTrue(check.allow_nulls)

    def test_model_dump(self):
        """Test model serialization."""
        check = AllowNullsCheck(allow_nulls=True)
        data = check.model_dump()
        self.assertEqual(data, {"allow_nulls": True})


class TestValueInCheck(unittest.TestCase):
    """Test the ValueInCheck model."""

    def test_valid_creation(self):
        """Test creating ValueInCheck with valid data."""
        values = ["value1", "value2", "value3"]
        check = ValueInCheck(value_in=values)
        self.assertEqual(check.value_in, values)

    def test_empty_list(self):
        """Test creating ValueInCheck with empty list."""
        check = ValueInCheck(value_in=[])
        self.assertEqual(check.value_in, [])

    def test_single_value(self):
        """Test creating ValueInCheck with single value."""
        check = ValueInCheck(value_in=["single"])
        self.assertEqual(check.value_in, ["single"])

    def test_model_dump(self):
        """Test model serialization."""
        values = ["a", "b", "c"]
        check = ValueInCheck(value_in=values)
        data = check.model_dump()
        self.assertEqual(data, {"value_in": values})


class TestSQLQueryCheck(unittest.TestCase):
    """Test the SQLQueryCheck model."""

    @patch('focus_validator.config_objects.common.sqlglot.parse_one')
    def test_valid_sql_query(self, mock_parse_one):
        """Test creating SQLQueryCheck with valid SQL."""
        # Mock sqlglot parsing to return expected structure
        mock_column = Mock()
        mock_column.alias = "check_output"
        mock_query = Mock()
        mock_query.find_all.return_value = [mock_column]
        mock_parse_one.return_value = mock_query
        
        sql = "SELECT result AS check_output FROM table"
        check = SQLQueryCheck(sql_query=sql)
        self.assertEqual(check.sql_query, sql)

    @patch('focus_validator.config_objects.common.sqlglot.parse_one')
    def test_invalid_sql_query_wrong_column(self, mock_parse_one):
        """Test creating SQLQueryCheck with wrong column name."""
        # Mock sqlglot parsing to return wrong column name
        mock_column = Mock()
        mock_column.alias = "wrong_column"
        mock_query = Mock()
        mock_query.find_all.return_value = [mock_column]
        mock_parse_one.return_value = mock_query
        
        with self.assertRaises(ValidationError):
            SQLQueryCheck(sql_query="SELECT result AS wrong_column FROM table")

    @patch('focus_validator.config_objects.common.sqlglot.parse_one')
    def test_invalid_sql_query_multiple_columns(self, mock_parse_one):
        """Test creating SQLQueryCheck with multiple columns."""
        # Mock sqlglot parsing to return multiple columns
        mock_column1 = Mock()
        mock_column1.alias = "check_output"
        mock_column2 = Mock()
        mock_column2.alias = "extra_column"
        mock_query = Mock()
        mock_query.find_all.return_value = [mock_column1, mock_column2]
        mock_parse_one.return_value = mock_query
        
        with self.assertRaises(ValidationError):
            SQLQueryCheck(sql_query="SELECT result AS check_output, extra AS extra_column FROM table")


class TestValueComparisonCheck(unittest.TestCase):
    """Test the ValueComparisonCheck model."""

    def test_valid_operators(self):
        """Test creating ValueComparisonCheck with valid operators."""
        valid_operators = ["equals", "not_equals", "greater_equal", "not_equals_column", "equals_column"]
        
        for operator in valid_operators:
            with self.subTest(operator=operator):
                check = ValueComparisonCheck(operator=operator, value="test")
                self.assertEqual(check.operator, operator)

    def test_different_value_types(self):
        """Test ValueComparisonCheck with different value types."""
        test_cases = [
            ("equals", "string_value"),
            ("not_equals", 42),
            ("greater_equal", 3.14),
            ("equals_column", None),
        ]
        
        for operator, value in test_cases:
            with self.subTest(operator=operator, value=value):
                check = ValueComparisonCheck(operator=operator, value=value)
                self.assertEqual(check.value, value)

    def test_model_dump(self):
        """Test model serialization."""
        check = ValueComparisonCheck(operator="equals", value="test")
        data = check.model_dump()
        self.assertEqual(data, {"operator": "equals", "value": "test"})


class TestFormatCheck(unittest.TestCase):
    """Test the FormatCheck model."""

    def test_valid_format_types(self):
        """Test creating FormatCheck with valid format types."""
        valid_formats = ["numeric", "datetime", "currency_code", "string", "key_value", "unit"]
        
        for format_type in valid_formats:
            with self.subTest(format_type=format_type):
                check = FormatCheck(format_type=format_type)
                self.assertEqual(check.format_type, format_type)

    def test_model_dump(self):
        """Test model serialization."""
        check = FormatCheck(format_type="datetime")
        data = check.model_dump()
        self.assertEqual(data, {"format_type": "datetime"})


class TestDistinctCountCheck(unittest.TestCase):
    """Test the DistinctCountCheck model."""

    def test_valid_creation(self):
        """Test creating DistinctCountCheck with valid data."""
        check = DistinctCountCheck(
            column_a_name="col_a",
            column_b_name="col_b",
            expected_count=5
        )
        self.assertEqual(check.column_a_name, "col_a")
        self.assertEqual(check.column_b_name, "col_b")
        self.assertEqual(check.expected_count, 5)

    def test_zero_expected_count(self):
        """Test DistinctCountCheck with zero expected count."""
        check = DistinctCountCheck(
            column_a_name="col_a",
            column_b_name="col_b",
            expected_count=0
        )
        self.assertEqual(check.expected_count, 0)

    def test_model_dump(self):
        """Test model serialization."""
        check = DistinctCountCheck(
            column_a_name="col_a",
            column_b_name="col_b",
            expected_count=10
        )
        data = check.model_dump()
        expected = {
            "column_a_name": "col_a",
            "column_b_name": "col_b",
            "expected_count": 10
        }
        self.assertEqual(data, expected)


class TestModelRuleCheck(unittest.TestCase):
    """Test the ModelRuleCheck model."""

    def test_valid_creation(self):
        """Test creating ModelRuleCheck with valid data."""
        check = ModelRuleCheck(model_rule_id="MODEL-001")
        self.assertEqual(check.model_rule_id, "MODEL-001")

    def test_empty_rule_id(self):
        """Test ModelRuleCheck with empty rule ID."""
        check = ModelRuleCheck(model_rule_id="")
        self.assertEqual(check.model_rule_id, "")

    def test_model_dump(self):
        """Test model serialization."""
        check = ModelRuleCheck(model_rule_id="MODEL-123")
        data = check.model_dump()
        self.assertEqual(data, {"model_rule_id": "MODEL-123"})


class TestColumnComparisonCheck(unittest.TestCase):
    """Test the ColumnComparisonCheck model."""

    def test_valid_creation(self):
        """Test creating ColumnComparisonCheck with valid data."""
        check = ColumnComparisonCheck(
            comparison_column="other_col",
            operator="equals"
        )
        self.assertEqual(check.comparison_column, "other_col")
        self.assertEqual(check.operator, "equals")

    def test_different_operators(self):
        """Test ColumnComparisonCheck with different operators."""
        operators = ["equals", "not_equals"]
        
        for operator in operators:
            with self.subTest(operator=operator):
                check = ColumnComparisonCheck(
                    comparison_column="col",
                    operator=operator
                )
                self.assertEqual(check.operator, operator)

    def test_model_dump(self):
        """Test model serialization."""
        check = ColumnComparisonCheck(
            comparison_column="test_col",
            operator="not_equals"
        )
        data = check.model_dump()
        expected = {
            "comparison_column": "test_col",
            "operator": "not_equals"
        }
        self.assertEqual(data, expected)


class TestDataTypeCheck(unittest.TestCase):
    """Test the DataTypeCheck model."""

    def test_valid_creation_with_enum(self):
        """Test creating DataTypeCheck with DataTypes enum."""
        check = DataTypeCheck(data_type=DataTypes.STRING)
        self.assertEqual(check.data_type, DataTypes.STRING)

    def test_valid_creation_with_string(self):
        """Test creating DataTypeCheck with string value."""
        check = DataTypeCheck(data_type="decimal")
        self.assertEqual(check.data_type, DataTypes.DECIMAL)

    def test_all_data_types(self):
        """Test creating DataTypeCheck with all valid data types."""
        for data_type in DataTypes:
            with self.subTest(data_type=data_type):
                check = DataTypeCheck(data_type=data_type)
                self.assertEqual(check.data_type, data_type)

    def test_model_dump(self):
        """Test model serialization."""
        check = DataTypeCheck(data_type=DataTypes.DATETIME)
        data = check.model_dump()
        self.assertEqual(data, {"data_type": DataTypes.DATETIME})


class TestGenerateCheckFriendlyName(unittest.TestCase):
    """Test the generate_check_friendly_name function."""

    def test_with_description(self):
        """Test generating friendly name when description is provided."""
        result = generate_check_friendly_name(
            check={},
            column_id="test_column",
            description="  Custom description  "
        )
        self.assertEqual(result, "Custom description")

    def test_with_empty_description(self):
        """Test generating friendly name with empty description."""
        result = generate_check_friendly_name(
            check={},
            column_id="test_column",
            description=""
        )
        self.assertEqual(result, "Rule that does something")

    def test_without_description(self):
        """Test generating friendly name without description."""
        result = generate_check_friendly_name(
            check={},
            column_id="test_column"
        )
        self.assertEqual(result, "Rule that does something")

    def test_with_none_description(self):
        """Test generating friendly name with None description."""
        result = generate_check_friendly_name(
            check={},
            column_id="test_column",
            description=None
        )
        self.assertEqual(result, "Rule that does something")

    def test_description_with_whitespace(self):
        """Test generating friendly name with whitespace-only description."""
        result = generate_check_friendly_name(
            check={},
            column_id="test_column",
            description="   \t\n   "
        )
        self.assertEqual(result, "Rule that does something")

    def test_description_stripping(self):
        """Test that description is properly stripped."""
        result = generate_check_friendly_name(
            check={},
            column_id="test_column",
            description="\n\t  Detailed rule description  \t\n"
        )
        self.assertEqual(result, "Detailed rule description")


class TestIntegrationScenarios(unittest.TestCase):
    """Test integration scenarios combining multiple check types."""

    def test_check_serialization_roundtrip(self):
        """Test that checks can be serialized and deserialized."""
        checks = [
            AllowNullsCheck(allow_nulls=True),
            ValueInCheck(value_in=["a", "b", "c"]),
            ValueComparisonCheck(operator="equals", value="test"),
            FormatCheck(format_type="numeric"),
            DataTypeCheck(data_type=DataTypes.STRING)
        ]
        
        for check in checks:
            with self.subTest(check=type(check).__name__):
                # Serialize to dict
                data = check.model_dump()
                self.assertIsInstance(data, dict)
                
                # Recreate from dict
                recreated = type(check)(**data)
                self.assertEqual(check.model_dump(), recreated.model_dump())

    def test_complex_check_combinations(self):
        """Test complex combinations of checks."""
        # Simulate a complex validation scenario
        distinct_check = DistinctCountCheck(
            column_a_name="resource_id",
            column_b_name="usage_date", 
            expected_count=1
        )
        
        format_check = FormatCheck(format_type="datetime")
        
        value_check = ValueComparisonCheck(
            operator="greater_equal",
            value=0
        )
        
        # All should be independently valid
        self.assertIsNotNone(distinct_check.column_a_name)
        self.assertIsNotNone(format_check.format_type)
        self.assertIsNotNone(value_check.operator)

    def test_error_scenarios(self):
        """Test various error scenarios."""
        # Test invalid operator for ValueComparisonCheck
        with self.assertRaises(Exception):  # Should raise validation error
            try:
                ValueComparisonCheck(operator="invalid_operator", value="test")
            except Exception as e:
                # Expect pydantic validation error for invalid literal
                self.assertTrue("invalid_operator" in str(e) or "validation" in str(e).lower())
                raise

        # Test invalid format type for FormatCheck
        with self.assertRaises(Exception):  # Should raise validation error
            try:
                FormatCheck(format_type="invalid_format")
            except Exception as e:
                # Expect pydantic validation error for invalid literal
                self.assertTrue("invalid_format" in str(e) or "validation" in str(e).lower())
                raise


if __name__ == '__main__':
    unittest.main()