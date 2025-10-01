"""Comprehensive tests for DuckDB SQL generators.

This module tests the SQL generation capabilities of various DuckDB check generators
using real rule configurations similar to those found in FOCUS specifications.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
from types import SimpleNamespace
import json
import sys

# Mock duckdb since it might not be available in test environment
sys.modules['duckdb'] = MagicMock()

from focus_validator.config_objects.focus_to_duckdb_converter import (
    # Core classes
    DuckDBColumnCheck,
    DuckDBCheckGenerator,
    
    # Type generators
    TypeDecimalCheckGenerator,
    TypeStringCheckGenerator,
    TypeDateTimeGenerator,
    
    # Format generators
    FormatNumericGenerator,
    FormatStringGenerator,
    FormatDateTimeGenerator,
    FormatBillingCurrencyCodeGenerator,
    FormatKeyValueGenerator,
    FormatCurrencyGenerator,
    
    # Value check generators
    CheckValueGenerator,
    CheckNotValueGenerator,
    CheckSameValueGenerator,
    CheckNotSameValueGenerator,
    CheckGreaterOrEqualGenerator,
    
    # Column comparison generators
    ColumnByColumnEqualsColumnValueGenerator,
    
    # Advanced generators
    CheckDistinctCountGenerator,
    CheckConformanceRuleGenerator,
    CompositeBaseRuleGenerator,
    
    # Utility functions
    _compact_json
)
from focus_validator.config_objects.rule import ConformanceRule, ValidationCriteria


class TestDuckDBColumnCheck(unittest.TestCase):
    """Test the DuckDBColumnCheck data structure."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_rule = Mock(spec=ConformanceRule)
        self.mock_rule.rule_id = "TEST-001"
        self.mock_rule.function = "TypeDecimal"
    
    def test_basic_column_check_creation(self):
        """Test basic DuckDBColumnCheck creation."""
        check = DuckDBColumnCheck(
            rule_id="TEST-001",
            rule=self.mock_rule,
            check_type="type_decimal",
            check_sql="SELECT COUNT(*) AS violations FROM test",
            error_message="Type validation failed",
            nested_checks=[]
        )
        
        self.assertEqual(check.rule_id, "TEST-001")
        self.assertEqual(check.rule, self.mock_rule)
        self.assertEqual(check.checkType, "type_decimal")
        self.assertEqual(check.checkSql, "SELECT COUNT(*) AS violations FROM test")
        self.assertEqual(check.errorMessage, "Type validation failed")
        self.assertEqual(check.nestedChecks, [])
        self.assertEqual(check.exec_mode, "requirement")
        
    def test_column_check_with_metadata(self):
        """Test DuckDBColumnCheck with metadata."""
        meta = {"generator": "TypeDecimal", "column": "ListUnitPrice"}
        
        check = DuckDBColumnCheck(
            rule_id="TEST-002",
            rule=self.mock_rule,
            check_type="type_decimal",
            check_sql="SELECT COUNT(*) AS violations FROM test",
            error_message="Validation failed",
            nested_checks=None,
            meta=meta,
            exec_mode="condition"
        )
        
        self.assertEqual(check.meta, meta)
        self.assertEqual(check.exec_mode, "condition")
        self.assertEqual(check.nestedChecks, [])


class TestTypeDecimalGenerator(unittest.TestCase):
    """Test TypeDecimal SQL generation for FOCUS ListUnitPrice rule."""
    
    def setUp(self):
        """Set up test fixtures with real FOCUS rule structure."""
        # Create a ConformanceRule similar to the ListUnitPrice example
        self.rule_data = {
            "Function": "Type",
            "Reference": "ListUnitPrice", 
            "EntityType": "Column",
            "Notes": "",
            "CRVersionIntroduced": "1.2",
            "Status": "Active",
            "ApplicabilityCriteria": [],
            "Type": "Static",
            "ValidationCriteria": {
                "MustSatisfy": "ListUnitPrice MUST be of type Decimal.",
                "Keyword": "MUST",
                "Requirement": {
                    "CheckFunction": "TypeDecimal",
                    "ColumnName": "ListUnitPrice"
                },
                "Condition": {},
                "Dependencies": []
            }
        }
        
        # Create ValidationCriteria
        self.validation_criteria = ValidationCriteria(
            MustSatisfy=self.rule_data["ValidationCriteria"]["MustSatisfy"],
            Keyword=self.rule_data["ValidationCriteria"]["Keyword"],
            Requirement=self.rule_data["ValidationCriteria"]["Requirement"],
            Condition=self.rule_data["ValidationCriteria"]["Condition"],
            Dependencies=self.rule_data["ValidationCriteria"]["Dependencies"]
        )
        
        # Create ConformanceRule 
        self.mock_rule = Mock(spec=ConformanceRule)
        self.mock_rule.rule_id = "ListUnitPrice-C-001-M"
        self.mock_rule.function = self.rule_data["Function"]
        self.mock_rule.validation_criteria = self.validation_criteria
        
        # Create generator with required parameters
        self.generator = TypeDecimalCheckGenerator(
            rule=self.mock_rule,
            rule_id="ListUnitPrice-C-001-M",
            ColumnName="ListUnitPrice"
        )
    
    def test_type_decimal_sql_generation(self):
        """Test SQL generation for TypeDecimal check."""
        sql = self.generator.generateSql()
        
        # Verify SQL structure
        self.assertIn("WITH invalid AS", sql)
        self.assertIn("SELECT 1", sql)
        self.assertIn("FROM {table_name}", sql)
        self.assertIn("WHERE ListUnitPrice IS NOT NULL", sql)
        self.assertIn("typeof(ListUnitPrice) NOT IN ('DECIMAL', 'DOUBLE', 'FLOAT')", sql)
        self.assertIn("COUNT(*) AS violations", sql)
        self.assertIn("CASE WHEN COUNT(*) > 0", sql)
        
        # Verify error message
        self.assertIn("ListUnitPrice MUST be of type DECIMAL, DOUBLE, or FLOAT", sql)
        
    def test_type_decimal_check_type(self):
        """Test check type identification."""
        check_type = self.generator.getCheckType()
        self.assertEqual(check_type, "type_decimal")
        
    def test_type_decimal_with_custom_error_message(self):
        """Test TypeDecimal with custom error message."""
        self.generator.errorMessage = "Custom decimal validation failed"
        sql = self.generator.generateSql()
        
        self.assertIn("Custom decimal validation failed", sql)
        
    def test_required_keys_validation(self):
        """Test that required keys are properly defined."""
        self.assertEqual(self.generator.REQUIRED_KEYS, {"ColumnName"})


class TestTypeStringGenerator(unittest.TestCase):
    """Test TypeString SQL generation."""
    
    def setUp(self):
        """Set up TypeString generator."""
        mock_rule = Mock(spec=ConformanceRule)
        mock_rule.rule_id = "BillingAccountId-TYPE-001"
        
        self.generator = TypeStringCheckGenerator(
            rule=mock_rule,
            rule_id="BillingAccountId-TYPE-001",
            ColumnName="BillingAccountId"
        )
        
    def test_type_string_sql_generation(self):
        """Test SQL generation for TypeString check."""
        sql = self.generator.generateSql()
        
        # Verify SQL structure for string type validation
        self.assertIn("WITH invalid AS", sql)
        self.assertIn("BillingAccountId IS NOT NULL", sql)
        self.assertIn("typeof(BillingAccountId) != 'VARCHAR'", sql)
        self.assertIn("BillingAccountId MUST be of type VARCHAR", sql)
        
    def test_type_string_check_type(self):
        """Test check type identification."""
        check_type = self.generator.getCheckType()
        self.assertEqual(check_type, "type_string")


class TestCheckValueGenerator(unittest.TestCase):
    """Test CheckValue SQL generation for exact value matching."""
    
    def setUp(self):
        """Set up CheckValue generator."""
        self.mock_rule = Mock(spec=ConformanceRule)
        self.mock_rule.rule_id = "TEST-CHECK-VALUE"
        
    def test_check_value_with_string_value(self):
        """Test CheckValue with string value."""
        generator = CheckValueGenerator(
            rule=self.mock_rule,
            rule_id="TEST-CHECK-VALUE",
            ColumnName="Currency",
            Value="USD"
        )
        
        sql = generator.generateSql()
        
        self.assertIn("WITH invalid AS", sql)
        self.assertIn("Currency != 'USD'", sql)
        self.assertIn("Currency MUST equal ''USD''", sql)
        
    def test_check_value_with_null_value(self):
        """Test CheckValue with NULL value."""
        generator = CheckValueGenerator(
            rule=self.mock_rule,
            rule_id="TEST-CHECK-VALUE-NULL",
            ColumnName="OptionalField",
            Value=None
        )
        
        sql = generator.generateSql()
        
        self.assertIn("OptionalField IS NOT NULL", sql)
        self.assertIn("OptionalField MUST be NULL", sql)
        
    def test_check_value_with_numeric_value(self):
        """Test CheckValue with numeric value."""
        generator = CheckValueGenerator(
            rule=self.mock_rule,
            rule_id="TEST-CHECK-VALUE-NUM",
            ColumnName="Version",
            Value=1.0
        )
        
        sql = generator.generateSql()
        
        self.assertIn("Version != '1.0'", sql)
        self.assertIn("Version MUST equal ''1.0''", sql)
        
    def test_check_value_sql_injection_prevention(self):
        """Test that single quotes are properly escaped."""
        generator = CheckValueGenerator(
            rule=self.mock_rule,
            rule_id="TEST-CHECK-VALUE-INJECT",
            ColumnName="TestField",
            Value="O'Reilly"
        )
        
        sql = generator.generateSql()
        
        # Single quotes should be escaped
        self.assertIn("O''Reilly", sql)


class TestFormatGenerators(unittest.TestCase):
    """Test various format validation generators."""
    
    def test_format_numeric_generator(self):
        """Test FormatNumeric SQL generation."""
        mock_rule = Mock(spec=ConformanceRule)
        mock_rule.rule_id = "FORMAT-NUMERIC-001"
        
        generator = FormatNumericGenerator(
            rule=mock_rule,
            rule_id="FORMAT-NUMERIC-001",
            ColumnName="Amount"
        )
        
        sql = generator.generateSql()
        
        self.assertIn("WITH invalid AS", sql)
        self.assertIn("Amount IS NOT NULL", sql)
        # Should validate numeric format (no currency symbols, proper decimal places)
        
    def test_format_datetime_generator(self):
        """Test FormatDateTime SQL generation."""
        mock_rule = Mock(spec=ConformanceRule)
        mock_rule.rule_id = "FORMAT-DATETIME-001"
        
        generator = FormatDateTimeGenerator(
            rule=mock_rule,
            rule_id="FORMAT-DATETIME-001",
            ColumnName="BillingPeriodStart"
        )
        
        sql = generator.generateSql()
        
        self.assertIn("WITH invalid AS", sql)
        self.assertIn("BillingPeriodStart IS NOT NULL", sql)
        # Should validate ISO 8601 datetime format
        
    def test_format_currency_code_generator(self):
        """Test FormatBillingCurrencyCode SQL generation."""
        mock_rule = Mock(spec=ConformanceRule)
        mock_rule.rule_id = "FORMAT-CURRENCY-001"
        
        generator = FormatBillingCurrencyCodeGenerator(
            rule=mock_rule,
            rule_id="FORMAT-CURRENCY-001",
            ColumnName="BillingCurrency"
        )
        
        sql = generator.generateSql()
        
        self.assertIn("WITH invalid AS", sql)
        self.assertIn("BillingCurrency IS NOT NULL", sql)
        # Should validate against ISO 4217 currency codes


class TestComparisonGenerators(unittest.TestCase):
    """Test comparison and relational check generators."""
    
    def test_check_greater_or_equal_generator(self):
        """Test CheckGreaterOrEqual SQL generation."""
        mock_rule = Mock(spec=ConformanceRule)
        mock_rule.rule_id = "CHECK-GTE-001"
        
        generator = CheckGreaterOrEqualGenerator(
            rule=mock_rule,
            rule_id="CHECK-GTE-001",
            ColumnName="UsageQuantity",
            Value=0
        )
        
        sql = generator.generateSql()
        
        self.assertIn("WITH invalid AS", sql)
        self.assertIn("UsageQuantity < 0", sql)
        self.assertIn("UsageQuantity MUST be greater than or equal to 0", sql)
        
    def test_check_not_value_generator(self):
        """Test CheckNotValue SQL generation."""
        mock_rule = Mock(spec=ConformanceRule)
        mock_rule.rule_id = "CHECK-NOT-VALUE-001"
        
        generator = CheckNotValueGenerator(
            rule=mock_rule,
            rule_id="CHECK-NOT-VALUE-001",
            ColumnName="Status",
            Value="Invalid"
        )
        
        sql = generator.generateSql()
        
        # Fixed: CheckNotValue should find violations where Status IS NOT NULL AND Status = 'Invalid'
        self.assertIn("Status IS NOT NULL AND Status = 'Invalid'", sql)
        self.assertIn("Status MUST NOT be ''Invalid''", sql)
        
    def test_check_not_value_generator_with_null_handling(self):
        """Test CheckNotValue handles NULL values correctly (ChargeClass != 'Correction' scenario)."""
        mock_rule = Mock(spec=ConformanceRule)
        mock_rule.rule_id = "CHECK-NOT-VALUE-002"
        
        # Test the ChargeClass != 'Correction' scenario that was failing
        generator = CheckNotValueGenerator(
            rule=mock_rule,
            rule_id="CHECK-NOT-VALUE-002",
            ColumnName="ChargeClass",
            Value="Correction"
        )
        
        sql = generator.generateSql()
        
        # Should only find violations where ChargeClass is NOT NULL and equals 'Correction'
        # This will exclude NULL values (which should not be violations)
        self.assertIn("ChargeClass IS NOT NULL AND ChargeClass = 'Correction'", sql)
        self.assertIn("ChargeClass MUST NOT be ''Correction''", sql)
        
        # Verify SQL structure is valid
        self.assertIn("WITH invalid AS", sql)
        self.assertIn("COUNT(*) AS violations", sql)
        
    def test_column_comparison_generator(self):
        """Test ColumnByColumnEqualsColumnValue generator."""
        mock_rule = Mock(spec=ConformanceRule)
        mock_rule.rule_id = "COLUMN-COMPARE-001"
        
        generator = ColumnByColumnEqualsColumnValueGenerator(
            rule=mock_rule,
            rule_id="COLUMN-COMPARE-001",
            ColumnAName="EffectiveCost",
            ColumnBName="BilledCost",
            ResultColumnName="ExpectedCost"
        )
        
        sql = generator.generateSql()
        
        self.assertIn("(EffectiveCost * BilledCost)", sql)
        

class TestAdvancedGenerators(unittest.TestCase):
    """Test advanced generator functionality."""
    
    def test_check_distinct_count_generator(self):
        """Test CheckDistinctCount SQL generation."""
        mock_rule = Mock(spec=ConformanceRule)
        mock_rule.rule_id = "DISTINCT-COUNT-001"
        
        generator = CheckDistinctCountGenerator(
            rule=mock_rule,
            rule_id="DISTINCT-COUNT-001",
            ColumnAName="BillingAccountId",
            ColumnBName="BillingAccountName",
            ExpectedCount=1
        )
        
        sql = generator.generateSql()
        
        self.assertIn("GROUP BY BillingAccountId", sql)
        self.assertIn("COUNT(DISTINCT BillingAccountName)", sql)
        self.assertIn("<> 1", sql)


class TestSQLGenerationPatterns(unittest.TestCase):
    """Test common SQL generation patterns and utilities."""
    
    def test_compact_json_function(self):
        """Test JSON compaction utility."""
        data = {"key": "value", "nested": {"inner": "data"}}
        
        # Short JSON should not be truncated
        result = _compact_json(data, max_len=1000)
        self.assertIn("key", result)
        self.assertIn("value", result)
        self.assertNotIn("truncated", result)
        
        # Long JSON should be truncated
        result = _compact_json(data, max_len=10)
        self.assertIn("truncated", result)
        
    def test_sql_template_structure(self):
        """Test that all generators follow consistent SQL template structure."""
        mock_rule = Mock(spec=ConformanceRule)
        mock_rule.rule_id = "TEMPLATE-TEST"
        
        generators = [
            TypeDecimalCheckGenerator(rule=mock_rule, rule_id="TEMPLATE-TEST", ColumnName="TestColumn"),
            TypeStringCheckGenerator(rule=mock_rule, rule_id="TEMPLATE-TEST", ColumnName="TestColumn"), 
            CheckValueGenerator(rule=mock_rule, rule_id="TEMPLATE-TEST", ColumnName="TestColumn", Value="TestValue"),
            FormatNumericGenerator(rule=mock_rule, rule_id="TEMPLATE-TEST", ColumnName="TestColumn")
        ]
        
        for generator in generators:
            
            sql = generator.generateSql()
            
            # All generators should follow CTE pattern
            self.assertIn("WITH invalid AS", sql, f"Generator {type(generator)} missing CTE pattern")
            self.assertIn("COUNT(*) AS violations", sql, f"Generator {type(generator)} missing violations count")
            self.assertIn("FROM {table_name}", sql, f"Generator {type(generator)} missing table placeholder")


class TestRuleIntegration(unittest.TestCase):
    """Test integration with actual FOCUS rule structures."""
    
    def test_listunitprice_rule_integration(self):
        """Test complete ListUnitPrice rule processing."""
        # Real FOCUS rule structure
        rule_json = {
            "Function": "Type",
            "Reference": "ListUnitPrice",
            "EntityType": "Column", 
            "Notes": "",
            "CRVersionIntroduced": "1.2",
            "Status": "Active",
            "ApplicabilityCriteria": [],
            "Type": "Static",
            "ValidationCriteria": {
                "MustSatisfy": "ListUnitPrice MUST be of type Decimal.",
                "Keyword": "MUST",
                "Requirement": {
                    "CheckFunction": "TypeDecimal",
                    "ColumnName": "ListUnitPrice"
                },
                "Condition": {},
                "Dependencies": []
            }
        }
        
        # Create ConformanceRule from JSON
        validation_criteria = ValidationCriteria(**rule_json["ValidationCriteria"])
        
        # Create and configure generator
        mock_rule = Mock(spec=ConformanceRule)
        mock_rule.rule_id = "ListUnitPrice-C-001-M"
        mock_rule.validation_criteria = validation_criteria
        
        generator = TypeDecimalCheckGenerator(
            rule=mock_rule,
            rule_id="ListUnitPrice-C-001-M",
            ColumnName=rule_json["ValidationCriteria"]["Requirement"]["ColumnName"]
        )
        
        # Generate SQL
        sql = generator.generateSql()
        
        # Validate generated SQL matches expected structure
        self.assertIn("ListUnitPrice", sql)
        self.assertIn("DECIMAL", sql)
        self.assertIn("DOUBLE", sql)
        self.assertIn("FLOAT", sql)
        
        # Verify SQL is executable (syntax check)
        # Note: We're not actually executing against a database
        self.assertTrue(sql.strip().endswith('invalid'))
        
    def test_multiple_generator_consistency(self):
        """Test that different generators produce consistent SQL patterns."""
        test_cases = [
            {
                "generator_class": TypeDecimalCheckGenerator,
                "params": {"ColumnName": "Amount"},
                "expected_patterns": ["DECIMAL", "DOUBLE", "FLOAT"]
            },
            {
                "generator_class": CheckValueGenerator, 
                "params": {"ColumnName": "Currency", "Value": "USD"},
                "expected_patterns": ["Currency != 'USD'", "MUST equal"]
            },
            {
                "generator_class": FormatDateTimeGenerator,
                "params": {"ColumnName": "Date"},
                "expected_patterns": ["Date IS NOT NULL"]
            }
        ]
        
        for case in test_cases:
            mock_rule = Mock(spec=ConformanceRule)
            mock_rule.rule_id = f"CONSISTENCY-TEST-{case['generator_class'].__name__}"
            
            generator = case["generator_class"](
                rule=mock_rule,
                rule_id=f"CONSISTENCY-TEST-{case['generator_class'].__name__}",
                **case["params"]
            )
            
            sql = generator.generateSql()
            
            # Check expected patterns exist
            for pattern in case["expected_patterns"]:
                self.assertIn(pattern, sql, 
                    f"Pattern '{pattern}' not found in {case['generator_class'].__name__} SQL")
            
            # Verify common structure
            self.assertIn("WITH invalid AS", sql)
            self.assertIn("COUNT(*) AS violations", sql)


class TestGeneratePredicateFunctionality(unittest.TestCase):
    """Test the generatePredicate() functionality for condition mode."""
    
    def test_check_value_generate_predicate(self):
        """Test CheckValue generatePredicate for condition mode."""
        mock_rule = Mock(spec=ConformanceRule)
        mock_rule.rule_id = "TEST-PREDICATE-001"
        
        # Test with string value
        generator = CheckValueGenerator(
            rule=mock_rule,
            rule_id="TEST-PREDICATE-001",
            ColumnName="Currency",
            Value="USD",
            exec_mode="condition"  # This triggers predicate generation
        )
        
        predicate = generator.generatePredicate()
        self.assertIsNotNone(predicate)
        self.assertEqual(predicate, "Currency = 'USD'")
        
        # Test with NULL value
        null_generator = CheckValueGenerator(
            rule=mock_rule,
            rule_id="TEST-PREDICATE-002", 
            ColumnName="OptionalField",
            Value=None,
            exec_mode="condition"
        )
        
        null_predicate = null_generator.generatePredicate()
        self.assertIsNotNone(null_predicate)
        self.assertEqual(null_predicate, "OptionalField IS NULL")
        
    def test_check_not_same_value_generate_predicate(self):
        """Test CheckNotSameValue generatePredicate for condition mode."""
        mock_rule = Mock(spec=ConformanceRule)
        mock_rule.rule_id = "TEST-PREDICATE-003"
        
        generator = CheckNotSameValueGenerator(
            rule=mock_rule,
            rule_id="TEST-PREDICATE-003",
            ColumnAName="ProviderName",
            ColumnBName="InvoiceIssuerName", 
            exec_mode="condition"
        )
        
        predicate = generator.generatePredicate()
        self.assertIsNotNone(predicate)
        self.assertIn("ProviderName IS NOT NULL", predicate)
        self.assertIn("InvoiceIssuerName IS NOT NULL", predicate)
        self.assertIn("ProviderName <> InvoiceIssuerName", predicate)
        
        # Verify it's a boolean expression, not a full query
        self.assertNotIn("SELECT", predicate)
        self.assertNotIn("FROM", predicate)
        self.assertNotIn("WITH", predicate)
        
    def test_check_greater_or_equal_generate_predicate(self):
        """Test CheckGreaterOrEqual generatePredicate for condition mode."""
        mock_rule = Mock(spec=ConformanceRule)
        mock_rule.rule_id = "TEST-PREDICATE-004"
        
        generator = CheckGreaterOrEqualGenerator(
            rule=mock_rule,
            rule_id="TEST-PREDICATE-004",
            ColumnName="Amount",
            Value=100,
            exec_mode="condition"
        )
        
        predicate = generator.generatePredicate()
        self.assertIsNotNone(predicate)
        self.assertIn("Amount IS NOT NULL", predicate)
        self.assertIn("Amount >= 100", predicate)
        
    def test_generators_without_predicate_support(self):
        """Test generators that don't support generatePredicate (requirement mode only)."""
        mock_rule = Mock(spec=ConformanceRule)
        mock_rule.rule_id = "TEST-PREDICATE-005"
        
        # TypeDecimal generator doesn't support condition mode
        type_generator = TypeDecimalCheckGenerator(
            rule=mock_rule,
            rule_id="TEST-PREDICATE-005",
            ColumnName="Amount"
        )
        
        # Should return None when not in condition mode
        predicate = type_generator.generatePredicate()
        self.assertIsNone(predicate)
        
    def test_predicate_vs_sql_difference(self):
        """Test the difference between generatePredicate() and generateSql()."""
        mock_rule = Mock(spec=ConformanceRule)
        mock_rule.rule_id = "TEST-PREDICATE-006"
        
        # Test same generator in different modes
        requirement_generator = CheckValueGenerator(
            rule=mock_rule,
            rule_id="TEST-PREDICATE-006",
            ColumnName="Status",
            Value="Active"
            # exec_mode defaults to "requirement"
        )
        
        condition_generator = CheckValueGenerator(
            rule=mock_rule,
            rule_id="TEST-PREDICATE-006",
            ColumnName="Status",
            Value="Active",
            exec_mode="condition"
        )
        
        # Requirement mode: full SQL query for violation detection
        requirement_sql = requirement_generator.generateSql()
        self.assertIn("SELECT", requirement_sql)
        self.assertIn("COUNT(*) AS violations", requirement_sql)
        self.assertIn("Status != 'Active'", requirement_sql)
        
        # Condition mode: boolean predicate for WHERE clause
        condition_predicate = condition_generator.generatePredicate()
        self.assertEqual(condition_predicate, "Status = 'Active'")
        
        # Requirement generator should not generate predicate (wrong mode)
        requirement_predicate = requirement_generator.generatePredicate()
        self.assertIsNone(requirement_predicate)


class TestConditionalRules(unittest.TestCase):
    """Test conditional rules with requirements and conditions."""
    
    def setUp(self):
        """Set up test fixtures for conditional rules."""
        # Real FOCUS conditional rule structure - BilledCost with condition
        self.conditional_rule_data = {
            "Function": "Validation",
            "Reference": "BilledCost",
            "EntityType": "Column",
            "Notes": "",
            "CRVersionIntroduced": "1.2",
            "Status": "Active",
            "ApplicabilityCriteria": [],
            "Type": "Static",
            "ValidationCriteria": {
                "MustSatisfy": "BilledCost MUST be 0 for charges where payments are received by a third party (e.g., marketplace transactions).",
                "Keyword": "MUST",
                "Requirement": {
                    "CheckFunction": "CheckValue",
                    "ColumnName": "BilledCost",
                    "Value": 0
                },
                "Condition": {
                    "CheckFunction": "CheckNotSameValue",
                    "ColumnAName": "ProviderName",
                    "ColumnBName": "InvoiceIssuerName"
                },
                "Dependencies": [
                    "ProviderName-C-000-M",
                    "InvoiceIssuerName-C-000-M"
                ]
            }
        }
        
        self.mock_rule = Mock(spec=ConformanceRule)
        self.mock_rule.rule_id = "BilledCost-C-004-C"
        self.mock_rule.function = "Validation"
    
    def test_check_value_with_condition_requirement(self):
        """Test CheckValue generator for requirement part of conditional rule."""
        # Test the requirement: BilledCost MUST be 0
        requirement_generator = CheckValueGenerator(
            rule=self.mock_rule,
            rule_id="BilledCost-C-004-C-REQ",
            ColumnName="BilledCost",
            Value=0
        )
        
        sql = requirement_generator.generateSql()
        
        # Verify SQL structure for BilledCost = 0 check
        self.assertIn("WITH invalid AS", sql)
        self.assertIn("BilledCost != '0'", sql)
        self.assertIn("BilledCost MUST equal ''0''.", sql)  # Note: escaped quotes in SQL
        self.assertIn("COUNT(*) AS violations", sql)
        
    def test_check_not_same_value_condition(self):
        """Test CheckNotSameValue generator for condition part of conditional rule."""
        # Test the condition: ProviderName != InvoiceIssuerName
        condition_generator = CheckNotSameValueGenerator(
            rule=self.mock_rule,
            rule_id="BilledCost-C-004-C-COND",
            ColumnAName="ProviderName",
            ColumnBName="InvoiceIssuerName"
        )
        
        sql = condition_generator.generateSql()
        
        # Verify SQL structure for column comparison
        self.assertIn("WITH invalid AS", sql)
        self.assertIn("ProviderName = InvoiceIssuerName", sql)
        self.assertIn("ProviderName and InvoiceIssuerName MUST NOT have the same value", sql)
        
    def test_conditional_rule_integration_pattern(self):
        """Test that both requirement and condition generators work together conceptually."""
        # In practice, these would be combined with AND/OR logic in the final SQL
        # This test verifies both parts generate valid SQL that could be combined
        
        # Create both generators
        requirement_gen = CheckValueGenerator(
            rule=self.mock_rule,
            rule_id="BilledCost-C-004-C-REQ",
            ColumnName="BilledCost",
            Value=0
        )
        
        condition_gen = CheckNotSameValueGenerator(
            rule=self.mock_rule,
            rule_id="BilledCost-C-004-C-COND",
            ColumnAName="ProviderName",
            ColumnBName="InvoiceIssuerName"
        )
        
        # Generate SQL for both parts
        requirement_sql = requirement_gen.generateSql()
        condition_sql = condition_gen.generateSql()
        
        # Both should be valid SQL with proper structure
        for sql in [requirement_sql, condition_sql]:
            self.assertIn("WITH invalid AS", sql)
            self.assertIn("SELECT", sql)
            self.assertIn("FROM {table_name}", sql)
            self.assertIn("COUNT(*) AS violations", sql)
            
        # Verify specific logic for each part
        self.assertIn("BilledCost", requirement_sql)
        self.assertIn("ProviderName", condition_sql)
        self.assertIn("InvoiceIssuerName", condition_sql)
    
    def test_marketplace_transaction_scenario(self):
        """Test the real-world scenario: marketplace transactions with third-party payments."""
        # This represents the business logic:
        # WHEN ProviderName != InvoiceIssuerName (third-party payment scenario)
        # THEN BilledCost MUST = 0 (no direct billing)
        
        # Test requirement part with descriptive error message
        requirement_gen = CheckValueGenerator(
            rule=self.mock_rule,
            rule_id="BilledCost-Marketplace-REQ",
            ColumnName="BilledCost",
            Value=0
        )
        requirement_gen.errorMessage = "BilledCost MUST be 0 for marketplace transactions where payments are received by a third party"
        
        sql = requirement_gen.generateSql()
        
        # Verify the custom error message is included
        self.assertIn("marketplace transactions", sql)
        self.assertIn("third party", sql)
        
    def test_dependency_references_in_conditional_rules(self):
        """Test that conditional rules can reference their dependencies."""
        # The rule depends on ProviderName-C-000-M and InvoiceIssuerName-C-000-M
        # This tests that our generators can handle rules that reference other rules
        
        dependencies = self.conditional_rule_data["ValidationCriteria"]["Dependencies"]
        
        self.assertEqual(len(dependencies), 2)
        self.assertIn("ProviderName-C-000-M", dependencies)
        self.assertIn("InvoiceIssuerName-C-000-M", dependencies)
        
        # Test that condition generator can work with the dependent columns
        condition_gen = CheckNotSameValueGenerator(
            rule=self.mock_rule,
            rule_id="BilledCost-C-004-C-COND",
            ColumnAName="ProviderName",  # Referenced in dependency
            ColumnBName="InvoiceIssuerName"  # Referenced in dependency
        )
        
        sql = condition_gen.generateSql()
        
        # SQL should reference both dependent columns
        self.assertIn("ProviderName", sql)
        self.assertIn("InvoiceIssuerName", sql)
        
    def test_complex_conditional_rule_validation(self):
        """Test validation of the complete conditional rule structure."""
        rule_data = self.conditional_rule_data
        validation_criteria = rule_data["ValidationCriteria"]
        
        # Validate rule structure
        self.assertEqual(validation_criteria["Keyword"], "MUST")
        self.assertIn("BilledCost MUST be 0", validation_criteria["MustSatisfy"])
        self.assertIn("third party", validation_criteria["MustSatisfy"])
        
        # Validate requirement structure
        requirement = validation_criteria["Requirement"]
        self.assertEqual(requirement["CheckFunction"], "CheckValue")
        self.assertEqual(requirement["ColumnName"], "BilledCost")
        self.assertEqual(requirement["Value"], 0)
        
        # Validate condition structure
        condition = validation_criteria["Condition"]
        self.assertEqual(condition["CheckFunction"], "CheckNotSameValue")
        self.assertEqual(condition["ColumnAName"], "ProviderName")
        self.assertEqual(condition["ColumnBName"], "InvoiceIssuerName")


class TestAdvancedConditionalScenarios(unittest.TestCase):
    """Test advanced conditional rule scenarios and combinations."""
    
    def test_multiple_condition_types(self):
        """Test generators that could be used in various conditional scenarios."""
        mock_rule = Mock(spec=ConformanceRule)
        mock_rule.rule_id = "MULTI-CONDITION-TEST"
        
        # Test various condition types that might be used in complex rules
        test_scenarios = [
            {
                "name": "String comparison condition",
                "generator": CheckValueGenerator,
                "params": {"ColumnName": "ChargeCategory", "Value": "Usage"},
                "expected_patterns": ["ChargeCategory != 'Usage'"]
            },
            {
                "name": "Numeric threshold condition", 
                "generator": CheckGreaterOrEqualGenerator,
                "params": {"ColumnName": "UsageQuantity", "Value": 0},
                "expected_patterns": ["UsageQuantity < 0"]
            },
            {
                "name": "Column equality condition",
                "generator": CheckNotSameValueGenerator,
                "params": {"ColumnAName": "BillingAccountId", "ColumnBName": "PayerAccountId"},
                "expected_patterns": ["BillingAccountId = PayerAccountId"]
            }
        ]
        
        for scenario in test_scenarios:
            with self.subTest(scenario["name"]):
                generator = scenario["generator"](
                    rule=mock_rule,
                    rule_id=f"CONDITION-{scenario['name'].upper().replace(' ', '-')}",
                    **scenario["params"]
                )
                
                sql = generator.generateSql()
                
                # Verify basic SQL structure
                self.assertIn("WITH invalid AS", sql)
                self.assertIn("COUNT(*) AS violations", sql)
                
                # Verify scenario-specific patterns
                for pattern in scenario["expected_patterns"]:
                    self.assertIn(pattern, sql)
    
    def test_nested_conditional_logic_preparation(self):
        """Test generators that could support nested conditional logic."""
        # This tests the building blocks for complex rules like:
        # IF (ServiceCategory = 'Compute' AND ChargeCategory = 'Usage') 
        # THEN UsageQuantity > 0
        
        mock_rule = Mock(spec=ConformanceRule)
        mock_rule.rule_id = "NESTED-LOGIC-TEST"
        
        # Generate components that could be combined with AND/OR logic
        service_condition = CheckValueGenerator(
            rule=mock_rule,
            rule_id="NESTED-SERVICE-COND",
            ColumnName="ServiceCategory",
            Value="Compute"
        )
        
        charge_condition = CheckValueGenerator(
            rule=mock_rule,
            rule_id="NESTED-CHARGE-COND", 
            ColumnName="ChargeCategory",
            Value="Usage"
        )
        
        quantity_requirement = CheckGreaterOrEqualGenerator(
            rule=mock_rule,
            rule_id="NESTED-QUANTITY-REQ",
            ColumnName="UsageQuantity",
            Value=0
        )
        
        # All should generate valid SQL that could be combined
        for generator in [service_condition, charge_condition, quantity_requirement]:
            sql = generator.generateSql()
            self.assertIn("WITH invalid AS", sql)
            self.assertIn("FROM {table_name}", sql)
            
        # Verify specific logic for each component
        service_sql = service_condition.generateSql()
        charge_sql = charge_condition.generateSql() 
        quantity_sql = quantity_requirement.generateSql()
        
        self.assertIn("ServiceCategory", service_sql)
        self.assertIn("ChargeCategory", charge_sql)
        self.assertIn("UsageQuantity", quantity_sql)
        

class TestErrorHandling(unittest.TestCase):
    """Test error handling and edge cases in generators."""
    
    def test_missing_required_parameters(self):
        """Test generator behavior with missing required parameters."""
        mock_rule = Mock(spec=ConformanceRule)
        mock_rule.rule_id = "MISSING-PARAMS-TEST"
        
        # This should raise a KeyError for missing ColumnName
        with self.assertRaises(KeyError):
            TypeDecimalCheckGenerator(rule=mock_rule, rule_id="MISSING-PARAMS-TEST")
            
    def test_empty_column_name(self):
        """Test generator with empty column name."""
        mock_rule = Mock(spec=ConformanceRule)
        mock_rule.rule_id = "EMPTY-COLUMN-TEST"
        
        generator = TypeDecimalCheckGenerator(
            rule=mock_rule,
            rule_id="EMPTY-COLUMN-TEST",
            ColumnName=""
        )
        
        sql = generator.generateSql()
        # Should still generate valid SQL structure
        self.assertIn("WITH invalid AS", sql)
        
    def test_special_characters_in_column_names(self):
        """Test generators with special characters in column names."""
        mock_rule = Mock(spec=ConformanceRule)
        mock_rule.rule_id = "SPECIAL-CHARS-TEST"
        
        generator = CheckValueGenerator(
            rule=mock_rule,
            rule_id="SPECIAL-CHARS-TEST",
            ColumnName="Column With Spaces",
            Value="test"
        )
        
        sql = generator.generateSql()
        
        # Should handle column names with spaces
        self.assertIn("Column With Spaces", sql)


class TestFOCUSRuleScenarios(unittest.TestCase):
    """Test real FOCUS rule scenarios and edge cases."""
    
    def test_billing_currency_rule(self):
        """Test BillingCurrency format validation rule."""
        rule_json = {
            "Function": "FormatBillingCurrencyCode",
            "Reference": "BillingCurrency",
            "EntityType": "Column",
            "ValidationCriteria": {
                "MustSatisfy": "BillingCurrency MUST be a valid ISO 4217 currency code.",
                "Keyword": "MUST",
                "Requirement": {
                    "CheckFunction": "FormatBillingCurrencyCode",
                    "ColumnName": "BillingCurrency"
                },
                "Condition": {},
                "Dependencies": []
            }
        }
        
        mock_rule = Mock(spec=ConformanceRule)
        mock_rule.rule_id = "BillingCurrency-FORMAT-001"
        
        generator = FormatBillingCurrencyCodeGenerator(
            rule=mock_rule,
            rule_id="BillingCurrency-FORMAT-001",
            ColumnName="BillingCurrency"
        )
        
        sql = generator.generateSql()
        
        # Should validate against ISO currency codes
        self.assertIn("WITH invalid AS", sql)
        self.assertIn("BillingCurrency IS NOT NULL", sql)
        self.assertIn("TRIM(BillingCurrency::TEXT) NOT IN", sql)
        
    def test_usage_quantity_range_rule(self):
        """Test UsageQuantity >= 0 validation rule."""
        mock_rule = Mock(spec=ConformanceRule)
        mock_rule.rule_id = "UsageQuantity-GTE-001"
        
        generator = CheckGreaterOrEqualGenerator(
            rule=mock_rule,
            rule_id="UsageQuantity-GTE-001",
            ColumnName="UsageQuantity",
            Value=0
        )
        
        sql = generator.generateSql()
        
        # Should check for non-negative values
        self.assertIn("UsageQuantity IS NOT NULL", sql)
        self.assertIn("UsageQuantity < 0", sql)
        
    def test_composite_cost_calculation_rule(self):
        """Test composite rule: EffectiveCost = ListUnitPrice * UsageQuantity."""
        mock_rule = Mock(spec=ConformanceRule)
        mock_rule.rule_id = "EffectiveCost-CALC-001"
        
        generator = ColumnByColumnEqualsColumnValueGenerator(
            rule=mock_rule,
            rule_id="EffectiveCost-CALC-001",
            ColumnAName="ListUnitPrice",
            ColumnBName="UsageQuantity", 
            ResultColumnName="EffectiveCost"
        )
        
        sql = generator.generateSql()
        
        # Should validate calculation
        self.assertIn("ListUnitPrice IS NOT NULL", sql)
        self.assertIn("UsageQuantity IS NOT NULL", sql)
        self.assertIn("EffectiveCost IS NOT NULL", sql)
        self.assertIn("(ListUnitPrice * UsageQuantity) <> EffectiveCost", sql)
        
    def test_account_id_consistency_rule(self):
        """Test BillingAccountId to BillingAccountName consistency."""
        mock_rule = Mock(spec=ConformanceRule)
        mock_rule.rule_id = "Account-CONSISTENCY-001"
        
        generator = CheckDistinctCountGenerator(
            rule=mock_rule,
            rule_id="Account-CONSISTENCY-001",
            ColumnAName="BillingAccountId",
            ColumnBName="BillingAccountName",
            ExpectedCount=1
        )
        
        sql = generator.generateSql()
        
        # Should group by account ID and count distinct names
        self.assertIn("GROUP BY BillingAccountId", sql)
        self.assertIn("COUNT(DISTINCT BillingAccountName)", sql)
        self.assertIn("<> 1", sql)


class TestSQLSafetyAndPerformance(unittest.TestCase):
    """Test SQL safety, performance, and edge cases."""
    
    def test_sql_injection_protection(self):
        """Test protection against SQL injection in various generators."""
        dangerous_values = [
            "'; DROP TABLE users; --",
            "' OR '1'='1",
            "test'; SELECT * FROM sensitive_data; --"
        ]
        
        mock_rule = Mock(spec=ConformanceRule)
        mock_rule.rule_id = "INJECTION-TEST"
        
        for dangerous_value in dangerous_values:
            generator = CheckValueGenerator(
                rule=mock_rule,
                rule_id="INJECTION-TEST",
                ColumnName="TestColumn",
                Value=dangerous_value
            )
            
            sql = generator.generateSql()
            
            # Should properly escape quotes to prevent SQL injection
            escaped_value = dangerous_value.replace("'", "''")
            self.assertIn(escaped_value, sql)
            
            # Dangerous values should be safely quoted in SQL literals
            # (they may appear in error messages but should be properly escaped)
            self.assertTrue("''" in sql)  # Should contain escaped quotes
            
    def test_null_handling_across_generators(self):
        """Test proper NULL handling in different generator types.""" 
        mock_rule = Mock(spec=ConformanceRule)
        mock_rule.rule_id = "NULL-HANDLING-TEST"
        
        generators = [
            TypeDecimalCheckGenerator(rule=mock_rule, rule_id="NULL-TEST-1", ColumnName="TestCol"),
            CheckValueGenerator(rule=mock_rule, rule_id="NULL-TEST-2", ColumnName="TestCol", Value=None),
            CheckNotValueGenerator(rule=mock_rule, rule_id="NULL-TEST-3", ColumnName="TestCol", Value=None)
        ]
        
        for generator in generators:
            sql = generator.generateSql()
            
            # All generators should handle NULL appropriately
            self.assertIn("NULL", sql)
            # Should use proper SQL NULL handling
            if "Value=None" in str(type(generator)):
                self.assertTrue("IS NULL" in sql or "IS NOT NULL" in sql)
                
    def test_large_value_handling(self):
        """Test handling of large numeric and string values."""
        mock_rule = Mock(spec=ConformanceRule)
        mock_rule.rule_id = "LARGE-VALUE-TEST"
        
        # Test very large number
        large_number = 999999999999999999.99
        generator = CheckValueGenerator(
            rule=mock_rule,
            rule_id="LARGE-VALUE-TEST-NUM",
            ColumnName="LargeAmount",
            Value=large_number
        )
        
        sql = generator.generateSql()
        self.assertIn(str(large_number), sql)
        
        # Test very long string
        long_string = "x" * 1000
        generator = CheckValueGenerator(
            rule=mock_rule, 
            rule_id="LARGE-VALUE-TEST-STR",
            ColumnName="LongString",
            Value=long_string
        )
        
        sql = generator.generateSql()
        self.assertIn(long_string, sql)
        
    def test_unicode_and_special_characters(self):
        """Test handling of Unicode and special characters."""
        mock_rule = Mock(spec=ConformanceRule)
        mock_rule.rule_id = "UNICODE-TEST"
        
        unicode_values = [
            "Café",
            "北京",
            "Москва", 
            "emoji 🚀 test",
            "newline\ntest",
            "tab\ttest"
        ]
        
        for unicode_value in unicode_values:
            generator = CheckValueGenerator(
                rule=mock_rule,
                rule_id="UNICODE-TEST",
                ColumnName="UnicodeColumn",
                Value=unicode_value
            )
            
            sql = generator.generateSql()
            
            # Should handle Unicode properly
            self.assertIn("WITH invalid AS", sql)
            self.assertTrue(len(sql) > 100)  # Should generate reasonable SQL


if __name__ == "__main__":
    unittest.main()