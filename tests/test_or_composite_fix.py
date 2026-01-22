"""Test OR composite rules with CheckValue children correctly evaluate."""
import pandas as pd
from io import StringIO
import unittest
import sys
import os

# Add parent directory to path to import helper
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'scenarios'))
from helper import SpecRulesFromData, load_rule_data_from_file


class TestORCompositeFix(unittest.TestCase):
    """Test that OR composites with CheckValue children work correctly."""

    def test_or_composite_passes_with_valid_values(self):
        """Test that OR composite passes when data contains one of the allowed values."""
        rule_data = load_rule_data_from_file("base_rule_data.json")
        rule_data['ModelDatasets'] = {
            "CostAndUsage": {
                "ModelRules": ["TestColumn-C-001-M"]
            }
        }
        rule_data["ModelRules"] = {
            "TestColumn-C-001-M": {
                "Function": "AllowedValues",
                "Reference": "TestColumn",
                "EntityType": "Column",
                "ModelVersionIntroduced": "1.0",
                "Status": "Active",
                "ApplicabilityCriteria": [],
                "Type": "Static",
                "ValidationCriteria": {
                    "MustSatisfy": "TestColumn MUST be one of the allowed values",
                    "Keyword": "MUST",
                    "Requirement": {
                        "CheckFunction": "OR",
                        "Items": [
                            {"CheckFunction": "CheckValue", "ColumnName": "TestColumn", "Value": "ValueA"},
                            {"CheckFunction": "CheckValue", "ColumnName": "TestColumn", "Value": "ValueB"},
                            {"CheckFunction": "CheckValue", "ColumnName": "TestColumn", "Value": "ValueC"},
                        ]
                    },
                    "Condition": {},
                    "Dependencies": []
                }
            }
        }

        spec_rules = SpecRulesFromData(
            rule_data=rule_data,
            focus_dataset="CostAndUsage",
            filter_rules=None,
        )
        spec_rules.load()

        # Create test data with two allowed values
        csv_data = """TestColumn
ValueA
ValueA
ValueB
ValueB
"""
        df = pd.read_csv(StringIO(csv_data))
        results = spec_rules.validate(focus_data=df)

        # Check rule result
        rule_result = results.by_rule_id["TestColumn-C-001-M"]
        self.assertTrue(
            rule_result.get("ok"),
            f"Rule should PASS when data contains allowed values but got: {rule_result}"
        )
        self.assertEqual(
            rule_result.get("details", {}).get("violations"),
            0,
            f"Violations should be 0 but got: {rule_result}"
        )

    def test_or_composite_fails_with_invalid_values(self):
        """Test that OR composite fails when data contains NO allowed values."""
        rule_data = load_rule_data_from_file("base_rule_data.json")
        rule_data['ModelDatasets'] = {
            "CostAndUsage": {
                "ModelRules": ["TestColumn-C-001-M"]
            }
        }
        rule_data["ModelRules"] = {
            "TestColumn-C-001-M": {
                "Function": "AllowedValues",
                "Reference": "TestColumn",
                "EntityType": "Column",
                "ModelVersionIntroduced": "1.0",
                "Status": "Active",
                "ApplicabilityCriteria": [],
                "Type": "Static",
                "ValidationCriteria": {
                    "MustSatisfy": "TestColumn MUST be one of the allowed values",
                    "Keyword": "MUST",
                    "Requirement": {
                        "CheckFunction": "OR",
                        "Items": [
                            {"CheckFunction": "CheckValue", "ColumnName": "TestColumn", "Value": "ValueA"},
                            {"CheckFunction": "CheckValue", "ColumnName": "TestColumn", "Value": "ValueB"},
                            {"CheckFunction": "CheckValue", "ColumnName": "TestColumn", "Value": "ValueC"},
                        ]
                    },
                    "Condition": {},
                    "Dependencies": []
                }
            }
        }

        spec_rules = SpecRulesFromData(
            rule_data=rule_data,
            focus_dataset="CostAndUsage",
            filter_rules=None,
        )
        spec_rules.load()

        # Create test data with only invalid values
        csv_data = """TestColumn
InvalidValue
AnotherInvalid
"""
        df = pd.read_csv(StringIO(csv_data))
        results = spec_rules.validate(focus_data=df)

        # Check rule result
        rule_result = results.by_rule_id["TestColumn-C-001-M"]
        self.assertFalse(
            rule_result.get("ok"),
            f"Rule should FAIL when data contains NO allowed values but got: {rule_result}"
        )
        self.assertGreater(
            rule_result.get("details", {}).get("violations"),
            0,
            f"Violations should be > 0 but got: {rule_result}"
        )
        # Check that message indicates OR failure
        message = rule_result.get("details", {}).get("message", "")
        self.assertIn(
            "OR failed",
            message,
            f"Message should indicate OR failure but got: {message}"
        )

    def test_or_composite_passes_with_mixed_values(self):
        """Test that OR composite passes when data contains mix of valid and invalid values."""
        rule_data = load_rule_data_from_file("base_rule_data.json")
        rule_data['ModelDatasets'] = {
            "CostAndUsage": {
                "ModelRules": ["TestColumn-C-001-M"]
            }
        }
        rule_data["ModelRules"] = {
            "TestColumn-C-001-M": {
                "Function": "AllowedValues",
                "Reference": "TestColumn",
                "EntityType": "Column",
                "ModelVersionIntroduced": "1.0",
                "Status": "Active",
                "ApplicabilityCriteria": [],
                "Type": "Static",
                "ValidationCriteria": {
                    "MustSatisfy": "TestColumn MUST be one of the allowed values",
                    "Keyword": "MUST",
                    "Requirement": {
                        "CheckFunction": "OR",
                        "Items": [
                            {"CheckFunction": "CheckValue", "ColumnName": "TestColumn", "Value": "Storage"},
                            {"CheckFunction": "CheckValue", "ColumnName": "TestColumn", "Value": "Compute"},
                            {"CheckFunction": "CheckValue", "ColumnName": "TestColumn", "Value": "Networking"},
                        ]
                    },
                    "Condition": {},
                    "Dependencies": []
                }
            }
        }

        spec_rules = SpecRulesFromData(
            rule_data=rule_data,
            focus_dataset="CostAndUsage",
            filter_rules=None,
        )
        spec_rules.load()

        # Create test data with only valid values (subset of allowed)
        csv_data = """TestColumn
Storage
Storage
Compute
"""
        df = pd.read_csv(StringIO(csv_data))
        results = spec_rules.validate(focus_data=df)

        # Check rule result
        rule_result = results.by_rule_id["TestColumn-C-001-M"]
        self.assertTrue(
            rule_result.get("ok"),
            f"Rule should PASS when data contains subset of allowed values but got: {rule_result}"
        )
        self.assertEqual(
            rule_result.get("details", {}).get("violations"),
            0,
            f"Violations should be 0 but got: {rule_result}"
        )
        # Check that message indicates OR success with specific child rules
        message = rule_result.get("details", {}).get("message", "")
        self.assertIn(
            "OR passed",
            message,
            f"Message should indicate OR passed but got: {message}"
        )
