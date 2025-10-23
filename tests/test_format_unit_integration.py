"""
Integration test for FormatUnitGenerator to reproduce and fix the command line error.
"""

import unittest
import pandas as pd
from io import StringIO
import sys
import os

# Add the tests directory to Python path to import helper
sys.path.append(os.path.join(os.path.dirname(__file__), 'scenarios'))
from helper import load_rule_data_from_file, SpecRulesFromData


class TestFormatUnitIntegration(unittest.TestCase):
    """Test FormatUnitGenerator integration with the validator."""

    def setUp(self):
        """Set up test with FormatUnit rule for ConsumedUnit column."""
        self.rule_data = load_rule_data_from_file("base_rule_data.json")
        self.rule_data['ModelDatasets'] = {
            "CostAndUsage": {
                "ModelRules": ["ConsumedUnit-C-003-O"]
            }
        }
        self.rule_data["ModelRules"] = {
            "ConsumedUnit-C-003-O": {
                "Function": "FormatUnit",
                "Reference": "4.9.4",
                "EntityType": "Column",
                "Notes": "ConsumedUnit values SHOULD follow the FOCUS Unit Format specification.",
                "ModelVersionIntroduced": "1.2",
                "Status": "Active",
                "ApplicabilityCriteria": [],
                "Type": "Static",
                "ValidationCriteria": {
                    "MustSatisfy": "ConsumedUnit values SHOULD follow the FOCUS Unit Format specification.",
                    "Keyword": "SHOULD",
                    "Requirement": {
                        "CheckFunction": "FormatUnit",
                        "ColumnName": "ConsumedUnit"
                    },
                    "Condition": {},
                    "Dependencies": []
                }
            }
        }

    def test_format_unit_command_line_error_reproduction(self):
        """
        This test now demonstrates that the original error has been FIXED.
        The error was: RuntimeError: 'violations' is not an integer for ConsumedUnit-C-003-O (got str: 'ConsumedUnit').
        After the fix, this should work without errors.
        """
        spec_rules = SpecRulesFromData(
            rule_data=self.rule_data,
            focus_dataset="CostAndUsage",
            filter_rules=None,
            applicability_criteria_list=["ALL"]
        )
        spec_rules.load()

        # Create test data with various unit values including invalid ones
        csv_data = """ConsumedUnit
GB
Hours
invalid-unit
GB-Hours
terabyte
"""
        df = pd.read_csv(StringIO(csv_data))

        # This should now work without the "violations is not an integer" error
        results = spec_rules.validate(focus_data=df)
        
        # Verify the fix worked - we should get proper results
        self.assertIsNotNone(results)
        self.assertTrue(hasattr(results, 'by_rule_id'))
        self.assertIn("ConsumedUnit-C-003-O", results.by_rule_id)
        
        # Check that we're getting integer violation counts, not string values
        rule_result = results.by_rule_id["ConsumedUnit-C-003-O"]
        self.assertIn("details", rule_result)
        self.assertIn("violations", rule_result["details"])
        self.assertIsInstance(rule_result["details"]["violations"], int)
        
        # Should have violations since 'invalid-unit' and 'terabyte' are not valid FOCUS units
        self.assertGreater(rule_result["details"]["violations"], 0)

    def test_format_unit_works_after_fix(self):
        """
        Test that FormatUnitGenerator works correctly after fixing the SQL structure.
        This test should pass after we fix the SQL to return violation counts.
        """
        spec_rules = SpecRulesFromData(
            rule_data=self.rule_data,
            focus_dataset="CostAndUsage",
            filter_rules=None,
            applicability_criteria_list=["ALL"]
        )
        spec_rules.load()

        # Create test data with various unit values
        csv_data = """ConsumedUnit
GB
Hours
invalid-unit
GB-Hours
terabyte
"""
        df = pd.read_csv(StringIO(csv_data))

        # This should work without errors after the fix
        results = spec_rules.validate(focus_data=df)
        
        # Verify results structure
        self.assertIsNotNone(results)
        self.assertTrue(hasattr(results, 'by_rule_id'))
        self.assertIn("ConsumedUnit-C-003-O", results.by_rule_id)
        
        # Check the specific rule result
        rule_result = results.by_rule_id["ConsumedUnit-C-003-O"]
        self.assertIn("ok", rule_result)
        self.assertIn("details", rule_result)
        
        # Should fail because we have invalid units
        self.assertFalse(rule_result["ok"])
        self.assertIn("violations", rule_result["details"])
        
        # Should have violations for invalid units ('invalid-unit' and 'terabyte')
        # The exact count might vary based on our test data, but should be > 0
        self.assertGreater(rule_result["details"]["violations"], 0)