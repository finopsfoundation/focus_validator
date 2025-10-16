import unittest
import pandas as pd
from io import StringIO
from helper import load_rule_data_from_file
from helper import SpecRulesFromData

class TestTypeDateTime(unittest.TestCase):
    """Test datetimes."""

    def setUp(self):
        self.rule_data = load_rule_data_from_file("base_rule_data.json")
        self.rule_data['ModelDatasets'] = {
            "CostAndUsage": {
                "ModelRules": ["BillingPeriodStart-C-001-M", "BillingPeriodStart-C-002-M"]
            }
        }
        self.rule_data["ModelRules"] = {
            "BillingPeriodStart-C-001-M": {
                "Function": "Type",
                "Reference": "BillingPeriodStart",
                "EntityType": "Column",
                "Notes": "",
                "ModelVersionIntroduced": "1.2",
                "Status": "Active",
                "ApplicabilityCriteria": [],
                "Type": "Static",
                "ValidationCriteria": {
                "MustSatisfy": "BillingPeriodStart MUST be of type Date/Time.",
                "Keyword": "MUST",
                "Requirement": {
                    "CheckFunction": "TypeDateTime",
                    "ColumnName": "BillingPeriodStart"
                },
                "Condition": {},
                "Dependencies": []
                }
            },
            "BillingPeriodStart-C-002-M": {
                "Function": "Format",
                "Reference": "BillingPeriodStart",
                "EntityType": "Column",
                "Notes": "",
                "ModelVersionIntroduced": "1.2",
                "Status": "Active",
                "ApplicabilityCriteria": [],
                "Type": "Static",
                "ValidationCriteria": {
                "MustSatisfy": "BillingPeriodStart MUST conform to DateTimeFormat requirements.",
                "Keyword": "MUST",
                "Requirement": {
                    "CheckFunction": "FormatDateTime",
                    "ColumnName": "BillingPeriodStart"
                },
                "Condition": {},
                "Dependencies": []
                }
            }
        }
        self.spec_rules = SpecRulesFromData(
            rule_data=self.rule_data,
            focus_dataset="CostAndUsage",
            filter_rules=None,
            applicability_criteria_list=["CostAndUsage"]
        )
        self.spec_rules.load()

    def test_rule_pass_scenario(self):
        """Test pass."""
        csv_data = """BillingPeriodStart
2024-01-01T00:00:00Z
2024-01-02T00:00:00Z
"""
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)
        
        # Check rule state
        rule_result = results.by_rule_id["BillingPeriodStart-C-001-M"]
        self.assertTrue(rule_result.get("ok"), f"Rule should PASS but got: {rule_result}")
    
        # Check that we have exactly 0 violations (all rows should pass case-sensitive matching)
        violations = rule_result.get("details", {}).get("violations", 0)
        self.assertEqual(violations, 0, f"Expected 0 violations but got {violations}")

    def test_rule_fail_scenario(self):
        """Test failure."""
        csv_data = """BillingPeriodStart
not-a-date
2024-01-01T00:00:00
2024-01-01 00:00:00Z
2024-01-01
2024-01-02T00:00:00Z
"""
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)
        
        # Check rule state
        rule_result = results.by_rule_id["BillingPeriodStart-C-001-M"]
        self.assertFalse(rule_result.get("ok"), f"Rule should FAIL but got: {rule_result}")
        
        # Check that we have exactly 4 violations (all rows should fail case-sensitive matching)
        violations = rule_result.get("details", {}).get("violations", 0)
        self.assertEqual(violations, 4, f"Expected 4 violations but got {violations}")

if __name__ == '__main__':
    unittest.main()
