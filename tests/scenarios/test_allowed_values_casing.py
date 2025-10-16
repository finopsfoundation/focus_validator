import unittest
import pandas as pd
from io import StringIO
from helper import load_rule_data_from_file
from helper import SpecRulesFromData

class TestAllowedValuesCasing(unittest.TestCase):
    """Test allowed values casing."""

    def setUp(self):
        self.rule_data = load_rule_data_from_file("base_rule_data.json")
        self.rule_data['ModelDatasets'] = {
            "CostAndUsage": {
                "ModelRules": ["ChargeCategory-C-003-M"]
            }
        }
        self.rule_data["ModelRules"] = {
                "ChargeCategory-C-003-M": {
                "Function": "Validation",
                "Reference": "ChargeCategory",
                "EntityType": "Column",
                "Notes": "",
                "ModelVersionIntroduced": "1.2",
                "Status": "Active",
                "ApplicabilityCriteria": [],
                "Type": "Static",
                "ValidationCriteria": {
                "MustSatisfy": "ChargeCategory MUST be one of the allowed values.",
                "Keyword": "MUST",
                "Requirement": {
                    "CheckFunction": "OR",
                    "Items": [
                    {
                        "CheckFunction": "CheckValue",
                        "ColumnName": "ChargeCategory",
                        "Value": "Usage"
                    },
                    {
                        "CheckFunction": "CheckValue",
                        "ColumnName": "ChargeCategory",
                        "Value": "Purchase"
                    },
                    {
                        "CheckFunction": "CheckValue",
                        "ColumnName": "ChargeCategory",
                        "Value": "Tax"
                    },
                    {
                        "CheckFunction": "CheckValue",
                        "ColumnName": "ChargeCategory",
                        "Value": "Credit"
                    },
                    {
                        "CheckFunction": "CheckValue",
                        "ColumnName": "ChargeCategory",
                        "Value": "Adjustment"
                    }
                    ]
                },
                "Condition": {},
                "Dependencies": []
                }
            }
        }
        self.spec_rules = SpecRulesFromData(
            rule_data=self.rule_data,
            focus_dataset="CostAndUsage",
            filter_rules="ChargeCategory",
            applicability_criteria_list=["ALL"]
        )
        self.spec_rules.load()

    def test_rule_pass_scenario(self):
        """Test pass."""
        csv_data = """ChargeCategory
Purchase
Usage
Tax
Credit
Adjustment
"""
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)
        
        # Check rule state
        rule_result = results.by_rule_id["ChargeCategory-C-003-M"]
        self.assertTrue(rule_result.get("ok"), f"Rule should PASS but got: {rule_result}")
    
        # Check that we have exactly 0 violations (all rows should pass case-sensitive matching)
        violations = rule_result.get("details", {}).get("violations", 0)
        self.assertEqual(violations, 0, f"Expected 0 violations but got {violations}")

    def test_rule_fail_scenario(self):
        """Test failure."""
        csv_data = """ChargeCategory
usage
USAGE
purchase
TAX
"""
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)
        
        # Check rule state
        rule_result = results.by_rule_id["ChargeCategory-C-003-M"]
        self.assertFalse(rule_result.get("ok"), f"Rule should FAIL but got: {rule_result}")
        
        # Check that we have exactly 4 violations (all rows should fail case-sensitive matching)
        violations = rule_result.get("details", {}).get("violations", 0)
        self.assertEqual(violations, 4, f"Expected 4 violations but got {violations}")

if __name__ == '__main__':
    unittest.main()
