import unittest
import pandas as pd
from io import StringIO
from helper import load_rule_data_from_file
from helper import SpecRulesFromData

class TestNotNullable(unittest.TestCase):
    """Test allowed values casing."""

    def setUp(self):
        self.rule_data = load_rule_data_from_file("base_rule_data.json")
        self.rule_data['ModelDatasets'] = {
            "CostAndUsage": {
                "ModelRules": ["BilledCost-C-003-M"]
            }
        }
        self.rule_data["ModelRules"] = {
                "BilledCost-C-003-M": {
                "Function": "Nullability",
                "Reference": "BilledCost",
                "EntityType": "Column",
                "Notes": "",
                "ModelVersionIntroduced": "1.2",
                "Status": "Active",
                "ApplicabilityCriteria": [],
                "Type": "Static",
                "ValidationCriteria": {
                "MustSatisfy": "BilledCost MUST NOT be null.",
                "Keyword": "MUST NOT",
                "Requirement": {
                    "CheckFunction": "CheckNotValue",
                    "ColumnName": "BilledCost",
                    "Value": None
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
            applicability_criteria_list=["ALL"]
        )
        self.spec_rules.load()

    def test_rule_pass_scenario(self):
        """Test pass."""
        csv_data = """BilledCost,x_foo
0.2,b
10.5,a
"""
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)
        
        # Check rule state
        rule_result = results.by_rule_id["BilledCost-C-003-M"]
        self.assertTrue(rule_result.get("ok"), f"Rule should PASS but got: {rule_result}")
    
        # Check that we have exactly 0 violations (all rows should pass case-sensitive matching)
        violations = rule_result.get("details", {}).get("violations", 0)
        self.assertEqual(violations, 0, f"Expected 0 violations but got {violations}")

    def test_rule_fail_scenario(self):
        """Test failure."""
        csv_data = """BilledCost,x_foo
,b
10.5,a
"""
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)
        
        # Check rule state
        rule_result = results.by_rule_id["BilledCost-C-003-M"]
        self.assertFalse(rule_result.get("ok"), f"Rule should FAIL but got: {rule_result}")
        
        # Check that we have exactly 1 violations (all rows should fail case-sensitive matching)
        violations = rule_result.get("details", {}).get("violations", 0)
        self.assertEqual(violations, 1, f"Expected 1 violations but got {violations}")

if __name__ == '__main__':
    unittest.main()
