import unittest
import pandas as pd
from io import StringIO
from helper import load_rule_data_from_file
from helper import SpecRulesFromData

class TestGreaterThanOrEqual(unittest.TestCase):
    """Test greater than or equal rule."""
    
    def setUp(self):
        self.rule_data = load_rule_data_from_file("base_rule_data.json")
        self.rule_data['ModelDatasets'] = {
            "CostAndUsage": {
                "ModelRules": ["ListUnitPrice-C-008-M"]
            }
        }
        self.rule_data["ModelRules"] = {
            "ListUnitPrice-C-008-M": {
                "Function": "Validation",
                "Reference": "ListUnitPrice",
                "EntityType": "Column",
                "Notes": None,
                "ModelVersionIntroduced": "1.2",
                "Status": "Active",
                "ApplicabilityCriteria": [],
                "Type": "Static",
                "ValidationCriteria": {
                "MustSatisfy": "ListUnitPrice MUST be a non-negative decimal value.",
                "Keyword": "MUST",
                "Requirement": {
                    "CheckFunction": "CheckGreaterOrEqualThanValue",
                    "ColumnName": "ListUnitPrice",
                    "Value": 0
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
        csv_data = """ListUnitPrice
0.00
"""
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)
        
        # Check rule state
        rule_result = results.by_rule_id["ListUnitPrice-C-008-M"]
        self.assertTrue(rule_result.get("ok"), f"Rule should PASS but got: {rule_result}")
    
    def test_rule_pass_scenario2(self):
        """Test pass."""
        csv_data = """ListUnitPrice
123.45
"""
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)
        
        # Check rule state
        rule_result = results.by_rule_id["ListUnitPrice-C-008-M"]
        self.assertTrue(rule_result.get("ok"), f"Rule should PASS but got: {rule_result}")

    def test_rule_fail_scenario(self):
        """Test failure."""
        csv_data = """ListUnitPrice
-100.50
"""
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)
        
        # Check rule state
        rule_result = results.by_rule_id["ListUnitPrice-C-008-M"]
        self.assertFalse(rule_result.get("ok"), f"Rule should FAIL but got: {rule_result}")

if __name__ == '__main__':
    unittest.main()
