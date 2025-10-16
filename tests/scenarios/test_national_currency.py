import unittest
import pandas as pd
from io import StringIO
from helper import load_rule_data_from_file
from helper import SpecRulesFromData

class TestNationalCurrency(unittest.TestCase):
    """Test national currency."""
    def setUp(self):
        self.rule_data = load_rule_data_from_file("base_rule_data.json")
        self.rule_data['ModelDatasets'] = {
            "CostAndUsage": {
                "ModelRules": ["BillingCurrency-C-006-M"]
            }
        }
        self.rule_data["ModelRules"] = {
            "BillingCurrency-C-006-M": {
                "Function": "Validation",
                "Reference": "BillingCurrency",
                "EntityType": "Column",
                "Notes": "",
                "ModelVersionIntroduced": "1.2",
                "Status": "Active",
                "ApplicabilityCriteria": [],
                "Type": "Static",
                "ValidationCriteria": {
                "MustSatisfy": "BillingCurrency MUST be expressed in national currency (e.g., USD, EUR).",
                "Keyword": "MUST",
                "Requirement": {
                    "CheckFunction": "CheckNationalCurrency",
                    "ColumnName": "BillingCurrency"
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
        csv_data = """BillingCurrency
USD
EUR
"""
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)
        
        # Check rule state
        rule_result = results.by_rule_id["BillingCurrency-C-006-M"]
        self.assertTrue(rule_result.get("ok"), f"Rule should PASS but got: {rule_result}")
    
    def test_rule_fail_scenario(self):
        """Test failure."""
        csv_data = """BillingCurrency
FOO
"""
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)
        
        # Check rule state
        rule_result = results.by_rule_id["BillingCurrency-C-006-M"]
        self.assertFalse(rule_result.get("ok"), f"Rule should FAIL but got: {rule_result}")

if __name__ == '__main__':
    unittest.main()
