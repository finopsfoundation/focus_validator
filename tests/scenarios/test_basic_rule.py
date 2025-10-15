import unittest
import pandas as pd
from io import StringIO
from helper import load_rule_data_from_file
from helper import SpecRulesFromData

class TestExample(unittest.TestCase):
    """Example basic test to use as template."""
    
    def setUp(self):
        self.rule_data = load_rule_data_from_file("base_rule_data.json")
        self.rule_data['ModelDatasets'] = {
            "CostAndUsage": {
                "ModelRules": ["BillingAccountName-C-001-M"]
            }
        }
        self.rule_data["ModelRules"] = {
                "BillingAccountName-C-001-M": {
                "Function": "Type",
                "Reference": "BillingAccountName",
                "EntityType": "Column",
                "Notes": "",
                "ModelVersionIntroduced": "1.2",
                "Status": "Active",
                "ApplicabilityCriteria": [],
                "Type": "Static",
                "ValidationCriteria": {
                "MustSatisfy": "BillingAccountName MUST be of type String.",
                "Keyword": "MUST",
                "Requirement": {
                    "CheckFunction": "TypeString",
                    "ColumnName": "BillingAccountName"
                },
                "Condition": {},
                "Dependencies": []
                }
            }
        }
        self.spec_rules = SpecRulesFromData(
            rule_data=self.rule_data,
            focus_dataset="CostAndUsage",
            filter_rules="BillingAccountName",
            applicability_criteria_list=["CostAndUsage"]
        )
        self.spec_rules.load()

    def test_rule_pass_scenario(self):
        """Test pass."""
        csv_data = """BillingAccountName
"AccountName123"
"""
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)
        
        # Check rule state
        rule_result = results.by_rule_id["BillingAccountName-C-001-M"]
        self.assertTrue(rule_result.get("ok"), f"Rule should PASS but got: {rule_result}")
    
    def test_rule_fail_scenario(self):
        """Test failure."""
        csv_data = """BillingAccountName
123.45
"""
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)
        
        # Check rule state
        rule_result = results.by_rule_id["BillingAccountName-C-001-M"]
        self.assertFalse(rule_result.get("ok"), f"Rule should FAIL but got: {rule_result}")

if __name__ == '__main__':
    unittest.main()
