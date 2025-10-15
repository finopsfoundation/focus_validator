import unittest
import pandas as pd
from io import StringIO
from helper import SpecRulesFromData


class TestEmbeddedRuleData(unittest.TestCase):
    """Example test using embedded rule data instead of files."""
    
    def setUp(self):
        """Set up test with embedded rule data."""
        # Example: minimal FOCUS rule data structure
        self.rule_data = {
            "ModelDatasets": {
                "CostAndUsage": {
                    "ModelRules": ["BillingAccountName-C-001-M"]
                }
            },
            "ModelRules": {
                "BillingAccountName-C-001-M": {
                    "Function": "TypeString",
                    "Reference": "FOCUS-1.2#billing_account_name",
                    "EntityType": "Attribute",
                    "ModelVersionIntroduced": "1.0", 
                    "Status": "Active",
                    "ApplicabilityCriteria": ["CostAndUsage"],
                    "Type": "Static",
                    "ValidationCriteria": {
                        "MustSatisfy": "All rows",
                        "Keyword": "TypeString",
                        "Requirement": {
                            "CheckFunction": "TypeString",
                            "ColumnName": "BillingAccountName"
                        },
                        "Condition": {},
                        "Dependencies": []
                    }
                }
            },
            "CheckFunctions": {
                "TypeString": {
                    "Description": "Validates that column contains string values"
                }
            },
            "ApplicabilityCriteria": {
                "CostAndUsage": "Cost and Usage data validation"
            }
        }
        
        # Initialize with embedded data
        self.spec_rules = SpecRulesFromData(
            rule_data=self.rule_data,
            focus_dataset="CostAndUsage",
            filter_rules="BillingAccountName",
            applicability_criteria_list=["CostAndUsage"]  # Provide the criteria
        )
        self.spec_rules.load()

    def test_rule_pass_scenario_embedded_data(self):
        """Test with valid string data using embedded rules."""
        csv_data = """BillingAccountName
"account-123"
"""
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)
        
        rule_result = results.by_rule_id["BillingAccountName-C-001-M"]
        self.assertTrue(rule_result.get("ok"), f"Rule should PASS but got: {rule_result}")
    
    def test_rule_fail_scenario_embedded_data(self):
        """Test with invalid numeric data using embedded rules."""
        csv_data = """BillingAccountName
123.45
"""
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)
        
        rule_result = results.by_rule_id["BillingAccountName-C-001-M"]
        self.assertFalse(rule_result.get("ok"), f"Rule should FAIL but got: {rule_result}")


if __name__ == '__main__':
    unittest.main()