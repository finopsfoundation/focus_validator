import unittest
import pandas as pd
from io import StringIO
from helper import load_rule_data_from_file
from helper import SpecRulesFromData

class TestColumnPresence(unittest.TestCase):
    """Test column presence rule."""
    
    def setUp(self):
        self.rule_data = load_rule_data_from_file("base_rule_data.json")
        self.rule_data['ModelDatasets'] = {
            "CostAndUsage": {
                "ModelRules": ["CostAndUsage-D-004-C"]
            }
        }
        self.rule_data["ModelRules"] = {
            "CostAndUsage-D-004-C": {
                "Function": "Presence",
                "Reference": "ListUnitPrice",
                "EntityType": "Dataset",
                "Notes": "",
                "ModelVersionIntroduced": "1.2",
                "Status": "Active",
                "ApplicabilityCriteria": [
                "PUBLIC_PRICE_LIST_SUPPORTED"
                ],
                "Type": "Static",
                "ValidationCriteria": {
                "MustSatisfy": "ListUnitPrice MUST be present in a FOCUS dataset when the provider publishes unit prices exclusive of discounts.",
                "Keyword": "MUST",
                "Requirement": {
                    "CheckFunction": "ColumnPresent",
                    "ColumnName": "ListUnitPrice"
                },
                "Condition": {},
                "Dependencies": []
                }
            }
        }

    def test_rule_pass_with_applicability_scenario(self):
        """Test pass."""
        self.spec_rules = SpecRulesFromData(
            rule_data=self.rule_data,
            focus_dataset="CostAndUsage",
            filter_rules=None,
            applicability_criteria_list=["ALL"]
        )
        self.spec_rules.load()

        csv_data = """ListUnitPrice
12.10
"""
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)
        
        # Check rule state
        rule_result = results.by_rule_id["CostAndUsage-D-004-C"]
        self.assertTrue(rule_result.get("ok"), f"Rule should PASS but got: {rule_result}")
    
    def test_rule_pass_without_applicability_scenario(self):
        """Test pass."""
        self.spec_rules = SpecRulesFromData(
            rule_data=self.rule_data,
            focus_dataset="CostAndUsage",
            filter_rules=None,
            applicability_criteria_list=[]
        )
        self.spec_rules.load()

        csv_data = """ListUnitPrice
12.10
"""
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)
        
        # Check rule state
        rule_result = results.by_rule_id["CostAndUsage-D-004-C"]
        self.assertTrue(rule_result.get("ok"), f"Rule should PASS (skipped) but got: {rule_result}")
        # Assert that the rule was skipped due to missing applicability criteria
        self.assertTrue(rule_result.get("details", {}).get("skipped", False), 
                       f"Rule should be skipped when no applicability criteria provided but got: {rule_result}")
        self.assertEqual(rule_result.get("details", {}).get("reason"), "Rule skipped - not applicable to current dataset or configuration",
                        f"Expected 'Rule skipped - not applicable to current dataset or configuration' reason but got: {rule_result}")

    def test_rule_pass_without_applicability_scenario2(self):
        """Test pass."""
        self.spec_rules = SpecRulesFromData(
            rule_data=self.rule_data,
            focus_dataset="CostAndUsage",
            filter_rules=None,
            applicability_criteria_list=[]
        )
        self.spec_rules.load()

        csv_data = """BilledCost
12.10
"""
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)
        
        # Check rule state
        rule_result = results.by_rule_id["CostAndUsage-D-004-C"]
        self.assertTrue(rule_result.get("ok"), f"Rule should PASS (skipped) but got: {rule_result}")
        # Assert that the rule was skipped due to missing applicability criteria
        self.assertTrue(rule_result.get("details", {}).get("skipped", False), 
                       f"Rule should be skipped when no applicability criteria provided but got: {rule_result}")
        self.assertEqual(rule_result.get("details", {}).get("reason"), "Rule skipped - not applicable to current dataset or configuration",
                        f"Expected 'Rule skipped - not applicable to current dataset or configuration' reason but got: {rule_result}")

    def test_rule_fail_scenario(self):
        """Test failure."""
        self.spec_rules = SpecRulesFromData(
            rule_data=self.rule_data,
            focus_dataset="CostAndUsage",
            filter_rules=None,
            applicability_criteria_list=["ALL"]
        )
        self.spec_rules.load()

        csv_data = """BillingAccountName
MyAccountName
"""
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)
        
        # Check rule state
        rule_result = results.by_rule_id["CostAndUsage-D-004-C"]
        self.assertFalse(rule_result.get("ok"), f"Rule should FAIL but got: {rule_result}")

if __name__ == '__main__':
    unittest.main()
