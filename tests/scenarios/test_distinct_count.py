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
                "ModelRules": ["ServiceName-C-008-C"]
            }
        }
        self.rule_data["ModelRules"] = {
            "ServiceName-C-008-C": {
                "Function": "Validation",
                "Reference": "ServiceName",
                "EntityType": "Column",
                "Notes": "",
                "ModelVersionIntroduced": "1.2",
                "Status": "Active",
                "ApplicabilityCriteria": [],
                "Type": "Static",
                "ValidationCriteria": {
                "MustSatisfy": "ServiceName SHOULD have one and only one ServiceSubcategory that best aligns with its primary purpose, except when no suitable ServiceSubcategory is available.",
                "Keyword": "SHOULD",
                "Requirement": {
                    "CheckFunction": "CheckDistinctCount",
                    "ColumnAName": "ServiceName",
                    "ColumnBName": "ServiceSubcategory",
                    "ExpectedCount": 1
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
        csv_data = """ServiceName,ServiceSubcategory
"ServiceA","SubcategoryA"
"ServiceB","SubcategoryB"
"ServiceC","SubcategoryC"
"ServiceA","SubcategoryA"
"ServiceB","SubcategoryB"
"ServiceA","SubcategoryA"
"""
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)
        
        # Check rule state
        rule_result = results.by_rule_id["ServiceName-C-008-C"]
        self.assertTrue(rule_result.get("ok"), f"Rule should PASS but got: {rule_result}")
    
    def test_rule_fail_scenario(self):
        """Test failure."""
        csv_data = """ServiceName,ServiceSubcategory
"ServiceA","SubcategoryA"
"ServiceA","SubcategoryB"
"""
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)
        
        # Check rule state
        rule_result = results.by_rule_id["ServiceName-C-008-C"]
        self.assertFalse(rule_result.get("ok"), f"Rule should FAIL but got: {rule_result}")

if __name__ == '__main__':
    unittest.main()
