import unittest
import pandas as pd
from io import StringIO
from helper import load_rule_data_from_file
from helper import SpecRulesFromData

class TestSameValue(unittest.TestCase):
    """Test same value rule."""
    
    def setUp(self):
        self.rule_data = load_rule_data_from_file("base_rule_data.json")
        self.rule_data['ModelDatasets'] = {
            "CostAndUsage": {
                "ModelRules": ["SkuPriceId-C-011-O"]
            }
        }
        self.rule_data["ModelRules"] = {
            "SkuPriceId-C-011-O": {
                "Function": "Validation",
                "Reference": "SkuPriceId",
                "EntityType": "Column",
                "Notes": "",
                "ModelVersionIntroduced": "1.2",
                "Status": "Active",
                "ApplicabilityCriteria": [],
                "Type": "Static",
                "ValidationCriteria": {
                "MustSatisfy": "SkuPriceId MAY equal SkuId.",
                "Keyword": "MAY",
                "Requirement": {
                    "CheckFunction": "CheckSameValue",
                    "ColumnAName": "SkuPriceId",
                    "ColumnBName": "SkuId"
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
        """Test that MAY/OPTIONAL rules are skipped (not validated)."""
        csv_data = """SkuPriceId,SkuId
"abc123","abc123"
"""
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)
        
        # Check rule state - MAY rules should be skipped
        rule_result = results.by_rule_id["SkuPriceId-C-011-O"]
        self.assertTrue(rule_result.get("ok"), f"Rule should be skipped (ok=True) but got: {rule_result}")
        self.assertTrue(rule_result.get("details", {}).get("skipped"), f"Rule should be marked as skipped but got: {rule_result}")
        self.assertIn("MAY/OPTIONAL", rule_result.get("details", {}).get("reason", ""), f"Skip reason should mention MAY/OPTIONAL but got: {rule_result}")
        
        # Check KeyWord context
        rule_obj = results.rules["SkuPriceId-C-011-O"]
        keyword = rule_obj.validation_criteria.keyword
        self.assertEqual(keyword, "MAY", f"Expected MAY keyword but got {keyword}")
        
    
    def test_rule_fail_scenario(self):
        """Test that MAY/OPTIONAL rules are skipped even when data would fail validation."""
        csv_data = """SkuPriceId,SkuId
"abc123","abc1234"
"""
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)
        
        # Check rule state - MAY rules should be skipped, not failed
        rule_result = results.by_rule_id["SkuPriceId-C-011-O"]
        self.assertTrue(rule_result.get("ok"), f"Rule should be skipped (ok=True) but got: {rule_result}")
        self.assertTrue(rule_result.get("details", {}).get("skipped"), f"Rule should be marked as skipped but got: {rule_result}")
        self.assertIn("MAY/OPTIONAL", rule_result.get("details", {}).get("reason", ""), f"Skip reason should mention MAY/OPTIONAL but got: {rule_result}")

if __name__ == '__main__':
    unittest.main()
