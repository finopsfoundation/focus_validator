import unittest
import pandas as pd
from io import StringIO
from helper import SpecRulesFromData
from helper import load_rule_data_from_file

class TestTypeDecimalData(unittest.TestCase):
    """Test column data type decimal."""

    def setUp(self):
        self.rule_data = load_rule_data_from_file("base_rule_data.json")
        self.rule_data['ModelDatasets'] = {
            "CostAndUsage": {
                "ModelRules": ["BilledCost-C-001-M"]
            }
        }
        self.rule_data["ModelRules"] = {
            "BilledCost-C-001-M": {
                "Function": "Type",
                "EntityType": "Column",
                "Reference": "BilledCost",
                "Notes": "",
                "ModelVersionIntroduced": "1.2",
                "Status": "Active",
                "ApplicabilityCriteria": [],
                "Type": "Static",
                "ValidationCriteria": {
                "MustSatisfy": "BilledCost MUST be of type Decimal.",
                "Keyword": "MUST",
                "Requirement": {
                    "CheckFunction": "TypeDecimal",
                    "ColumnName": "BilledCost"
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

    def test_rule_pass_scenario_embedded_data(self):
        """Test with valid string data using embedded rules."""
        csv_data = """BilledCost
0
9.99
10.5
"""
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)
        
        rule_result = results.by_rule_id["BilledCost-C-001-M"]
        self.assertTrue(rule_result.get("ok"), f"Rule should PASS but got: {rule_result}")
    
    def test_rule_fail_scenario_embedded_data(self):
        """Test with invalid numeric data using embedded rules."""
        csv_data = """BilledCost
123.45a
abc
"add"
"""
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)

        rule_result = results.by_rule_id["BilledCost-C-001-M"]
        self.assertFalse(rule_result.get("ok"), f"Rule should FAIL but got: {rule_result}")

        # Check that we have exactly 3 violations (all rows should fail)
        violations = rule_result.get("details", {}).get("violations", 0)
        self.assertEqual(violations, 3, f"Expected 3 violations but got {violations}")

    def test_rule_mix_fail_scenario_embedded_data(self):
        """Test with mixed valid and invalid data using embedded rules."""
        csv_data = """BilledCost
123.45
0.16
"add"
"""
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)

        rule_result = results.by_rule_id["BilledCost-C-001-M"]
        self.assertFalse(rule_result.get("ok"), f"Rule should FAIL but got: {rule_result}")

        # Check that we have exactly 3 violations (all rows should fail)
        violations = rule_result.get("details", {}).get("violations", 0)
        self.assertEqual(violations, 3, f"Expected 3 violations but got {violations}")

if __name__ == '__main__':
    unittest.main()