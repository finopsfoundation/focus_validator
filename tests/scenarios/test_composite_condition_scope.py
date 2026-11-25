import unittest
import pandas as pd
from helper import SpecRulesFromData
from helper import load_rule_data_from_file


def _leaf(column, check_function, value, must_satisfy):
    return {
        "Function": "Nullability",
        "Reference": column,
        "EntityType": "Column",
        "Notes": "",
        "ModelVersionIntroduced": "1.4",
        "Status": "Active",
        "ApplicabilityCriteria": [],
        "Type": "Static",
        "ValidationCriteria": {
            "MustSatisfy": must_satisfy,
            "Keyword": "MUST",
            "Requirement": {"CheckFunction": check_function, "ColumnName": column, "Value": value},
            "Condition": {},
            "Dependencies": []
        }
    }


class TestCompositeConditionScope(unittest.TestCase):
    """A composite's Condition applies to the rules it references, not to its other Dependencies."""

    def setUp(self):
        self.rule_data = load_rule_data_from_file("base_rule_data.json")
        self.rule_data['ModelDatasets'] = {
            "CostAndUsage": {
                "ModelRules": ["ChargeCategory-C-002-M", "SkuId-C-000-C"]
            }
        }
        self.rule_data["ModelRules"] = {
            # Only listed as a Dependency of the composite below
            "ChargeCategory-C-002-M": _leaf(
                "ChargeCategory", "CheckNotValue", None, "ChargeCategory MUST NOT be null."
            ),
            # Referenced by the composite below
            "SkuId-C-001-C": _leaf(
                "SkuId", "CheckNotValue", None, "SkuId MUST NOT be null."
            ),
            "SkuId-C-000-C": {
                "Function": "Composite",
                "Reference": "SkuId",
                "EntityType": "Column",
                "Notes": "",
                "ModelVersionIntroduced": "1.4",
                "Status": "Active",
                "ApplicabilityCriteria": [],
                "Type": "Static",
                "ValidationCriteria": {
                    "MustSatisfy": "When ChargeCategory is \"Purchase\", SkuId adheres to the following requirements:",
                    "Keyword": "MUST",
                    "Requirement": {
                        "CheckFunction": "AND",
                        "Items": [{"CheckFunction": "CheckModelRule", "ModelRuleId": "SkuId-C-001-C"}]
                    },
                    "Condition": {"CheckFunction": "CheckValue", "ColumnName": "ChargeCategory", "Value": "Purchase"},
                    "Dependencies": ["SkuId-C-001-C", "ChargeCategory-C-002-M"]
                }
            },
        }

        self.spec_rules = SpecRulesFromData(
            rule_data=self.rule_data,
            focus_dataset="CostAndUsage",
            filter_rules=None,
            applicability_criteria_list=["ALL"]
        )
        self.spec_rules.load()

    def test_dependency_keeps_its_own_scope(self):
        df = pd.DataFrame({"ChargeCategory": ["Usage", None], "SkuId": ["SKU-1", "SKU-2"]})
        results = self.spec_rules.validate(focus_data=df)

        rule_result = results.by_rule_id["ChargeCategory-C-002-M"]
        self.assertFalse(rule_result.get("ok"), f"Rule should FAIL but got: {rule_result}")
        self.assertEqual(rule_result["details"]["violations"], 1)

    def test_referenced_rule_inherits_condition(self):
        df = pd.DataFrame({"ChargeCategory": ["Usage", "Purchase"], "SkuId": [None, "SKU-2"]})
        results = self.spec_rules.validate(focus_data=df)

        rule_result = results.by_rule_id["SkuId-C-001-C"]
        self.assertTrue(rule_result.get("ok"), f"Rule should PASS but got: {rule_result}")


if __name__ == '__main__':
    unittest.main()
