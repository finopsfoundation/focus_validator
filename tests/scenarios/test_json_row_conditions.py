import json
import unittest
import pandas as pd
from helper import SpecRulesFromData
from helper import load_rule_data_from_file


class TestJSONRequirementRowConditions(unittest.TestCase):
    """JSON path requirements only check rows that match the rule Condition."""

    def setUp(self):
        self.rule_data = load_rule_data_from_file("base_rule_data.json")
        self.rule_data['ModelDatasets'] = {
            "CostAndUsage": {
                "ModelRules": ["ContractAppliedObject-O-039-C"]
            }
        }
        self.rule_data["ModelRules"] = {
            "ContractAppliedObject-O-039-C": {
                "Function": "Validation",
                "Reference": "ContractAppliedObject",
                "EntityType": "Column",
                "Notes": "",
                "ModelVersionIntroduced": "1.3",
                "Status": "Active",
                "ApplicabilityCriteria": [],
                "Type": "Static",
                "ValidationCriteria": {
                    "MustSatisfy": "\"ContractCommitmentId\" MUST be equal to ResourceId when ChargeCategory is \"Purchase\".",
                    "Keyword": "MUST",
                    "Requirement": {
                        "CheckFunction": "JSONCheckPathSameValue",
                        "ColumnAName": "ContractApplied",
                        "PathA": "$.Elements[*].ContractCommitmentId",
                        "ColumnBName": "ResourceId",
                        "PathB": None
                    },
                    "Condition": {
                        "CheckFunction": "CheckValue",
                        "ColumnName": "ChargeCategory",
                        "Value": "Purchase"
                    },
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

    def _validate(self, charge_category):
        contract_applied = json.dumps({"Elements": [{"ContractCommitmentId": "CC-1"}]})
        df = pd.DataFrame({
            "ChargeCategory": [charge_category],
            "ResourceId": ["i-123"],
            "ContractApplied": [contract_applied],
        })
        results = self.spec_rules.validate(focus_data=df)
        return results.by_rule_id["ContractAppliedObject-O-039-C"]

    def test_row_outside_condition_is_not_checked(self):
        rule_result = self._validate("Usage")
        self.assertTrue(rule_result.get("ok"), f"Rule should PASS but got: {rule_result}")

    def test_row_matching_condition_is_checked(self):
        rule_result = self._validate("Purchase")
        self.assertFalse(rule_result.get("ok"), f"Rule should FAIL but got: {rule_result}")
        self.assertEqual(rule_result["details"]["violations"], 1)


if __name__ == '__main__':
    unittest.main()
