import json
import unittest
import pandas as pd
from helper import SpecRulesFromData
from helper import load_rule_data_from_file


def _rule(requirement, must_satisfy):
    return {
        "Function": "Format",
        "Reference": "StringHandling",
        "EntityType": "Column",
        "Notes": "",
        "ModelVersionIntroduced": "1.4",
        "Status": "Active",
        "ApplicabilityCriteria": [],
        "Type": "Static",
        "ValidationCriteria": {
            "MustSatisfy": must_satisfy,
            "Keyword": "MUST",
            "Requirement": requirement,
            "Condition": {},
            "Dependencies": []
        }
    }


class TestFormatStringNonASCII(unittest.TestCase):
    """StringHandling has no character-set requirement, so non-ASCII strings pass."""

    def setUp(self):
        self.rule_data = load_rule_data_from_file("base_rule_data.json")
        self.rule_data['ModelDatasets'] = {
            "CostAndUsage": {
                "ModelRules": ["BillingAccountName-C-002-M", "ContractAppliedObject-O-021-M"]
            }
        }
        self.rule_data["ModelRules"] = {
            "BillingAccountName-C-002-M": _rule(
                {"CheckFunction": "FormatString", "ColumnName": "BillingAccountName"},
                "BillingAccountName MUST conform to StringHandling requirements."
            ),
            "ContractAppliedObject-O-021-M": _rule(
                {"CheckFunction": "JSONFormatString", "ColumnName": "ContractApplied",
                 "Path": "$.Elements[*].ContractId"},
                "\"ContractId\" MUST conform to StringHandling requirements."
            ),
        }

        self.spec_rules = SpecRulesFromData(
            rule_data=self.rule_data,
            focus_dataset="CostAndUsage",
            filter_rules=None,
            applicability_criteria_list=["ALL"]
        )
        self.spec_rules.load()

    def test_non_ascii_strings_pass(self):
        df = pd.DataFrame({
            "BillingAccountName": ["Société Générale", "東京-web-01", "plain-ascii"],
            "ContractApplied": [
                json.dumps({"Elements": [{"ContractId": "Société-2026"}]}, ensure_ascii=False),
                json.dumps({"Elements": [{"ContractId": "契約-0001"}]}, ensure_ascii=False),
                json.dumps({"Elements": [{"ContractId": "CONTRACT-1"}]}),
            ],
        })
        results = self.spec_rules.validate(focus_data=df)

        for rule_id in ("BillingAccountName-C-002-M", "ContractAppliedObject-O-021-M"):
            with self.subTest(rule_id=rule_id):
                rule_result = results.by_rule_id[rule_id]
                self.assertTrue(rule_result.get("ok"), f"Rule should PASS but got: {rule_result}")


if __name__ == '__main__':
    unittest.main()
