import json
import unittest
import pandas as pd
from helper import SpecRulesFromData
from helper import load_rule_data_from_file


def _rule(requirement, must_satisfy):
    return {
        "Function": "Format",
        "Reference": "BilledCost",
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


class TestFormatNumericSign(unittest.TestCase):
    """NumericFormat values must not carry a plus sign or a plus-signed exponent."""

    def setUp(self):
        self.rule_data = load_rule_data_from_file("base_rule_data.json")
        self.rule_data['ModelDatasets'] = {
            "CostAndUsage": {"ModelRules": ["BilledCost-C-002-M", "ContractAppliedObject-O-061-M"]}
        }
        self.rule_data["ModelRules"] = {
            "BilledCost-C-002-M": _rule(
                {"CheckFunction": "FormatNumeric", "ColumnName": "BilledCost"},
                "BilledCost MUST conform to NumericFormat requirements."
            ),
            "ContractAppliedObject-O-061-M": _rule(
                {"CheckFunction": "JSONCheckPathNumericFormat", "ColumnName": "ContractApplied",
                 "Path": "$.Elements[*].ContractCommitmentAppliedQuantity"},
                "\"ContractCommitmentAppliedQuantity\" MUST conform to NumericFormat requirements."
            ),
        }
        self.spec_rules = SpecRulesFromData(
            rule_data=self.rule_data,
            focus_dataset="CostAndUsage",
            filter_rules=None,
            applicability_criteria_list=["ALL"]
        )
        self.spec_rules.load()

    def _rule_result(self, rule_id, column, values):
        df = pd.DataFrame({column: values})
        return self.spec_rules.validate(focus_data=df).by_rule_id[rule_id]

    def test_spec_examples_pass(self):
        result = self._rule_result("BilledCost-C-002-M", "BilledCost", ["-100.2", "-3", "4", "35.2E-7", "1.234"])
        self.assertTrue(result.get("ok"), f"Rule should PASS but got: {result}")

    def test_plus_signs_fail(self):
        result = self._rule_result("BilledCost-C-002-M", "BilledCost", ["4", "+333", "35.2E+7"])
        self.assertFalse(result.get("ok"), f"Rule should FAIL but got: {result}")
        self.assertEqual(result["details"]["violations"], 2)

    def test_native_large_double_passes(self):
        result = self._rule_result("BilledCost-C-002-M", "BilledCost", [1e20, 0.0416])
        self.assertTrue(result.get("ok"), f"Rule should PASS but got: {result}")

    def test_json_string_with_plus_sign_fails(self):
        values = [
            json.dumps({"Elements": [{"ContractCommitmentAppliedQuantity": 1.5}]}),
            json.dumps({"Elements": [{"ContractCommitmentAppliedQuantity": "+333"}]}),
        ]
        result = self._rule_result("ContractAppliedObject-O-061-M", "ContractApplied", values)
        self.assertFalse(result.get("ok"), f"Rule should FAIL but got: {result}")
        self.assertEqual(result["details"]["violations"], 1)


if __name__ == '__main__':
    unittest.main()
