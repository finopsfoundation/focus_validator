import unittest
import pandas as pd
from helper import SpecRulesFromData
from helper import load_rule_data_from_file


def _rule(rule_id, requirement, must_satisfy):
    return {
        "Function": "Validation",
        "Reference": rule_id.split("-")[0],
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


def _value(column, value):
    return {"CheckFunction": "CheckValue", "ColumnName": column, "Value": value}


class TestORCompositeRowLevel(unittest.TestCase):
    """An OR must hold for every row, not for some row in the dataset."""

    def setUp(self):
        self.rule_data = load_rule_data_from_file("base_rule_data.json")
        self.rule_data['ModelDatasets'] = {
            "CostAndUsage": {
                "ModelRules": ["ChargeCategory-C-003-M", "ServiceSubcategory-C-004-M"]
            }
        }
        self.rule_data["ModelRules"] = {
            "ChargeCategory-C-003-M": _rule(
                "ChargeCategory-C-003-M",
                {"CheckFunction": "OR", "Items": [
                    _value("ChargeCategory", "Usage"),
                    _value("ChargeCategory", "Purchase"),
                ]},
                "ChargeCategory MUST be one of the allowed values."
            ),
            "ServiceSubcategory-C-004-M": _rule(
                "ServiceSubcategory-C-004-M",
                {"CheckFunction": "OR", "Items": [
                    {"CheckFunction": "AND", "Items": [
                        _value("ServiceCategory", "Compute"),
                        _value("ServiceSubcategory", "Virtual Machines"),
                    ]},
                    {"CheckFunction": "AND", "Items": [
                        _value("ServiceCategory", "AI and Machine Learning"),
                        _value("ServiceSubcategory", "Bots"),
                    ]},
                ]},
                "ServiceSubcategory MUST have one and only one parent ServiceCategory."
            ),
        }

        self.spec_rules = SpecRulesFromData(
            rule_data=self.rule_data,
            focus_dataset="CostAndUsage",
            filter_rules=None,
            applicability_criteria_list=["ALL"]
        )
        self.spec_rules.load()

    def _validate(self, rows):
        df = pd.DataFrame(rows, columns=["ChargeCategory", "ServiceCategory", "ServiceSubcategory"])
        return self.spec_rules.validate(focus_data=df).by_rule_id

    def test_valid_rows_pass(self):
        results = self._validate([
            ["Usage", "Compute", "Virtual Machines"],
            ["Purchase", "AI and Machine Learning", "Bots"],
        ])
        for rule_id in ("ChargeCategory-C-003-M", "ServiceSubcategory-C-004-M"):
            with self.subTest(rule_id=rule_id):
                self.assertTrue(results[rule_id].get("ok"), f"Rule should PASS but got: {results[rule_id]}")

    def test_invalid_row_next_to_valid_row_fails(self):
        results = self._validate([
            ["Usage", "Compute", "Virtual Machines"],
            ["Bogus", "Compute", "Bots"],
        ])
        for rule_id in ("ChargeCategory-C-003-M", "ServiceSubcategory-C-004-M"):
            with self.subTest(rule_id=rule_id):
                self.assertFalse(results[rule_id].get("ok"), f"Rule should FAIL but got: {results[rule_id]}")
                self.assertEqual(results[rule_id]["details"]["violations"], 1)


if __name__ == '__main__':
    unittest.main()
