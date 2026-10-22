import unittest
import polars as pl
from helper import SpecRulesFromData
from helper import load_rule_data_from_file


def _rule(check_function, keyword_text):
    return {
        "Function": "Format" if check_function == "FormatJSON" else "Type",
        "Reference": "Tags",
        "EntityType": "Column",
        "Notes": "",
        "ModelVersionIntroduced": "1.4",
        "Status": "Active",
        "ApplicabilityCriteria": [],
        "Type": "Static",
        "ValidationCriteria": {
            "MustSatisfy": keyword_text,
            "Keyword": "MUST",
            "Requirement": {"CheckFunction": check_function, "ColumnName": "Tags"},
            "Condition": {},
            "Dependencies": []
        }
    }


class TestJSONStructColumns(unittest.TestCase):
    """FormatJSON and TypeJSON accept STRUCT columns (e.g. Parquet maps) as JSON."""

    def setUp(self):
        self.rule_data = load_rule_data_from_file("base_rule_data.json")
        self.rule_data['ModelDatasets'] = {
            "CostAndUsage": {
                "ModelRules": ["Tags-C-001-M", "Tags-C-002-M"]
            }
        }
        self.rule_data["ModelRules"] = {
            "Tags-C-001-M": _rule("FormatJSON", "Tags MUST be valid JSON format."),
            "Tags-C-002-M": _rule("TypeJSON", "Tags MUST be of type JSON."),
        }

        self.spec_rules = SpecRulesFromData(
            rule_data=self.rule_data,
            focus_dataset="CostAndUsage",
            filter_rules=None,
            applicability_criteria_list=["ALL"]
        )
        self.spec_rules.load()

    def test_struct_column_passes(self):
        df = pl.DataFrame({"Tags": [{"env": "prod", "team": "ml"}, None]})
        results = self.spec_rules.validate(focus_data=df)

        for rule_id in ("Tags-C-001-M", "Tags-C-002-M"):
            with self.subTest(rule_id=rule_id):
                rule_result = results.by_rule_id[rule_id]
                self.assertTrue(rule_result.get("ok"), f"Rule should PASS but got: {rule_result}")

    def test_invalid_json_text_fails(self):
        df = pl.DataFrame({"Tags": ['{"env": "prod"}', '{bad']})
        results = self.spec_rules.validate(focus_data=df)

        for rule_id in ("Tags-C-001-M", "Tags-C-002-M"):
            with self.subTest(rule_id=rule_id):
                rule_result = results.by_rule_id[rule_id]
                self.assertFalse(rule_result.get("ok"), f"Rule should FAIL but got: {rule_result}")
                self.assertEqual(rule_result["details"]["violations"], 1)


if __name__ == '__main__':
    unittest.main()
