import unittest
import pandas as pd
from helper import SpecRulesFromData
from helper import load_rule_data_from_file


class TestNonApplicableColumnRules(unittest.TestCase):
    """Rules of a column whose presence rule is not applicable, with 1.3/1.4-style rule IDs."""

    def setUp(self):
        self.rule_data = load_rule_data_from_file("base_rule_data.json")
        self.rule_data['ModelDatasets'] = {
            "CostAndUsage": {
                "ModelRules": ["CAU-CostAndUsage-D-065-C", "CAU-CapacityReservationId-C-001-M"]
            }
        }
        self.rule_data["ModelRules"] = {
            "CAU-CostAndUsage-D-065-C": {
                "Function": "Presence",
                "Reference": "CapacityReservationId",
                "EntityType": "Dataset",
                "Notes": "",
                "ModelVersionIntroduced": "1.2",
                "Status": "Active",
                "ApplicabilityCriteria": ["CAPACITY_RESERVATION_SUPPORTED"],
                "Type": "Static",
                "ValidationCriteria": {
                    "MustSatisfy": "CostAndUsage MUST include CapacityReservationId when the service provider supports capacity reservations.",
                    "Keyword": "MUST",
                    "Requirement": {"CheckFunction": "ColumnPresent", "ColumnName": "CapacityReservationId"},
                    "Condition": {},
                    "Dependencies": []
                }
            },
            "CAU-CapacityReservationId-C-001-M": {
                "Function": "Type",
                "Reference": "CapacityReservationId",
                "EntityType": "Column",
                "Notes": "",
                "ModelVersionIntroduced": "1.2",
                "Status": "Active",
                "ApplicabilityCriteria": [],
                "Type": "Static",
                "ValidationCriteria": {
                    "MustSatisfy": "CapacityReservationId MUST be of type String.",
                    "Keyword": "MUST",
                    "Requirement": {"CheckFunction": "TypeString", "ColumnName": "CapacityReservationId"},
                    "Condition": {},
                    "Dependencies": []
                }
            },
        }

        # CAPACITY_RESERVATION_SUPPORTED is not declared
        self.spec_rules = SpecRulesFromData(
            rule_data=self.rule_data,
            focus_dataset="CostAndUsage",
            filter_rules=None,
            applicability_criteria_list=[]
        )
        self.spec_rules.load()

    def _column_rule(self, df):
        results = self.spec_rules.validate(focus_data=df)
        return results.by_rule_id["CAU-CapacityReservationId-C-001-M"]

    def test_absent_column_rules_are_skipped(self):
        rule_result = self._column_rule(pd.DataFrame({"ChargeCategory": ["Usage"]}))
        self.assertTrue(rule_result.get("ok"), f"Rule should be SKIPPED but got: {rule_result}")
        self.assertTrue(rule_result["details"].get("skipped"))

    def test_present_column_rules_are_skipped(self):
        rule_result = self._column_rule(pd.DataFrame({"CapacityReservationId": [123]}))
        self.assertTrue(rule_result.get("ok"), f"Rule should be SKIPPED but got: {rule_result}")
        self.assertTrue(rule_result["details"].get("skipped"))


if __name__ == '__main__':
    unittest.main()
