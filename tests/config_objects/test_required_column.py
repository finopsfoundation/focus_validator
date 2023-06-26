from unittest import TestCase
from uuid import uuid4

import pandas as pd
from pandera.errors import SchemaErrors

from focus_validator.config_objects import Rule, Override
from focus_validator.config_objects.common import DataTypes, DataTypeCheck
from focus_validator.rules.spec_rules import ValidationResult


class TestRequiredColumn(TestCase):
    def test_load_column_required_config(self):
        rules = [
            Rule.load_yaml(
                "samples/rule_configs/valid_rule_config_column_metadata.yaml"
            ),
            Rule.load_yaml("samples/rule_configs/valid_rule_config.yaml"),
            Rule.load_yaml("samples/rule_configs/valid_rule_config_required.yaml"),
        ]
        schema, _ = Rule.generate_schema(rules=rules)
        self.assertIn("ChargeType", schema.columns)
        self.assertTrue(schema.columns["ChargeType"].required)

    def test_load_column_required_config_but_ignored(self):
        rules = [
            Rule.load_yaml(
                "samples/rule_configs/valid_rule_config_column_metadata.yaml"
            ),
            Rule.load_yaml("samples/rule_configs/valid_rule_config_required.yaml"),
        ]
        schema, _ = Rule.generate_schema(
            rules=rules, override_config=Override(overrides=["FV-D001-0001"])
        )
        self.assertIn("ChargeType", schema.columns)
        self.assertFalse(schema.columns["ChargeType"].required)

    def test_check_summary_has_correct_mappings(self):
        random_column_name = str(uuid4())
        random_test_name = str(uuid4())

        sample_data = pd.read_csv("samples/multiple_failure_examples.csv")
        schema, checklist = Rule.generate_schema(
            rules=[
                Rule(
                    check_id=str(uuid4()),
                    column=random_column_name,
                    check=DataTypeCheck(data_type=DataTypes.STRING),
                ),
                Rule(
                    check_id=random_test_name,
                    column=random_column_name,
                    check="column_required",
                    check_friendly_name="Column required.",
                ),
                Rule.load_yaml(
                    "samples/rule_configs/valid_rule_config_column_metadata.yaml"
                ),
                Rule.load_yaml("samples/rule_configs/valid_rule_config.yaml"),
            ]
        )

        with self.assertRaises(SchemaErrors) as cm:
            schema.validate(sample_data, lazy=True)

        failure_cases = cm.exception.failure_cases
        result = ValidationResult(failure_cases=failure_cases, checklist=checklist)
        result.process_result()

        self.assertEqual(result.failure_cases.shape[0], 4)
        missing_column_errors = result.failure_cases[
            result.failure_cases["Column"] == random_column_name
        ]

        raw_values = missing_column_errors.to_dict()
        self.assertEqual(raw_values["Column"], {1: random_column_name})
        self.assertEqual(raw_values["Check Name"], {1: random_test_name})
        self.assertEqual(
            raw_values["Description"],
            {1: "Column required."},
        )
        self.assertEqual(raw_values["Values"], {1: None})
