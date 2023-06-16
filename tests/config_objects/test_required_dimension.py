from unittest import TestCase
from uuid import uuid4

import pandas as pd
from pandera.errors import SchemaErrors

from focus_validator.config_objects import Rule, Override
from focus_validator.config_objects.common import DataTypeConfig, DataTypes
from focus_validator.rules.spec_rules import ValidationResult


class TestRequiredDimension(TestCase):
    def test_load_dimension_required_config_but_ignored(self):
        rules = [
            Rule.load_yaml(
                "samples/rule_configs/valid_rule_config_dimension_metadata_required.yaml"
            )
        ]
        schema, _ = Rule.generate_schema(rules=rules)
        self.assertIn("ChargeType", schema.columns)
        self.assertTrue(schema.columns["ChargeType"].required)

    def test_load_dimension_required_config(self):
        rules = [
            Rule.load_yaml(
                "samples/rule_configs/valid_rule_config_dimension_metadata_required.yaml"
            )
        ]
        schema, _ = Rule.generate_schema(
            rules=rules, override_config=Override(overrides=["ChargeType"])
        )
        self.assertIn("ChargeType", schema.columns)
        self.assertFalse(schema.columns["ChargeType"].required)

    def test_check_summary_has_correct_mappings(self):
        random_dimension_name = str(uuid4())
        random_test_name = str(uuid4())

        sample_data = pd.read_csv("samples/multiple_failure_examples.csv")
        schema, checklist = Rule.generate_schema(
            rules=[
                Rule(
                    check_id=random_test_name,
                    dimension=random_dimension_name,
                    validation_config=DataTypeConfig(
                        data_type=DataTypes.STRING, required=True
                    ),
                ),
                Rule.load_yaml(
                    "samples/rule_configs/valid_rule_config_dimension_metadata.yaml"
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
        missing_dimension_errors = result.failure_cases[
            result.failure_cases["Dimension"] == random_dimension_name
        ]

        raw_values = missing_dimension_errors.to_dict()
        self.assertEqual(raw_values["Dimension"], {1: random_dimension_name})
        self.assertEqual(raw_values["Check Name"], {1: random_test_name})
        self.assertEqual(
            raw_values["Description"],
            {1: "Ensures that dimension is of string type and is present."},
        )
        self.assertEqual(raw_values["Values"], {1: None})
