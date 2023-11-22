from unittest import TestCase

from focus_validator.config_objects import Rule
from focus_validator.config_objects.common import ChecklistObjectStatus
from focus_validator.config_objects.focus_to_pandera_schema_converter import (
    FocusToPanderaSchemaConverter,
)
from focus_validator.config_objects.rule import InvalidRule


class TestLoadBadRuleConfigFile(TestCase):
    def test_load_empty_config(self):
        rule = Rule.load_yaml(
            "tests/samples/rule_configs/bad_rule_config_empty_file.yaml"
        )
        self.assertIsInstance(rule, InvalidRule)

    def test_load_incomplete_config(self):
        rule = Rule.load_yaml(
            "tests/samples/rule_configs/bad_rule_config_missing_check.yaml"
        )
        self.assertIsInstance(rule, InvalidRule)

    def test_load_bad_yaml(self):
        rule = Rule.load_yaml(
            "tests/samples/rule_configs/bad_rule_config_invalid_yaml.yaml"
        )
        self.assertIsInstance(rule, InvalidRule)

    def test_load_valid_rule(self):
        rule = Rule.load_yaml("tests/samples/rule_configs/valid_rule_config.yaml")
        self.assertIsInstance(rule, Rule)

    def test_load_schema(self):
        rules = [
            Rule.load_yaml(
                "tests/samples/rule_configs/bad_rule_config_empty_file.yaml"
            ),
            Rule.load_yaml(
                "tests/samples/rule_configs/bad_rule_config_missing_check.yaml"
            ),
            Rule.load_yaml("tests/samples/rule_configs/valid_rule_config.yaml"),
            Rule.load_yaml(
                "tests/samples/rule_configs/valid_rule_config_column_metadata.yaml"
            ),
        ]

        _, checklist = FocusToPanderaSchemaConverter.generate_pandera_schema(
            rules=rules, override_config=None
        )

        self.assertEqual(
            checklist["bad_rule_config_empty_file"].status, ChecklistObjectStatus.ERRORED
        )

        self.assertEqual(checklist["valid_rule_config_column_metadata"].column_id, "ChargeType")
        self.assertEqual(checklist["valid_rule_config_column_metadata"].status, ChecklistObjectStatus.PENDING)
        self.assertIsNone(checklist["valid_rule_config_column_metadata"].error)
        self.assertIsNotNone(checklist["valid_rule_config_column_metadata"].friendly_name)
        self.assertEqual(
            checklist["valid_rule_config_column_metadata"].friendly_name, "Ensures that column is of string type."
        )

        for errored_checks in [
            'bad_rule_config_empty_file',
            'bad_rule_config_missing_check'
        ]:
            self.assertEqual(
                checklist[errored_checks].status, ChecklistObjectStatus.ERRORED
            )
            self.assertIsNotNone(checklist[errored_checks].error)
            self.assertIsNone(checklist[errored_checks].friendly_name)
            self.assertEqual(checklist[errored_checks].column_id, "Unknown")

    def test_load_schema_without_valid_column_metadata(self):
        rules = [
            Rule.load_yaml(
                "tests/samples/rule_configs/bad_rule_config_empty_file.yaml"
            ),
            Rule.load_yaml(
                "tests/samples/rule_configs/bad_rule_config_missing_check.yaml"
            ),
            Rule.load_yaml("tests/samples/rule_configs/valid_rule_config.yaml"),
        ]

        _, checklist = FocusToPanderaSchemaConverter.generate_pandera_schema(
            rules=rules, override_config=None
        )
        self.assertEqual(
            checklist["bad_rule_config_missing_check"].status, ChecklistObjectStatus.ERRORED
        )
        self.assertRegex(
            checklist["bad_rule_config_missing_check"].error,
            "ValidationError:.*",
        )
        self.assertIsNotNone(checklist["valid_rule_config"].friendly_name)
        self.assertEqual(checklist["valid_rule_config"].column_id, "ChargeType")
