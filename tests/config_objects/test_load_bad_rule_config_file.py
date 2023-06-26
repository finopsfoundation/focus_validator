from unittest import TestCase

from focus_validator.config_objects import Rule
from focus_validator.config_objects.common import ChecklistObjectStatus
from focus_validator.config_objects.rule import InvalidRule


class TestLoadBadRuleConfigFile(TestCase):
    def test_load_empty_config(self):
        rule = Rule.load_yaml("samples/rule_configs/bad_rule_config_empty_file.yaml")
        self.assertIsInstance(rule, InvalidRule)

    def test_load_incomplete_config(self):
        rule = Rule.load_yaml("samples/rule_configs/bad_rule_config_missing_check.yaml")
        self.assertIsInstance(rule, InvalidRule)

    def test_load_bad_yaml(self):
        rule = Rule.load_yaml("samples/rule_configs/bad_rule_config_invalid_yaml.yaml")
        self.assertIsInstance(rule, InvalidRule)

    def test_load_valid_rule(self):
        rule = Rule.load_yaml("samples/rule_configs/valid_rule_config.yaml")
        self.assertIsInstance(rule, Rule)

    def test_load_schema(self):
        rules = [
            Rule.load_yaml("samples/rule_configs/bad_rule_config_empty_file.yaml"),
            Rule.load_yaml("samples/rule_configs/bad_rule_config_missing_check.yaml"),
            Rule.load_yaml("samples/rule_configs/valid_rule_config.yaml"),
            Rule.load_yaml(
                "samples/rule_configs/valid_rule_config_column_metadata.yaml"
            ),
        ]

        _, checklist = Rule.generate_schema(rules=rules, override_config=None)
        self.assertEqual(
            checklist["FV-D001-0001"].status, ChecklistObjectStatus.PENDING
        )
        self.assertIsNone(checklist["FV-D001-0001"].error)
        self.assertIsNotNone(checklist["FV-D001-0001"].friendly_name)

        self.assertEqual(checklist["FV-D001"].column, "ChargeType")
        self.assertEqual(checklist["FV-D001"].status, ChecklistObjectStatus.PENDING)
        self.assertIsNone(checklist["FV-D001"].error)
        self.assertIsNotNone(checklist["FV-D001"].friendly_name)
        self.assertEqual(
            checklist["FV-D001"].friendly_name, "Ensures that column is of string type."
        )

        for errored_file_paths in [
            "samples/rule_configs/bad_rule_config_empty_file.yaml",
            "samples/rule_configs/bad_rule_config_missing_check.yaml",
        ]:
            self.assertEqual(
                checklist[errored_file_paths].status, ChecklistObjectStatus.ERRORED
            )
            self.assertIsNotNone(checklist[errored_file_paths].error)
            self.assertIsNone(checklist[errored_file_paths].friendly_name)
            self.assertEqual(checklist[errored_file_paths].column, "Unknown")

    def test_load_schema_without_valid_column_metadata(self):
        rules = [
            Rule.load_yaml("samples/rule_configs/bad_rule_config_empty_file.yaml"),
            Rule.load_yaml("samples/rule_configs/bad_rule_config_missing_check.yaml"),
            Rule.load_yaml("samples/rule_configs/valid_rule_config.yaml"),
        ]

        _, checklist = Rule.generate_schema(rules=rules, override_config=None)
        self.assertEqual(
            checklist["FV-D001-0001"].status, ChecklistObjectStatus.ERRORED
        )
        self.assertEqual(
            checklist["FV-D001-0001"].error,
            "ConfigurationError: No configuration found for column.",
        )
        self.assertIsNotNone(checklist["FV-D001-0001"].friendly_name)
        self.assertEqual(checklist["FV-D001-0001"].column, "ChargeType")
