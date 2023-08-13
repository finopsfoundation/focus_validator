from unittest import TestCase

from focus_validator.config_objects import Rule
from focus_validator.config_objects.focus_to_pandera_schema_converter import (
    FocusToPanderaSchemaConverter,
)


class TestLoadBaseRulesOnly(TestCase):
    """
    Ensures column config with only base config can enforce validations.
    """

    def test_load_without_any_subsequent_rules(self):
        rules = [
            Rule.load_yaml(
                "tests/samples/rule_configs/valid_rule_config_column_metadata.yaml"
            )
        ]
        schema, _ = FocusToPanderaSchemaConverter.generate_pandera_schema(rules=rules)
        self.assertIn("ChargeType", schema.columns)
