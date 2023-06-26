from unittest import TestCase

from focus_validator.config_objects import Rule


class TestLoadBaseRulesOnly(TestCase):
    """
    Ensures column config with only base config can enforce validations.
    """

    def test_load_without_any_subsequent_rules(self):
        rules = [
            Rule.load_yaml(
                "samples/rule_configs/valid_rule_config_column_metadata.yaml"
            )
        ]
        schema, _ = Rule.generate_schema(rules=rules)
        self.assertIn("ChargeType", schema.columns)
