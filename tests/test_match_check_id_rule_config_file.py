import os
from pathlib import Path
from unittest import TestCase

from focus_validator.config_objects import Rule


class TestMatchCheckIdRuleConfigFile(TestCase):
    def test_match_check_id_in_base_definitions(self):
        for root, dirs, files in os.walk(
            "focus_validator/rules/base_rule_definitions/", topdown=False
        ):
            for name in files:
                rule_path = os.path.join(root, name)
                rule = Rule.load_yaml(rule_path=rule_path)
                self.assertIsInstance(rule, Rule)
                self.assertEqual(rule.check_id, Path(name).stem)
