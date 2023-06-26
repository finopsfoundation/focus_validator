from unittest import TestCase

from focus_validator.exceptions import UnsupportedVersion
from focus_validator.rules.spec_rules import SpecRules


class TestSpecRulesUnsupportedVersion(TestCase):
    def test_load_unsupported_version(self):
        with self.assertRaises(UnsupportedVersion) as cm:
            SpecRules(
                column_namespace=None,
                rule_set_path="focus_validator/rules/version_sets",
                rules_version="0.1",
                override_filename=None,
            )
        self.assertEqual("FOCUS version 0.1 not supported.", str(cm.exception))
