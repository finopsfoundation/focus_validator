from unittest import TestCase

from focus_validator.exceptions import UnsupportedVersion
from focus_validator.rules.spec_rules import SpecRules


class TestSpecRulesUnsupportedVersion(TestCase):
    def test_load_unsupported_version(self):
        with self.assertRaises(UnsupportedVersion) as cm:
            SpecRules(
                rule_set_path="focus_validator/rules",
                rules_file_prefix="cr-",
                rules_version="0.1",
                rules_file_suffix=".json",
                focus_dataset="CostAndUsage",
                filter_rules=None,
                rules_force_remote_download=False,
                allow_draft_releases=False,
                allow_prerelease_releases=False,
                column_namespace=None,
            )
        self.assertIn("FOCUS version 0.1 not supported.", str(cm.exception))
