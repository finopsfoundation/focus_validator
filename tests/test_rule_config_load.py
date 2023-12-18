import os

import pytest

from focus_validator.config_objects import Rule
from focus_validator.rules.spec_rules import SpecRules
from focus_validator.validator import DEFAULT_VERSION_SETS_PATH


def rules_version():
    return sorted([x for x in os.walk(DEFAULT_VERSION_SETS_PATH)][0][1])


@pytest.mark.parametrize("focus_spec_version", rules_version())
def test_rules_load_with_no_errors(focus_spec_version):
    """
    Test loading of rules with no errors
    """
    spec_rules = SpecRules(
        override_filename=None,
        rule_set_path=DEFAULT_VERSION_SETS_PATH,
        rules_version=focus_spec_version,
        column_namespace=None,
    )
    spec_rules.load()

    for rule in spec_rules.rules:
        # ensures that the rule is a Rule object and not InvalidRule
        assert isinstance(rule, Rule)
