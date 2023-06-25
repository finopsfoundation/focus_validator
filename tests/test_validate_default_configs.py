import os
from unittest import TestCase
import hypothesis
from focus_validator.config_objects import Rule
from focus_validator.rules.spec_rules import SpecRules


class TestValidateDefaultConfigs(TestCase):
    def __iter_version_set__(self):
        for root, dirs, _ in os.walk(
            "focus_validator/rules/version_sets", topdown=False
        ):
            for version in dirs:
                spec_rules = SpecRules(
                    override_filename=None,
                    rule_set_path="focus_validator/rules/version_sets",
                    rules_version=version,
                    dimension_namespace=None,
                )
                spec_rules.load_rules()
                schema = Rule.generate_schema(rules=spec_rules.rules)[0]

                @hypothesis.given(schema.strategy(size=5))
                def generate_sample(dataframe):
                    print(dataframe)

                print(spec_rules)
                generate_sample()
        raise ValueError

    def test_default_rules_with_sample_data(self):
        for version in self.__iter_version_set__():
            print(version)

    def test_dimensions_consistent_in_test_suite(self):
        pass

    def test_suite_have_consecutive_check_id(self):
        pass
