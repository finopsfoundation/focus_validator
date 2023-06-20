from unittest import TestCase

from focus_validator.validator import Validator


class TestDimensionNamespace(TestCase):
    def test_load_rule_config_with_namespace(self):
        validator = Validator(
            data_filename="samples/multiple_failure_example_namespaced.csv",
            output_type="console",
            output_destination=None,
            dimension_namespace="F",
        )
        validator.load()
        result = validator.spec_rules.validate(focus_data=validator.focus_data)
        self.assertIsNotNone(result.failure_cases)

    def test_load_rule_config_without_namespace(self):
        validator = Validator(
            data_filename="samples/multiple_failure_example_namespaced.csv",
            output_type="console",
            output_destination=None,
        )
        validator.load()
        result = validator.spec_rules.validate(focus_data=validator.focus_data)
        self.assertIsNone(result.failure_cases)
