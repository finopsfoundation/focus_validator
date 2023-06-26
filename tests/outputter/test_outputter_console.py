from unittest import TestCase

from focus_validator.config_objects import Rule, InvalidRule
from focus_validator.outputter.outputter_console import ConsoleOutputter
from focus_validator.rules.spec_rules import ValidationResult
from focus_validator.validator import Validator


class TestOutputterConsole(TestCase):
    def test_failure_output(self):
        validator = Validator(
            data_filename="samples/multiple_failure_example_namespaced.csv",
            output_type="console",
            output_destination=None,
            dimension_namespace="F",
        )
        validator.load()
        result = validator.spec_rules.validate(focus_data=validator.focus_data)

        outputter = ConsoleOutputter(output_destination=None)
        checklist = outputter.__restructure_check_list__(result_set=result)
        self.assertEqual(
            list(checklist.columns),
            [
                "Check Name",
                "Check Type",
                "Dimension",
                "Friendly Name",
                "Error",
                "Status",
            ],
        )

    def test_output_with_bad_configs_loaded(self):
        schema, checklist = Rule.generate_schema(
            rules=[
                InvalidRule(
                    rule_path="bad_rule_path",
                    error="random-error",
                    error_type="ValueError",
                )
            ]
        )

        validation_result = ValidationResult(failure_cases=None, checklist=checklist)
        validation_result.process_result()

        outputter = ConsoleOutputter(output_destination=None)
        checklist = outputter.__restructure_check_list__(result_set=validation_result)
        self.assertEqual(
            checklist.to_dict(orient="records"),
            [
                {
                    "Check Name": "bad_rule_path",
                    "Check Type": "ERRORED",
                    "Dimension": "Unknown",
                    "Friendly Name": None,
                    "Error": "ValueError: random-error",
                    "Status": "Errored",
                }
            ],
        )
