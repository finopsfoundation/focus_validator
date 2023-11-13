from unittest import TestCase

from focus_validator.config_objects import InvalidRule
from focus_validator.config_objects.focus_to_pandera_schema_converter import (
    FocusToPanderaSchemaConverter,
)
from focus_validator.outputter.outputter_console import ConsoleOutputter, collapse_occurrence_range
from focus_validator.rules.spec_rules import ValidationResult
from focus_validator.validator import Validator


class TestOutputterConsole(TestCase):
    def test_failure_output(self):
        validator = Validator(
            data_filename="tests/samples/multiple_failure_example_namespaced.csv",
            output_type="console",
            output_destination=None,
            column_namespace="F",
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
                "Column",
                "Friendly Name",
                "Error",
                "Status",
            ],
        )

    def test_collapse_range(self):
        self.assertEqual(
            collapse_occurrence_range([1, 5, 6, 7, 23.0]),
            '1,5-7,23'
        )
        self.assertEqual(
            collapse_occurrence_range(['category', 'category2', 'category3']),
            'category,category2,category3'
        )

    def test_output_with_bad_configs_loaded(self):
        schema, checklist = FocusToPanderaSchemaConverter.generate_pandera_schema(
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
        outputter.write(validation_result)
        self.assertEqual(
            checklist.to_dict(orient="records"),
            [
                {
                    "Check Name": "bad_rule_path",
                    "Check Type": "ERRORED",
                    "Column": "Unknown",
                    "Friendly Name": None,
                    "Error": "ValueError: random-error",
                    "Status": "Errored",
                }
            ],
        )
