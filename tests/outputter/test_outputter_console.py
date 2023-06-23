from unittest import TestCase

import pandas as pd

from focus_validator.outputter.outputter_console import ConsoleOutputter
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

    def test_summary_output(self):
        validator = Validator(
            data_filename="samples/multiple_failure_example_namespaced.csv",
            output_type="console",
            output_destination=None,
            dimension_namespace="F",
        )
        validator.load()
        result = validator.spec_rules.validate(focus_data=validator.focus_data)

        outputter = ConsoleOutputter(output_destination=None)
        summary_output = outputter.__generate_summary__(result_set=result)
        self.assertIsInstance(summary_output, pd.DataFrame)
        self.assertEqual(
            list(summary_output.columns),
            [
                "Status",
                "Count",
            ],
        )
