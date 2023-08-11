from focus_validator.exceptions import FocusNotImplementedError
from focus_validator.outputter.outputter_console import ConsoleOutputter
from focus_validator.outputter.outputter_unittest import UnittestOutputter
from focus_validator.rules.spec_rules import ValidationResult


class Outputter:
    def __init__(self, output_type, output_destination):
        if output_type == "console":
            self.outputter = ConsoleOutputter(output_destination=output_destination)
        elif output_type == "unittest":
            self.outputter = UnittestOutputter(output_destination=output_destination)
        else:
            raise FocusNotImplementedError("Output type not supported")

    def write(self, result_set: ValidationResult):
        self.outputter.write(result_set)
