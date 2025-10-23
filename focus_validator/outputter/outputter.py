import logging

from focus_validator.exceptions import FocusNotImplementedError
from focus_validator.outputter.outputter_console import ConsoleOutputter
from focus_validator.outputter.outputter_unittest import UnittestOutputter
from focus_validator.rules.spec_rules import ValidationResults


class Outputter:
    def __init__(self, output_type, output_destination, show_violations=False):
        self.log = logging.getLogger(f"{__name__}.{self.__class__.__qualname__}")
        if output_type == "console":
            self.outputter = ConsoleOutputter(
                output_destination=output_destination, show_violations=show_violations
            )
        elif output_type == "unittest":
            self.outputter = UnittestOutputter(output_destination=output_destination)
        else:
            raise FocusNotImplementedError("Output type not supported")

    def write(self, result_set: ValidationResults):
        self.outputter.write(result_set)
