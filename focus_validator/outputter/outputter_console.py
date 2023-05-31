from tabulate import tabulate

from focus_validator.rules.spec_rules import ValidationResult


class ConsoleOutputter:
    def __init__(self, output_destination):
        self.output_destination = output_destination
        self.result_set = None

    def write(self, result_set: ValidationResult):
        self.result_set = result_set
        print("Checklist:")
        print(tabulate(result_set.checklist, headers='keys', tablefmt='psql'))

        if result_set.failure_cases is not None:
            print("Checks summary:")
            print(tabulate(result_set.failure_cases, headers='keys', tablefmt='psql'))
