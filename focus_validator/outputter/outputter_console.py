import pandas as pd
from tabulate import tabulate

from focus_validator.rules.spec_rules import ValidationResult


class ConsoleOutputter:
    def __init__(self, output_destination):
        self.output_destination = output_destination
        self.result_set = None

    @staticmethod
    def __restructure_check_list__(result_set: ValidationResult):
        rows = [v.dict() for v in result_set.checklist.values()]
        for row in rows:
            row['status'] = row['status'].value.title()
        df = pd.DataFrame(rows)
        df = df.rename(
            columns={
                "check_name": "Check Name",
                "dimension": "Dimension",
                "friendly_name": "Friendly Name",
                "error": "Error",
                "status": "Status"
            }
        )
        return df

    def write(self, result_set: ValidationResult):
        self.result_set = result_set

        checklist = self.__restructure_check_list__(result_set)
        print("Checklist:")
        print(tabulate(checklist, headers="keys", tablefmt="psql"))

        if result_set.failure_cases is not None:
            print("Checks summary:")
            print(tabulate(result_set.failure_cases, headers="keys", tablefmt="psql"))
