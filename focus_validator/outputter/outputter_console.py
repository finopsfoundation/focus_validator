import pandas as pd
from tabulate import tabulate

from focus_validator.config_objects import Rule
from focus_validator.rules.spec_rules import ValidationResult


class ConsoleOutputter:
    def __init__(self, output_destination):
        self.output_destination = output_destination
        self.result_set = None

    @staticmethod
    def __restructure_check_list__(result_set: ValidationResult):
        rows = []
        for value in result_set.checklist.values():
            if isinstance(value.rule_ref, Rule):
                check_type = value.rule_ref.check_type_friendly_name
            else:
                check_type = "ERRORED"

            row_obj = value.dict()
            row_obj.update(
                {
                    "check_type": check_type,
                    "status": row_obj["status"].value.title(),
                }
            )
            rows.append(row_obj)
        df = pd.DataFrame(rows)
        df.rename(
            columns={
                "check_name": "Check Name",
                "check_type": "Check Type",
                "column": "Column",
                "friendly_name": "Friendly Name",
                "error": "Error",
                "status": "Status",
            },
            inplace=True,
        )
        df = df.reindex(
            columns=[
                "Check Name",
                "Check Type",
                "Column",
                "Friendly Name",
                "Error",
                "Status",
            ]
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
