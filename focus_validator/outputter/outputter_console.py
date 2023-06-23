import pandas as pd
from tabulate import tabulate

from focus_validator.rules.spec_rules import ValidationResult


class ConsoleOutputter:
    def __init__(self, output_destination):
        self.output_destination = output_destination
        self.result_set = None

    @staticmethod
    def __generate_summary__(result_set: ValidationResult):
        check_summary = {}

        for rule_check_list_object in result_set.checklist.values():
            status = rule_check_list_object.status
            try:
                check_summary[status.name] += 1
            except KeyError:
                check_summary[status.name] = 1
        return pd.DataFrame(check_summary.items(), columns=["Status", "Count"])

    @staticmethod
    def __restructure_check_list__(result_set: ValidationResult):
        rows = []
        for value in result_set.checklist.values():
            row_obj = value.dict()
            row_obj.update(
                {
                    "check_type": value.rule_ref.validation_config.check_type_friendly_name,
                    "status": row_obj["status"].value.title(),
                }
            )
            rows.append(row_obj)
        df = pd.DataFrame(rows)
        df.rename(
            columns={
                "check_name": "Check Name",
                "check_type": "Check Type",
                "dimension": "Dimension",
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
                "Dimension",
                "Friendly Name",
                "Error",
                "Status",
            ]
        )
        return df

    def write(self, result_set: ValidationResult):
        self.result_set = result_set

        check_summary = self.__generate_summary__(result_set)
        print("Summary:")
        print(tabulate(check_summary, headers="keys", tablefmt="psql"))

        checklist = self.__restructure_check_list__(result_set)
        print("Checklist:")
        print(tabulate(checklist, headers="keys", tablefmt="psql"))

        if result_set.failure_cases is not None:
            print("Checks summary:")
            print(tabulate(result_set.failure_cases, headers="keys", tablefmt="psql"))
