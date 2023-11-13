import math

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

            row_obj = value.model_dump()
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
                "column_id": "Column",
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
            aggregated_failures = result_set.failure_cases.groupby(
                by=["Check Name", "Column", "Description"], as_index=False
            ).aggregate(lambda x: collapse_occurrence_range(x.unique().tolist()))

            print("Checks summary:")
            print(
                tabulate(
                    tabular_data=aggregated_failures,  # type: ignore
                    headers="keys",
                    tablefmt="psql",
                )
            )


def collapse_occurrence_range(occurrence_range: list):
    start = None
    i = None
    collapsed = []
    for n in sorted(occurrence_range):
        if not isinstance(n, int) and not (isinstance(n, float) and not math.isnan(n)):
            return occurrence_range
        elif i is None:
            start = i = int(n)
        elif n == i + 1:
            i = int(n)
        elif i:
            if i == start:
                collapsed.append(f"{start}")
            else:
                collapsed.append(f"{start}-{i}")
            start = i = int(n)

    if start is not None:
        if i == start:
            collapsed.append(f"{start}")
        else:
            collapsed.append(f"{start}-{i}")

    return collapsed
