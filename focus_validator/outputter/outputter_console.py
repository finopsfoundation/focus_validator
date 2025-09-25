import math

import pandas as pd
import logging

from focus_validator.config_objects import Rule
from focus_validator.rules.spec_rules import ValidationResult


class ConsoleOutputter:
    def __init__(self, output_destination):
        self.log = logging.getLogger(f"{__name__}.{self.__class__.__qualname__}")
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

        status_groups = {
            "passed": [],
            "failed": [],
            "skipped": [],
            "errored": [],
            "pending": []
        }

        for item in result_set.checklist.values():
            status_groups[item.status.value].append(item)


        # Show all results by status
        for status in ["passed", "failed", "skipped", "errored", "pending"]:
            items = status_groups[status]
            print(status, "(" + str(len(items)) + ")")
            for item in items:
                if status == "failed" and item.error:
                    print(item.check_name, "FAIL")
                    print("     Description", item.error)
                    print("     Error", item.friendly_name)
                else:
                    print(item.check_name, status.upper())

        if len(status_groups['failed']) > 0 or len(status_groups['errored']) > 0:
            print("*********************")
            print("Validation failed!")
            print("*********************")
        else:
            print("*********************")
            print("Validation succeeded.")
            print("*********************")


def collapse_occurrence_range(occurrence_range: list):
    start = None
    i = None
    collapsed = []

    # Edge case
    if len(occurrence_range) == 1:
        if isinstance(occurrence_range[0], float) and math.isnan(occurrence_range[0]):
            return ""
        if occurrence_range[0] is None:
            return ""

    for n in sorted(occurrence_range):
        if not isinstance(n, int) and not (isinstance(n, float) and not math.isnan(n)):
            return ",".join([str(x) for x in occurrence_range])
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

    return ",".join(collapsed)
