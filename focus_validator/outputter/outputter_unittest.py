import logging
import sys
import xml.etree.cElementTree as ET
from datetime import datetime, timezone


class UnittestFormatter:
    def __init__(
        self,
        name,
        tests,
        failures,
        errors,
        skipped,
        assertions=None,
        time="0",
        timestamp=None,
    ):
        self.log = logging.getLogger(f"{__name__}.{self.__class__.__qualname__}")
        self.name = name
        self.tests = str(tests)
        self.failures = str(failures)
        self.errors = str(errors)
        self.skipped = str(skipped)
        self.assertions = str(assertions)
        if not self.assertions:
            self.assertions = self.tests
        self.time = time
        self.timestamp = timestamp
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        self.results = {}

    def add_testsuite(self, name, column):
        if name not in self.results:
            self.results[name] = {"tests": {}, "time": "0", "column": column}

    def add_testcase(self, testsuite, name, result, message, check_type_name):
        self.results[testsuite]["tests"][name] = {
            "result": result.lower(),
            "message": message,
            "check_type_name": check_type_name,
        }

    def generate_unittest(self):
        testsuites = ET.Element(
            "testsuites",
            name=self.name,
            tests=self.tests,
            failures=self.failures,
            errors=self.errors,
            skipped=self.skipped,
            assertions=self.assertions,
            time=self.time,
            timestamp=self.timestamp,
        )

        for testsuite in sorted(self.results.keys()):
            ts = ET.SubElement(
                testsuites,
                "testsuite",
                name=f'{testsuite}-{self.results[testsuite]["column"]}',
                time="0",
            )
            for testcase in sorted(self.results[testsuite]["tests"].keys()):
                tc = ET.SubElement(
                    ts,
                    "testcase",
                    name=f"{testcase} :: {self.results[testsuite]['tests'][testcase]['check_type_name']}",
                    time="0",
                )
                if (
                    self.results[testsuite]["tests"][testcase]["result"].lower()
                    == "failed"
                ):
                    ET.SubElement(
                        tc,
                        "failure",
                        name=testcase,
                        message=self.results[testsuite]["tests"][testcase]["message"],
                        type="AssertionError",
                    ).text = "Failed"
                elif (
                    self.results[testsuite]["tests"][testcase]["result"].lower()
                    == "skipped"
                ):
                    ET.SubElement(
                        tc,
                        "skipped",
                        message=self.results[testsuite]["tests"][testcase]["message"],
                    )
                elif (
                    self.results[testsuite]["tests"][testcase]["result"].lower()
                    == "errored"
                ):
                    ET.SubElement(
                        tc,
                        "error",
                        message=self.results[testsuite]["tests"][testcase]["message"],
                    )
        tree = ET.ElementTree(testsuites)
        if sys.version_info < (3, 9):
            self.log.warning(
                "produced output not indent due to lack of support before 3.9"
            )
        else:
            ET.indent(tree)
        return tree


class UnittestOutputter:
    def __init__(self, output_destination):
        self.output_destination = output_destination

    def write(self, result_set):
        # Convert ValidationResults format to expected format
        def _get_status_from_entry(entry):
            """Convert entry to status string."""
            # Handle ChecklistObject format (from tests)
            if hasattr(entry, "status"):
                status_value = (
                    entry.status.value
                    if hasattr(entry.status, "value")
                    else str(entry.status)
                )
                if status_value == "skipped":
                    return "skipped"
                elif status_value == "passed":
                    return "passed"
                elif status_value == "errored":
                    return "errored"
                else:
                    return "failed"

            # Handle ValidationResults format
            details = entry.get("details", {})
            if details.get("skipped"):
                return "skipped"
            elif entry.get("ok"):
                return "passed"
            # Check for error indicators in ValidationResults format
            elif details.get("error") or details.get("missing_columns"):
                return "errored"
            else:
                return "failed"

        def _convert_entry_to_row(rule_id, entry):
            """Convert entry to the format expected by formatter."""
            status = _get_status_from_entry(entry)

            # Handle ChecklistObject format (from tests)
            if hasattr(entry, "check_name"):
                return {
                    "check_name": getattr(entry, "check_name", rule_id),
                    "status": type(
                        "MockStatus", (), {"value": status}
                    )(),  # Mock status object
                    "column_id": getattr(entry, "column_id", "Unknown"),
                    "friendly_name": getattr(entry, "friendly_name", rule_id),
                    "error": (
                        getattr(entry, "error", None)
                        if status in ["failed", "errored"]
                        else None
                    ),
                    "rule_ref": {
                        "check_type_friendly_name": (
                            getattr(
                                entry.rule_ref, "check_type_friendly_name", "Unknown"
                            )
                            if hasattr(entry, "rule_ref")
                            else "Unknown"
                        )
                    },
                }

            # Handle ValidationResults format
            if hasattr(result_set, "rules") and result_set.rules:
                rule = result_set.rules.get(rule_id)
            else:
                rule = None

            details = entry.get("details", {})

            # Extract column/entity name from rule reference
            column_id = "Unknown"
            friendly_name = rule_id
            check_type_friendly_name = "Unknown"

            if rule:
                # Use rule reference as column_id (this is the entity being validated)
                column_id = getattr(rule, "reference", "Unknown")
                # Create a friendly name using rule function and reference
                function = getattr(rule, "function", "Unknown")
                reference = getattr(rule, "reference", "Unknown")
                friendly_name = f"{function} check for {reference}"
                # Use rule function as check type friendly name
                check_type_friendly_name = getattr(rule, "function", "Unknown")

            return {
                "check_name": rule_id,
                "status": type(
                    "MockStatus", (), {"value": status}
                )(),  # Mock status object
                "column_id": column_id,
                "friendly_name": friendly_name,
                "error": (
                    details.get("message") if status in ["failed", "errored"] else None
                ),
                "rule_ref": {"check_type_friendly_name": check_type_friendly_name},
            }

        # Handle both ValidationResults format and legacy mock format
        if hasattr(result_set, "by_rule_id") and isinstance(
            result_set.by_rule_id, dict
        ):
            # New ValidationResults format
            entries = result_set.by_rule_id
        elif hasattr(result_set, "checklist") and isinstance(
            result_set.checklist, dict
        ):
            # Legacy mock format for tests
            entries = result_set.checklist
        else:
            entries = {}

        # First generate the summary
        result_statuses = {}
        for status in ["passed", "failed", "skipped", "errored"]:
            result_statuses[status] = sum(
                [
                    1
                    for entry in entries.values()
                    if _get_status_from_entry(entry) == status
                ]
            )

        # format the results for processing
        rows = [
            _convert_entry_to_row(rule_id, entry) for rule_id, entry in entries.items()
        ]

        # Setup a Formatter and initiate with result totals
        formatter = UnittestFormatter(
            name="FOCUS Validations",
            tests=len(rows),
            failures=result_statuses["failed"],
            errors=result_statuses["errored"],
            skipped=result_statuses["skipped"],
        )

        # If there are any errors load them in first
        if result_statuses["errored"]:
            formatter.add_testsuite(name="Base", column="Unknown")
            for testcase in [r for r in rows if r["status"].value == "errored"]:
                formatter.add_testcase(
                    testsuite="Base",
                    name=testcase["check_name"],
                    result=testcase["status"].value,
                    message=testcase["error"],
                    check_type_name=None,
                )

        # Add the testcases to the testsuites
        added_testsuites = {}
        for testcase in rows:
            if testcase["status"].value == "errored":
                continue
            test_suite_id = testcase["check_name"].rsplit("-", 1)[0]
            if test_suite_id not in added_testsuites:
                formatter.add_testsuite(
                    name=test_suite_id, column=testcase["column_id"]
                )

            formatter.add_testcase(
                testsuite=test_suite_id,
                name=testcase["check_name"],
                result=testcase["status"].value,
                message=testcase["friendly_name"],
                check_type_name=testcase["rule_ref"]["check_type_friendly_name"],
            )

        tree = formatter.generate_unittest()
        tree.write(self.output_destination, encoding="utf-8", xml_declaration=True)
