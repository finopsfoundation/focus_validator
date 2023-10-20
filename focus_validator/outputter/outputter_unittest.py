import logging
import re
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
            logging.warning(
                "produced output not indent due to lack of support before 3.9"
            )
        else:
            ET.indent(tree)
        return tree


class UnittestOutputter:
    def __init__(self, output_destination):
        self.output_destination = output_destination

    def write(self, result_set):
        # First generate the summary
        result_statuses = {}
        for status in ["passed", "failed", "skipped", "errored"]:
            result_statuses[status] = sum(
                [1 for r in result_set.checklist.values() if r.status.value == status]
            )

        # format the results for processing
        rows = [v.model_dump() for v in result_set.checklist.values()]

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
            for testcase in [r for r in rows if r.get("error", False)]:
                formatter.add_testcase(
                    testsuite="Base",
                    name=testcase["check_name"],
                    result=testcase["status"].value,
                    message=testcase["error"],
                    check_type_name=None,
                )

        # Add the testcases to the testsuites
        added_testsuites = {}
        for testcase in [
            r for r in rows if re.match(r"^FV-[D,M][0-9]{3}-[0-9]{4}$", r["check_name"])
        ]:
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
