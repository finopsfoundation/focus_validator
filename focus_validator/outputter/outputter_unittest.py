import json
import xml.etree.cElementTree as ET
from datetime import datetime, timezone


class UnittestFormatter:
    def __init__(self, name):
        self.name = name
        self.tests = 0
        self.failures = 0
        self.errors = 0
        self.skipped = 0
        self.assertions = 0
        self.time = "0"
        self.timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        self.results = {}

    def add_testsuite(self, name):
        if name not in self.results:
            self.results[name] = {"tests": {}, "time": "0"}

    def add_testcase(self, testsuite, name, result, message):
        if result.lower() == "failed":
            self.failures += 1
        if result.lower() == "skipped":
            self.skipped += 1
        if result.lower() == "error":
            self.errors += 1
        self.tests += 1
        self.assertions += 1

        self.results[testsuite]["tests"][name] = {
            "result": result.lower(),
            "message": message,
        }

    def generate_unittest(self):
        testsuites = ET.Element(
            "testsuites",
            name=self.name,
            tests=str(self.tests),
            failures=str(self.failures),
            errors=str(self.errors),
            skipped=str(self.skipped),
            assertions=str(self.assertions),
            time=str(self.time),
            timestamp=self.timestamp,
        )

        for testsuite in sorted(self.results.keys()):
            ts = ET.SubElement(testsuites, "testsuite", name=testsuite, time="0")
            for testcase in sorted(self.results[testsuite]["tests"].keys()):
                tc = ET.SubElement(ts, "testcase", name=testcase, time="0")
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
        tree = ET.ElementTree(testsuites)
        ET.indent(tree)
        return tree


class UnittestOutputter:
    def __init__(self, output_destination):
        self.output_destination = output_destination

    def write(self, result_set):
        formatter = UnittestFormatter(name="FOCUS Validations")
        result_set.checklist = result_set.checklist.join(
            result_set.checklist["Check Name"]
            .str.rsplit("-", expand=True)
            .rename(columns={0: "FV", 1: "Testsuite", 2: "Testcase"})
        )
        result_json = json.loads(
            result_set.checklist.set_index("Check Name").to_json(orient="index")
        )
        for test in result_json.keys():
            testsuite = f'{result_json[test]["FV"]}-{result_json[test]["Testsuite"]}'
            formatter.add_testsuite(testsuite)
            if result_json[test]["Testcase"]:
                formatter.add_testcase(
                    testsuite=testsuite,
                    name=test,
                    result=result_json[test]["Status"],
                    message=result_json[test]["Friendly Name"],
                )

        tree = formatter.generate_unittest()
        tree.write(self.output_destination, encoding="utf-8", xml_declaration=True)
