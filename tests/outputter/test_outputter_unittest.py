import io
import xml.etree.cElementTree as ET
from random import randint
from unittest import TestCase
from uuid import uuid4

from focus_validator.config_objects import InvalidRule, Rule
from focus_validator.config_objects.common import DataTypeCheck, DataTypes
from focus_validator.config_objects.focus_to_pandera_schema_converter import (
    FocusToPanderaSchemaConverter,
)
from focus_validator.outputter.outputter_unittest import UnittestOutputter
from focus_validator.rules.spec_rules import ValidationResult


# noinspection DuplicatedCode
class TestOutputterUnittest(TestCase):
    def test_unittest_output_all_valid_rules(self):
        random_check_id = f"FV-D00{randint(0, 9)}"
        random_column_id = str(uuid4())

        rules = [
            Rule(
                check_id=f"{random_check_id}-0001",
                column_id=random_column_id,
                check=DataTypeCheck(data_type=DataTypes.DECIMAL),
            ),
            Rule(
                check_id=f"{random_check_id}-0002",
                column_id=random_column_id,
                check="column_required",
            ),
        ]

        _, checklist = FocusToPanderaSchemaConverter.generate_pandera_schema(
            rules=rules
        )
        result = ValidationResult(checklist=checklist)
        result.process_result()

        buffer = io.BytesIO()
        outputter = UnittestOutputter(output_destination=buffer)
        outputter.write(result_set=result)

        buffer.seek(0)
        output = buffer.read()
        testsuites = ET.fromstring(output)
        self.assertEqual(len(testsuites), 1)  # assert one column in sample rules config
        self.assertEqual(testsuites.get("name"), "FOCUS Validations")
        for testsuite in testsuites:
            self.assertEqual(
                testsuite.get("name"), f"{random_check_id}-{random_column_id}"
            )
            self.assertEqual(
                len(testsuite), 2
            )  # assert two tests in sample rules config
            self.assertEqual(
                testsuite[0].get("name"), f"{random_check_id}-0001 :: DataTypeCheck"
            )
            self.assertEqual(
                testsuite[1].get("name"), f"{random_check_id}-0002 :: ColumnRequired"
            )

    def test_unittest_output_with_bad_rule(self):
        random_path = str(uuid4())
        random_error = str(uuid4())
        random_error_type = str(uuid4())

        rules = [
            InvalidRule(
                error=random_error, error_type=random_error_type, rule_path=random_path
            )
        ]
        _, checklist = FocusToPanderaSchemaConverter.generate_pandera_schema(
            rules=rules
        )
        result = ValidationResult(checklist=checklist)
        result.process_result()

        buffer = io.BytesIO()
        outputter = UnittestOutputter(output_destination=buffer)
        outputter.write(result_set=result)

        buffer.seek(0)
        output = buffer.read()
        testsuites = ET.fromstring(output)

        self.assertEqual(testsuites.get("tests"), "1")
        self.assertEqual(testsuites.get("failures"), "0")
        self.assertEqual(testsuites.get("skipped"), "0")
        self.assertEqual(testsuites.get("errors"), "1")

    def test_outputter_with_metric_dimension(self):
        random_check_id = f"FV-M00{randint(0, 9)}"
        random_column_id = str(uuid4())

        rules = [
            Rule(
                check_id=f"{random_check_id}-0001",
                column_id=random_column_id,
                check=DataTypeCheck(data_type=DataTypes.DECIMAL),
            ),
            Rule(
                check_id=f"{random_check_id}-0002",
                column_id=random_column_id,
                check="column_required",
            ),
        ]

        _, checklist = FocusToPanderaSchemaConverter.generate_pandera_schema(
            rules=rules
        )
        result = ValidationResult(checklist=checklist)
        result.process_result()

        buffer = io.BytesIO()
        outputter = UnittestOutputter(output_destination=buffer)
        outputter.write(result_set=result)

        buffer.seek(0)
        output = buffer.read()
        testsuites = ET.fromstring(output)
        self.assertEqual(len(testsuites), 1)  # assert one column in sample rules config
        self.assertEqual(testsuites.get("name"), "FOCUS Validations")
        for testsuite in testsuites:
            self.assertEqual(
                testsuite.get("name"), f"{random_check_id}-{random_column_id}"
            )
            self.assertEqual(
                len(testsuite), 2
            )  # assert two tests in sample rules config
            self.assertEqual(
                testsuite[0].get("name"), f"{random_check_id}-0001 :: DataTypeCheck"
            )
            self.assertEqual(
                testsuite[1].get("name"), f"{random_check_id}-0002 :: ColumnRequired"
            )
