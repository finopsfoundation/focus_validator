from unittest import TestCase

import pandas as pd
from pandera.errors import SchemaErrors

from focus_validator.config_objects import Rule
from focus_validator.config_objects.common import (
    AllowNullsCheck,
    ChecklistObjectStatus,
    DataTypeCheck,
    DataTypes,
)
from focus_validator.config_objects.focus_to_pandera_schema_converter import (
    FocusToPanderaSchemaConverter,
)
from focus_validator.rules.spec_rules import ValidationResult


# noinspection DuplicatedCode
class TestNullValueCheck(TestCase):
    @staticmethod
    def __generate_sample_rule_type_string__(allow_nulls: bool, data_type: DataTypes):
        return [
            Rule(
                check_id="allow_null",
                column_id="test_dimension",
                check=AllowNullsCheck(allow_nulls=allow_nulls),
            ),
            Rule(
                check_id="test_dimension",
                column_id="test_dimension",
                check=DataTypeCheck(data_type=data_type),
            ),
        ]

    @staticmethod
    def __validate_helper__(schema, checklist, sample_data):
        try:
            schema.validate(sample_data, lazy=True)
            failure_cases = None
        except SchemaErrors as e:
            failure_cases = e.failure_cases

        validation_result = ValidationResult(
            checklist=checklist, failure_cases=failure_cases
        )
        validation_result.process_result()
        return validation_result

    def test_null_value_allowed_valid_case(self):
        rules = self.__generate_sample_rule_type_string__(
            allow_nulls=True, data_type=DataTypes.STRING
        )
        sample_data = pd.DataFrame(
            [{"test_dimension": "NULL"}, {"test_dimension": "some-value"}]
        )

        schema, checklist = FocusToPanderaSchemaConverter.generate_pandera_schema(
            rules=rules, override_config=None
        )
        validation_result = self.__validate_helper__(
            schema=schema, checklist=checklist, sample_data=sample_data
        )
        self.assertIsNone(validation_result.failure_cases)

    def test_null_value_not_allowed_valid_case(self):
        rules = self.__generate_sample_rule_type_string__(
            allow_nulls=False, data_type=DataTypes.STRING
        )
        sample_data = pd.DataFrame(
            [{"test_dimension": "val1"}, {"test_dimension": "val2"}]
        )
        schema, checklist = FocusToPanderaSchemaConverter.generate_pandera_schema(
            rules=rules, override_config=None
        )
        validation_result = self.__validate_helper__(
            schema=schema, checklist=checklist, sample_data=sample_data
        )
        self.assertIsNone(validation_result.failure_cases)

    def test_null_value_not_allowed_invalid_case(self):
        rules = self.__generate_sample_rule_type_string__(
            allow_nulls=False, data_type=DataTypes.STRING
        )
        sample_data = pd.DataFrame(
            [{"test_dimension": "NULL"}, {"test_dimension": "val2"}]
        )
        schema, checklist = FocusToPanderaSchemaConverter.generate_pandera_schema(
            rules=rules, override_config=None
        )
        validation_result = self.__validate_helper__(
            schema=schema, checklist=checklist, sample_data=sample_data
        )
        self.assertEqual(
            validation_result.checklist["allow_null"].status,
            ChecklistObjectStatus.FAILED,
        )
        self.assertIsNotNone(validation_result.failure_cases)
        failure_cases_dict = validation_result.failure_cases.to_dict(orient="records")
        self.assertEqual(len(failure_cases_dict), 1)
        self.assertEqual(
            failure_cases_dict[0],
            {
                "Column": "test_dimension",
                "Check Name": "allow_null",
                "Description": " test_dimension does not allow null values.",
                "Values": "NULL",
                "Row #": 1,
            },
        )

    def test_null_value_allowed_invalid_case_with_empty_strings(self):
        rules = self.__generate_sample_rule_type_string__(
            allow_nulls=True, data_type=DataTypes.STRING
        )
        sample_data = pd.DataFrame([{"test_dimension": "NULL"}, {"test_dimension": ""}])

        schema, checklist = FocusToPanderaSchemaConverter.generate_pandera_schema(
            rules=rules, override_config=None
        )
        validation_result = self.__validate_helper__(
            schema=schema, checklist=checklist, sample_data=sample_data
        )
        self.assertEqual(
            validation_result.checklist["allow_null"].status,
            ChecklistObjectStatus.FAILED,
        )
        self.assertIsNotNone(validation_result.failure_cases)
        failure_cases_dict = validation_result.failure_cases.to_dict(orient="records")
        self.assertEqual(len(failure_cases_dict), 1)
        self.assertEqual(
            failure_cases_dict[0],
            {
                "Column": "test_dimension",
                "Check Name": "allow_null",
                "Description": " test_dimension allows null values.",
                "Values": "",
                "Row #": 2,
            },
        )

    def test_null_value_allowed_invalid_case_with_nan_values(self):
        rules = self.__generate_sample_rule_type_string__(
            allow_nulls=True, data_type=DataTypes.STRING
        )
        sample_data = pd.DataFrame(
            [{"test_dimension": "NULL"}, {"test_dimension": None}]
        )

        schema, checklist = FocusToPanderaSchemaConverter.generate_pandera_schema(
            rules=rules, override_config=None
        )
        validation_result = self.__validate_helper__(
            schema=schema, checklist=checklist, sample_data=sample_data
        )
        self.assertEqual(
            validation_result.checklist["allow_null"].status,
            ChecklistObjectStatus.FAILED,
        )
        self.assertIsNotNone(validation_result.failure_cases)
        failure_cases_dict = validation_result.failure_cases.to_dict(orient="records")
        self.assertEqual(len(failure_cases_dict), 1)
        self.assertEqual(
            failure_cases_dict[0],
            {
                "Column": "test_dimension",
                "Check Name": "allow_null",
                "Description": " test_dimension allows null values.",
                "Values": None,
                "Row #": 2,
            },
        )
