from unittest import TestCase
from uuid import uuid4

import pandas as pd
from pandera.errors import SchemaErrors

from focus_validator.config_objects import Rule
from focus_validator.config_objects.common import (
    DataTypes,
    ChecklistObjectStatus,
    DataTypeCheck,
)
from focus_validator.rules.spec_rules import ValidationResult


# noinspection DuplicatedCode
class TestAttributeCurrencyType(TestCase):
    def __eval_function__(self, sample_value, should_fail):
        random_column_name = str(uuid4())
        random_check_id = str(uuid4())

        schema, checklist = Rule.generate_schema(
            rules=[
                Rule(
                    check_id=random_check_id,
                    column=random_column_name,
                    check=DataTypeCheck(data_type=DataTypes.CURRENCY_CODE),
                )
            ]
        )

        sample_data = pd.DataFrame([{random_column_name: sample_value}])

        try:
            schema.validate(sample_data, lazy=True)
            failure_cases = None
        except SchemaErrors as e:
            failure_cases = e.failure_cases

        validation_result = ValidationResult(
            failure_cases=failure_cases, checklist=checklist
        )
        validation_result.process_result()

        if should_fail:
            self.assertIsNotNone(validation_result.failure_cases)
            records = validation_result.failure_cases.to_dict(orient="records")
            self.assertEqual(len(records), 1)
            collected_values = [record["Values"] for record in records]
            self.assertEqual(collected_values, [sample_value])
            self.assertEqual(
                validation_result.checklist[random_check_id].status,
                ChecklistObjectStatus.FAILED,
            )
        else:
            self.assertIsNone(validation_result.failure_cases)
            self.assertEqual(
                validation_result.checklist[random_check_id].status,
                ChecklistObjectStatus.PASSED,
            )

    def test_valid_currency_code(self):
        self.__eval_function__("USD", False)

    def test_valid_currency_code_bad_data_type(self):
        self.__eval_function__(0, True)

    def test_valid_currency_code_null_value(self):
        self.__eval_function__(None, False)

    def test_valid_currency_code_empty_string(self):
        self.__eval_function__("", True)
