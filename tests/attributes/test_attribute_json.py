from unittest import TestCase
from uuid import uuid4

import pandas as pd
from pandera.errors import SchemaErrors

from focus_validator.config_objects import Rule
from focus_validator.config_objects.common import (
    ChecklistObjectStatus,
    DataTypeCheck,
    DataTypes,
)
from focus_validator.config_objects.focus_to_pandera_schema_converter import (
    FocusToPanderaSchemaConverter,
)
from focus_validator.rules.spec_rules import ValidationResult


# noinspection DuplicatedCode
class TestAttributeJSONObject(TestCase):
    def __eval_function__(self, sample_value, should_fail):
        random_column_id = str(uuid4())
        random_check_id = str(uuid4())

        schema, checklist = FocusToPanderaSchemaConverter.generate_pandera_schema(
            rules=[
                Rule(
                    check_id=random_check_id,
                    column_id=random_column_id,
                    check=DataTypeCheck(data_type=DataTypes.STRINGIFIED_JSON_OBJECT),
                )
            ]
        )

        sample_data = pd.DataFrame([{random_column_id: sample_value}])

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

    def test_valid_json(self):
        self.__eval_function__('{"my-cool-tag": "focus", "my-cool-tag-2": "whahoo"}', False)

    def test_valid_json_empty(self):
        self.__eval_function__('{}', False)

    def test_valid_json_bad_data_type(self):
        self.__eval_function__(0, True)

    def test_valid_json_null_value(self):
        self.__eval_function__(None, False)

    def test_valid_json_empty_string(self):
        self.__eval_function__("", True)
