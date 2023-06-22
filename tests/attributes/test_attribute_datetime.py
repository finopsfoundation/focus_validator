from unittest import TestCase
from uuid import uuid4

import pandas as pd
from pandera.errors import SchemaErrors

from focus_validator.config_objects import Rule
from focus_validator.config_objects.common import (
    DataTypeConfig,
    DataTypes,
    ChecklistObjectStatus,
)
from focus_validator.rules.spec_rules import ValidationResult


class TestAttributeDatetime(TestCase):
    def test_valid_datetime_with_timezone(self):
        random_dimension_name = str(uuid4())
        random_check_id = str(uuid4())

        schema, checklist = Rule.generate_schema(
            rules=[
                Rule(
                    check_id=random_check_id,
                    dimension=random_dimension_name,
                    validation_config=DataTypeConfig(data_type=DataTypes.DATETIME),
                )
            ]
        )

        sample_data = pd.DataFrame(
            [
                {random_dimension_name: "2023-05-13T21:00:00Z"},
                {random_dimension_name: "2023-13-13T21:00:00Z"},
                {random_dimension_name: "bad-value"},
                {random_dimension_name: 0},
                {random_dimension_name: None},
            ]
        )

        try:
            schema.validate(sample_data, lazy=True)
            failure_cases = None
        except SchemaErrors as e:
            failure_cases = e.failure_cases

        validation_result = ValidationResult(
            failure_cases=failure_cases, checklist=checklist
        )
        validation_result.process_result()

        self.assertEqual(
            validation_result.checklist[random_check_id].status,
            ChecklistObjectStatus.FAILED,
        )
        records = validation_result.failure_cases.to_dict(orient="records")
        self.assertEqual(len(records), 3)
        collected_values = [record["Values"] for record in records]
        self.assertEqual(collected_values, ["2023-13-13T21:00:00Z", "bad-value", 0])
