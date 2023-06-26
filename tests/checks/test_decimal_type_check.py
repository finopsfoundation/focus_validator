from unittest import TestCase
from uuid import uuid4

import numpy
import pandas as pd
from pandera.errors import SchemaErrors

from focus_validator.config_objects import Rule
from focus_validator.config_objects.common import DataTypeCheck, DataTypes
from focus_validator.rules.spec_rules import ValidationResult


class TestDecimalTypeCheck(TestCase):
    def test_decimal_column(self):
        random_column_name = str(uuid4())

        schema, checklist = Rule.generate_schema(
            rules=[
                Rule(
                    check_id=random_column_name,
                    column=random_column_name,
                    check=DataTypeCheck(data_type=DataTypes.DECIMAL),
                ),
            ]
        )

        sample_df = pd.DataFrame(
            [
                {random_column_name: 0.1},
                {random_column_name: 1},
                {random_column_name: 1.001},
            ]
        )
        values = schema.validate(sample_df)[random_column_name].values
        self.assertEqual(list(values), [0.1, 1.0, 1.001])

    def test_decimal_column_bad_data_type(self):
        random_column_name = str(uuid4())
        random_check_name = str(uuid4())

        schema, checklist = Rule.generate_schema(
            rules=[
                Rule(
                    check_id="some-check",
                    column=random_column_name,
                    check="column_required",
                ),
                Rule(
                    check_id=random_check_name,
                    column=random_column_name,
                    check=DataTypeCheck(data_type=DataTypes.DECIMAL),
                ),
            ]
        )

        sample_df = pd.DataFrame(
            [
                {random_column_name: "a"},
                {random_column_name: 1},
                {random_column_name: 1.001},
            ]
        )
        try:
            schema.validate(sample_df, lazy=True)
            failure_cases = None
        except SchemaErrors as e:
            failure_cases = e.failure_cases

        result = ValidationResult(checklist=checklist, failure_cases=failure_cases)
        result.process_result()
        self.assertEqual(
            result.failure_cases.to_dict(orient="records"),
            [
                {
                    "Column": random_column_name,
                    "Check Name": random_check_name,
                    "Description": "Ensures that column is of decimal type.",
                    "Values": None,
                    "Row #": numpy.NaN,
                }
            ],
        )
