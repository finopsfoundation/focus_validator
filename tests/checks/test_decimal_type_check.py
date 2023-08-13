from unittest import TestCase
from uuid import uuid4

import numpy
import pandas as pd
from pandera.errors import SchemaErrors

from focus_validator.config_objects import Rule
from focus_validator.config_objects.common import DataTypeCheck, DataTypes
from focus_validator.config_objects.focus_to_pandera_schema_converter import (
    FocusToPanderaSchemaConverter,
)
from focus_validator.rules.spec_rules import ValidationResult


class TestDecimalTypeCheck(TestCase):
    def test_decimal_column(self):
        random_column_id = str(uuid4())

        schema, checklist = FocusToPanderaSchemaConverter.generate_pandera_schema(
            rules=[
                Rule(
                    check_id=random_column_id,
                    column_id=random_column_id,
                    check=DataTypeCheck(data_type=DataTypes.DECIMAL),
                ),
            ]
        )

        sample_df = pd.DataFrame(
            [
                {random_column_id: 0.1},
                {random_column_id: 1},
                {random_column_id: 1.001},
            ]
        )
        values = schema.validate(sample_df)[random_column_id].values
        self.assertEqual(list(values), [0.1, 1.0, 1.001])

    def test_decimal_column_bad_data_type(self):
        random_column_id = str(uuid4())
        random_check_name = str(uuid4())

        schema, checklist = FocusToPanderaSchemaConverter.generate_pandera_schema(
            rules=[
                Rule(
                    check_id="some-check",
                    column_id=random_column_id,
                    check="column_required",
                ),
                Rule(
                    check_id=random_check_name,
                    column_id=random_column_id,
                    check=DataTypeCheck(data_type=DataTypes.DECIMAL),
                ),
            ]
        )

        sample_df = pd.DataFrame(
            [
                {random_column_id: "a"},
                {random_column_id: 1},
                {random_column_id: 1.001},
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
                    "Column": random_column_id,
                    "Check Name": random_check_name,
                    "Description": "Ensures that column is of decimal type.",
                    "Values": None,
                    "Row #": numpy.NaN,
                }
            ],
        )
