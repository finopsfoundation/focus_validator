from unittest import TestCase
from uuid import uuid4

import numpy
import pandas as pd
from pandera.errors import SchemaErrors

from focus_validator.config_objects import Rule
from focus_validator.config_objects.common import DataTypes, DataTypeConfig
from focus_validator.config_objects.rule import ValidationConfig
from focus_validator.rules.spec_rules import ValidationResult


class TestDecimalTypeCheck(TestCase):
    def test_decimal_dimension(self):
        random_dimension_name = str(uuid4())

        schema, checklist = Rule.generate_schema(
            rules=[
                Rule(
                    check_id=random_dimension_name,
                    dimension=random_dimension_name,
                    validation_config=DataTypeConfig(data_type=DataTypes.DECIMAL),
                ),
            ]
        )

        sample_df = pd.DataFrame(
            [
                {random_dimension_name: 0.1},
                {random_dimension_name: 1},
                {random_dimension_name: 1.001},
            ]
        )
        values = schema.validate(sample_df)[random_dimension_name].values
        self.assertEqual(list(values), [0.1, 1.0, 1.001])

    def test_decimal_dimension_bad_data_type(self):
        random_dimension_name = str(uuid4())
        random_check_name = str(uuid4())

        schema, checklist = Rule.generate_schema(
            rules=[
                Rule(
                    check_id="some-check",
                    dimension=random_dimension_name,
                    validation_config=ValidationConfig(
                        check="dimension_required",
                        check_friendly_name="random dimension required",
                    ),
                ),
                Rule(
                    check_id=random_check_name,
                    dimension=random_dimension_name,
                    validation_config=DataTypeConfig(data_type=DataTypes.DECIMAL),
                ),
            ]
        )

        sample_df = pd.DataFrame(
            [
                {random_dimension_name: "a"},
                {random_dimension_name: 1},
                {random_dimension_name: 1.001},
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
                    "Dimension": random_dimension_name,
                    "Check Name": random_check_name,
                    "Description": "Ensures that dimension is of decimal type.",
                    "Values": None,
                    "Row #": numpy.NaN,
                }
            ],
        )
