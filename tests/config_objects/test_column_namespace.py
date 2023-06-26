from unittest import TestCase
from uuid import uuid4

import pandas as pd

from focus_validator.config_objects import Rule
from focus_validator.config_objects.common import (
    DataTypes,
    AllowNullsCheck,
    DataTypeCheck,
)
from focus_validator.validator import Validator


class TestColumnNamespace(TestCase):
    def test_load_rule_config_with_namespace(self):
        validator = Validator(
            data_filename="samples/multiple_failure_example_namespaced.csv",
            output_type="console",
            output_destination=None,
            column_namespace="F",
        )
        validator.load()
        result = validator.spec_rules.validate(focus_data=validator.focus_data)
        self.assertIsNotNone(result.failure_cases)

    def test_load_rule_config_without_namespace(self):
        random_column_id = str(uuid4())
        random_test_name = str(uuid4())

        schema, checklist = Rule.generate_schema(
            rules=[
                Rule(
                    check_id=random_test_name,
                    column_id=random_column_id,
                    check=DataTypeCheck(data_type=DataTypes.STRING),
                ),
                Rule(
                    check_id=random_test_name,
                    column_id=random_column_id,
                    check=AllowNullsCheck(allow_nulls=False),
                ),
            ]
        )

        sample_data = pd.read_csv("samples/multiple_failure_example_namespaced.csv")
        result = schema.validate(
            sample_data
        )  # should not fail as columns are namespaced
        self.assertIsNotNone(result)
