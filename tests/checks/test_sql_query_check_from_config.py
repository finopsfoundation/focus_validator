import tempfile
from unittest import TestCase

import pandas as pd
from pandera.errors import SchemaErrors

from focus_validator.config_objects import Rule
from focus_validator.config_objects.common import DataTypeCheck, DataTypes
from focus_validator.config_objects.focus_to_pandera_schema_converter import (
    FocusToPanderaSchemaConverter,
)
from focus_validator.rules.spec_rules import ValidationResult

YAML_CONFIG = """
check_id: test_dimension
column_id: SkuPriceId
check_friendly_name: SkuPriceId must be set for certain values of ChargeType
check:
  sql_query: |
    SELECT CASE
        WHEN ChargeType IN ('Purchase', 'Usage', 'Refund') AND SkuPriceId IS NULL THEN FALSE
        ELSE TRUE
    END AS check_output
    FROM df;
"""


class TestSQLQueryCheckConfig(TestCase):
    def test_config_from_yaml(self):
        with tempfile.NamedTemporaryFile() as f:
            f.write(YAML_CONFIG.encode())
            f.seek(0)
            rule = Rule.load_yaml(f.name)

        dimension_check = Rule(
            check_id="test_dimension",
            column_id="SkuPriceId",
            check=DataTypeCheck(data_type=DataTypes.STRING),
        )

        sample_data = pd.DataFrame(
            [
                {
                    "test_dimension": "some-value",
                    "SkuPriceId": "some-value",
                    "ChargeType": "Purchase",
                },
                {
                    "test_dimension": "some-value",
                    "SkuPriceId": None,
                    "ChargeType": "Purchase",
                },
            ]
        )

        schema, checklist = FocusToPanderaSchemaConverter.generate_pandera_schema(
            rules=[dimension_check, rule], override_config=None
        )
        try:
            schema.validate(sample_data, lazy=True)
            failure_cases = None
        except SchemaErrors as e:
            failure_cases = e.failure_cases

        validation_result = ValidationResult(
            checklist=checklist, failure_cases=failure_cases
        )
        validation_result.process_result()

        failure_cases_dict = validation_result.failure_cases.to_dict(orient="records")
        self.assertEqual(len(failure_cases_dict), 2)
        self.assertEqual(
            failure_cases_dict[0],
            {
                "Column": "test_dimension",
                "Check Name": "test_dimension",
                "Description": " SkuPriceId must be set for certain values of ChargeType",
                "Values": "some-value",
                "Row #": 2,
            },
        )
