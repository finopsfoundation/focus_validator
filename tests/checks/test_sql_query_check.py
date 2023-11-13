from unittest import TestCase

import pandas as pd
from pandera.errors import SchemaErrors
from pydantic import ValidationError

from focus_validator.config_objects import Rule
from focus_validator.config_objects.common import (
    SQLQueryCheck,
    DataTypes,
    DataTypeCheck,
)
from focus_validator.config_objects.focus_to_pandera_schema_converter import (
    FocusToPanderaSchemaConverter,
)
from focus_validator.rules.spec_rules import ValidationResult


# noinspection SqlNoDataSourceInspection,SqlDialectInspection
class TestSQLQueryCheck(TestCase):
    @staticmethod
    def __generate_sample_rule_type_string__(allow_nulls: bool, data_type: DataTypes):
        return [
            Rule(
                check_id="sql_check_for_multiple_columns",
                column_id="test_dimension",
                check=SQLQueryCheck(
                    sql_query="""
                    SELECT
                        test_dimension,
                        CASE WHEN test_dimension = 'some-value' THEN true ELSE false END AS check_output
                        FROM df;
                    """
                ),
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

    def test_sql_check_for_multiple_columns(self):
        test_sql_query = "SELECT * FROM table"

        with self.assertRaises(ValidationError) as cm:
            SQLQueryCheck(sql_query=test_sql_query)
        self.assertIn(
            "Assertion failed, SQL query must only return a column called 'check_output'",
            str(cm.exception),
        )

    def test_sql_check_with_invalid_sql(self):
        """
        Check for sql query that do not return column called check
        """

        # noinspection SqlDialectInspection,SqlNoDataSourceInspection
        test_sql_query = """SELECT 
            product_id,
            (CASE 
                WHEN product_id = 'a' THEN TRUE
                ELSE FALSE
            END) AS check_output
            FROM Products;"""

        # this query should be valid
        SQLQueryCheck(sql_query=test_sql_query)

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

        failure_cases_dict = validation_result.failure_cases.to_dict(orient="records")
        self.assertEqual(len(failure_cases_dict), 1)
        self.assertEqual(
            failure_cases_dict[0],
            {
                "Column": "test_dimension",
                "Check Name": "sql_check_for_multiple_columns",
                "Description": " None",
                "Values": "NULL",
                "Row #": 1,
            },
        )

    def test_pass_case(self):
        rules = self.__generate_sample_rule_type_string__(
            allow_nulls=True, data_type=DataTypes.STRING
        )
        sample_data = pd.DataFrame(
            [{"test_dimension": "some-value"}, {"test_dimension": "some-value"}]
        )

        schema, checklist = FocusToPanderaSchemaConverter.generate_pandera_schema(
            rules=rules, override_config=None
        )
        validation_result = self.__validate_helper__(
            schema=schema, checklist=checklist, sample_data=sample_data
        )
        self.assertIsNone(validation_result.failure_cases)
