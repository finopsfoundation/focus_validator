import tempfile
from datetime import datetime
from unittest import TestCase
from uuid import uuid4

import pytz
import pandas as pd
import pytz
from pandera.errors import SchemaErrors

from focus_validator.config_objects import Rule
from focus_validator.config_objects.common import (
    DataTypeCheck,
    DataTypes,
    ChecklistObjectStatus,
)
from focus_validator.config_objects.focus_to_pandera_schema_converter import (
    FocusToPanderaSchemaConverter,
)
from focus_validator.rules.spec_rules import ValidationResult


# noinspection DuplicatedCode
class TestDatetimeColumnLoadFromCSV(TestCase):
    def __assert_values__(
        self, random_column_id, should_fail, sample_value, sample_data
    ):
        random_check_id = str(uuid4())

        schema, checklist = FocusToPanderaSchemaConverter.generate_pandera_schema(
            rules=[
                Rule(
                    check_id=str(uuid4()),
                    column_id=random_column_id,
                    check="column_required",
                    check_friendly_name="Column required.",
                ),
                Rule(
                    check_id=random_check_id,
                    column_id=random_column_id,
                    check=DataTypeCheck(data_type=DataTypes.DATETIME),
                ),
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

    def test_load_column_with_valid_datetime_utc(self):
        """
        Test case ensuring UTC datetime field passes test case.
        """

        random_column_id = str(uuid4())
        utc_datetime = datetime.now(tz=pytz.UTC)

        sample_df = pd.DataFrame([{random_column_id: utc_datetime}])

        with tempfile.NamedTemporaryFile(suffix=".csv") as temp_file:
            sample_df.to_csv(temp_file)
            read_df = pd.read_csv(temp_file.name)

        self.__assert_values__(
            random_column_id=random_column_id,
            should_fail=False,
            sample_value=str(utc_datetime),
            sample_data=read_df,
        )

    def test_load_column_with_valid_datetime_naive(self):
        """
        Test case ensuring naive datetime field fails test case.
        """

        random_column_id = str(uuid4())
        naive_datetime = datetime.now(tz=None)

        sample_df = pd.DataFrame([{random_column_id: naive_datetime}])

        with tempfile.NamedTemporaryFile(suffix=".csv") as temp_file:
            sample_df.to_csv(temp_file)
            read_df = pd.read_csv(temp_file.name)

        self.__assert_values__(
            random_column_id=random_column_id,
            should_fail=True,
            sample_value=str(naive_datetime),
            sample_data=read_df,
        )

    def test_load_column_with_valid_datetime_not_utc(self):
        """
        Test case ensures non UTC datetime value fails validation.
        """

        random_column_id = str(uuid4())

        local_timezone = pytz.timezone("America/Los_Angeles")
        aware_datetime = local_timezone.localize(datetime.now())

        # generate random dataframe
        sample_df = pd.DataFrame([{random_column_id: aware_datetime}])

        with tempfile.NamedTemporaryFile(suffix=".csv") as temp_file:
            # write csv to temporary location and read to simulate df read
            sample_df.to_csv(temp_file)
            read_df = pd.read_csv(temp_file.name)

        self.__assert_values__(
            random_column_id=random_column_id,
            should_fail=True,
            sample_value=str(aware_datetime),
            sample_data=read_df,
        )

    def test_load_column_with_invalid_datetime(self):
        """
        Test case ensures invalid date value fails validation.
        """

        random_column_id = str(uuid4())

        bad_value = str(uuid4())

        # generate random dataframe
        sample_df = pd.DataFrame([{random_column_id: bad_value}])

        with tempfile.NamedTemporaryFile(suffix=".csv") as temp_file:
            # write csv to temporary location and read to simulate df read
            sample_df.to_csv(temp_file)
            read_df = pd.read_csv(temp_file.name)

        self.__assert_values__(
            random_column_id=random_column_id,
            should_fail=True,
            sample_value=bad_value,
            sample_data=read_df,
        )
