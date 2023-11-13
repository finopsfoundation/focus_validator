from unittest import TestCase
from uuid import uuid4

from polyfactory.factories.pydantic_factory import ModelFactory
from pydantic import ValidationError

from focus_validator.config_objects import Rule
from focus_validator.config_objects.common import DataTypes, DataTypeCheck


class TestCheckTypeFriendlyName(TestCase):
    def test_generate_name_for_check_types(self):
        """
        there is no way to generate all values for a field type hence generating random instances
        in hope of catching any validation error
        :return:
        """
        model_factory = ModelFactory.create_factory(model=Rule)

        for _ in range(1000):  # there is no way to generate all values for a field type
            try:
                random_model = model_factory.build()
            except ValidationError as e:
                if "SQLQueryCheck" in str(e):
                    # SQLQueryCheck is not supported by ModelFactory
                    continue
                else:
                    raise e

            self.assertIn(
                random_model.check_type_friendly_name,
                [
                    "CheckUnique",
                    "AllowNullsCheck",
                    "ValueInCheck",
                    "ColumnRequired",
                    "DataTypeCheck",
                ],  # needs to be updated as more checks are introduced
            )

    def test_random_value_is_ignored(self):
        sample = Rule(
            check_id=str(uuid4()),
            column_id=str(uuid4()),
            check="check_unique",
            check_friendly_name="some-check",
            check_type_friendly_name="some-name",
        )
        self.assertEqual(sample.check_type_friendly_name, "CheckUnique")

    def test_data_type_config(self):
        model_factory = ModelFactory.create_factory(model=Rule)

        sample_data_type = model_factory.build(
            **{"check": DataTypeCheck(data_type=DataTypes.STRING)}
        )
        self.assertEqual(sample_data_type.check_type_friendly_name, "DataTypeCheck")

    def test_check_type_config_deny_update(self):
        model_factory = ModelFactory.create_factory(model=Rule)

        try:
            sample_data_type = model_factory.build()
        except ValidationError as e:
            if "SQLQueryCheck" in str(e):
                # SQLQueryCheck is not supported by ModelFactory
                return
            else:
                raise e

        with self.assertRaises(ValidationError) as cm:
            sample_data_type.check_type_friendly_name = "new_value"
        self.assertIn(
            "Instance is frozen",
            str(cm.exception),
        )

    def test_assign_bad_type(self):
        with self.assertRaises(ValidationError) as cm:
            Rule(
                check_id=str(uuid4()),
                column_id=str(uuid4()),
                check=DataTypeCheck(data_type="bad-type"),
                check_type_friendly_name="some-check",
            )
        self.assertEqual(len(cm.exception.errors()), 1)
        self.assertIn(
            "Input should be 'string', 'decimal', 'datetime', 'currency-code' or 'stringified-json-object'",
            str(cm.exception),
        )
