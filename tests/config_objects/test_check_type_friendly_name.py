from unittest import TestCase

from polyfactory.factories.pydantic_factory import ModelFactory
from pydantic import ValidationError

from focus_validator.config_objects.common import DataTypeConfig, DataTypes
from focus_validator.config_objects.rule import ValidationConfig


class TestCheckTypeFriendlyName(TestCase):
    def test_generate_name_for_validation_configs(self):
        """
        there is no way to generate all values for a field type hence generating random instances
        in hope of catching any validation error
        :return:
        """
        model_factory = ModelFactory.create_factory(model=ValidationConfig)

        for _ in range(1000):  # there is no way to generate all values for a field type
            random_model = model_factory.build()
            self.assertIn(
                random_model.check_type_friendly_name,
                [
                    "CheckUnique",
                    "AllowNullsCheck",
                    "ValueIn",
                ],  # needs to be updated as more checks are introduced
            )

    def test_random_value_is_ignored(self):
        sample = ValidationConfig(
            check="check_unique",
            check_friendly_name="some-check",
            check_type_friendly_name="some-name",
        )
        self.assertEqual(sample.check_type_friendly_name, "CheckUnique")

    def test_data_type_config(self):
        model_factory = ModelFactory.create_factory(model=DataTypeConfig)

        sample_data_type = model_factory.build()
        self.assertEqual(sample_data_type.check_type_friendly_name, "DataTypeCheck")

    def test_data_type_config_deny_update(self):
        model_factory = ModelFactory.create_factory(model=DataTypeConfig)

        sample_data_type = model_factory.build()
        with self.assertRaises(TypeError) as cm:
            sample_data_type.check_type_friendly_name = "new_value"
        self.assertIn(
            '"DataTypeConfig" is immutable and does not support item assignment',
            str(cm.exception),
        )
        self.assertEqual(sample_data_type.check_type_friendly_name, "DataTypeCheck")

    def test_assign_bad_type(self):
        with self.assertRaises(ValidationError) as cm:
            DataTypeConfig(
                data_type=DataTypes.DECIMAL, check_type_friendly_name="some-check"
            )
        self.assertIn("unexpected value; permitted: 'DataTypeCheck'", str(cm.exception))
