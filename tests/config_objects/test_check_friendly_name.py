from unittest import TestCase
from uuid import uuid4

from polyfactory.factories.pydantic_factory import ModelFactory

from focus_validator.config_objects import Rule
from focus_validator.config_objects.common import (
    AllowNullsCheck,
    DataTypeCheck,
    ValueInCheck,
)


class TestCheckFriendlyName(TestCase):
    def test_default_friendly_name_is_generated(self):
        random_column_name = str(uuid4())

        model_factory = ModelFactory.create_factory(
            model=Rule, check_friendly_name=None, column_id=random_column_name
        )

        for _ in range(1000):  # there is no way to generate all values for a field type
            random_model = model_factory.build()
            if random_model.check == "column_required":
                self.assertEqual(
                    random_model.check_friendly_name,
                    f"{random_column_name} is a required column.",
                )
            elif random_model.check == "check_unique":
                self.assertEqual(
                    random_model.check_friendly_name,
                    f"{random_column_name}, requires unique values.",
                )
            elif isinstance(random_model.check, AllowNullsCheck):
                if random_model.check.allow_nulls:
                    self.assertEqual(
                        random_model.check_friendly_name,
                        f"{random_column_name} allows null values.",
                    )
                else:
                    self.assertEqual(
                        random_model.check_friendly_name,
                        f"{random_column_name} does not allow null values.",
                    )
            elif isinstance(random_model.check, DataTypeCheck):
                self.assertEqual(
                    random_model.check_friendly_name,
                    f"{random_column_name} requires values of type {random_model.check.data_type.value}.",
                )
            elif isinstance(random_model.check, ValueInCheck):
                options = ",".join(random_model.check.value_in)
                self.assertEqual(
                    random_model.check_friendly_name,
                    f"{random_column_name} must have a value from the list: {options}.",
                )
            else:
                raise NotImplementedError(
                    f"check_type: {random_model.check} not implemented"
                )

    def test_override_friendly_name(self):
        random_friendly_name = str(uuid4())

        sample_rule = Rule(
            check="check_unique",
            check_id="sample-check",
            column_id="sample-column",
            check_friendly_name=random_friendly_name,
        )
        self.assertIsNotNone(random_friendly_name, sample_rule.check_friendly_name)
