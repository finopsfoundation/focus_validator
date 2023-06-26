from unittest import TestCase
from uuid import uuid4

import pandera as pa

from focus_validator.config_objects import Rule
from focus_validator.config_objects.common import ValueInCheck


class TestFriendlyNameInValuesTemplate(TestCase):
    def test_check_value_in(self):
        rule = Rule(
            check_id=str(uuid4()),
            column=str(uuid4()),
            check=ValueInCheck(value_in=["foo", "bar"]),
            check_friendly_name="Values in {values}",
        )
        pa_check = rule.generate_pandera_rule(check_id=str(uuid4()))
        self.assertIsInstance(pa_check, pa.Check)
