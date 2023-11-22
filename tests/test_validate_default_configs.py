import os
import re
from itertools import groupby
from unittest import TestCase

import pandas as pd

from focus_validator.config_objects import ChecklistObjectStatus, Rule
from focus_validator.config_objects.common import DataTypeCheck, DataTypes
from focus_validator.rules.spec_rules import SpecRules


class TestValidateDefaultConfigs(TestCase):
    def test_version_sets_have_valid_config(self):
        for root, dirs, _ in os.walk(
            "focus_validator/rules/version_sets", topdown=False
        ):
            for version in dirs:
                spec_rules = SpecRules(
                    override_filename=None,
                    rule_set_path="focus_validator/rules/version_sets",
                    rules_version=version,
                    column_namespace=None,
                )
                spec_rules.load_rules()

                result = spec_rules.validate(focus_data=pd.DataFrame())
                for check_id in result.checklist.keys():
                    self.assertIsNot(
                        result.checklist[check_id].status, ChecklistObjectStatus.ERRORED
                    )
