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

    def test_default_rules_with_sample_data(self):
        check_id_pattern = re.compile(r"FV-[D,M]\d{3}-\d{4}$")

        for root, dirs, files in os.walk(
            "focus_validator/rules/version_sets", topdown=False
        ):
            column_test_suites = []
            for file_path in files:
                rule_path = os.path.join(root, file_path)
                rule = Rule.load_yaml(rule_path=rule_path)
                self.assertIsInstance(rule, Rule)

                column_id = rule.column_id
                self.assertIsNotNone(re.match(check_id_pattern, rule.check_id))

                check_column_id = rule.check_id.split("-")[1]
                local_check_id = rule.check_id.split("-")[2]
                column_test_suites.append((column_id, check_column_id, local_check_id))

            # sort column test suites to allow grouping by column
            column_test_suites = sorted(column_test_suites, key=lambda item: item[0])
            for _, test_suites in groupby(column_test_suites, key=lambda item: item[0]):
                test_suites = list(test_suites)
                self.assertEqual(
                    len(set([test_suite[1] for test_suite in test_suites])), 1
                )
                local_check_ids = [int(test_suite[2]) for test_suite in test_suites]
                # check all ids are in order
                self.assertEqual(
                    sorted(local_check_ids), list(range(1, len(local_check_ids) + 1))
                )

    def test_metric_file_format_metric_vs_dimension(self):
        metric_check_id_pattern = re.compile(r"FV-M\d{3}-\d{4}$")
        dimension_check_id_pattern = re.compile(r"FV-D\d{3}-\d{4}$")

        for root, dirs, files in os.walk(
            "focus_validator/rules/version_sets", topdown=False
        ):
            for file_path in files:
                rule_path = os.path.join(root, file_path)
                rule = Rule.load_yaml(rule_path=rule_path)
                self.assertIsInstance(rule, Rule)

                if isinstance(rule.check, DataTypeCheck):
                    if rule.check.data_type == DataTypes.DECIMAL:
                        self.assertIsNotNone(
                            re.match(metric_check_id_pattern, rule.check_id),
                            "For metric column type check_id format should be FV-MYYY-YYYY",
                        )
                    else:
                        self.assertIsNotNone(
                            re.match(dimension_check_id_pattern, rule.check_id),
                            "For metric column type check_id format should be FV-DYYY-YYYY",
                        )
