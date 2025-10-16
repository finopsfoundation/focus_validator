import os
import re
from itertools import groupby
from unittest import TestCase

import pandas as pd

from focus_validator.config_objects import ChecklistObjectStatus
from focus_validator.rules.spec_rules import SpecRules


class TestValidateDefaultConfigs(TestCase):
    def test_available_versions_have_valid_config(self):
        # Test the available versions in the rules directory
        spec_rules = SpecRules(
            rule_set_path="focus_validator/rules",
            rules_file_prefix="model-",
            rules_version="1.2", 
            rules_file_suffix=".json",
            focus_dataset="CostAndUsage",
            filter_rules=None,
            rules_force_remote_download=False,
            allow_draft_releases=False,
            allow_prerelease_releases=False,
            column_namespace=None,
            rules_block_remote_download=True,
        )
        spec_rules.load_rules()

        # Create a minimal DataFrame with required columns for testing
        test_data = pd.DataFrame({
            'BillingAccountId': ['test-account'],
            'ChargeType': ['Usage'],
            'BilledCost': [10.50]
        })
        result = spec_rules.validate(focus_data=test_data)
        # Check that no rules errored during setup/loading
        for rule_id, rule_result in result.by_rule_id.items():
            details = rule_result.get("details", {})
            # Allow failures but not errors during execution
            self.assertNotEqual(details.get("status"), "errored", 
                              f"Rule {rule_id} had an error: {details.get('message')}")
        
        # Test that we can access rule metadata
        self.assertGreater(len(result.rules), 0, "Should have loaded some rules")
        for rule_id, rule in result.rules.items():
            self.assertIsNotNone(rule.validation_criteria.must_satisfy,
                               f"Rule {rule_id} should have MustSatisfy field")
