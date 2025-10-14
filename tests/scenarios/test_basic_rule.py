import unittest
import pandas as pd
from io import StringIO
from focus_validator.rules.spec_rules import SpecRules


class TestExample(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures for conditional rule testing."""
        # Initialize SpecRules for FOCUS 1.2
        self.spec_rules = SpecRules(
            rule_set_path="focus_validator/rules",
            rules_file_prefix="cr-",
            rules_version="1.2",
            rules_file_suffix=".json",
            focus_dataset="CostAndUsage",
            filter_rules="BillingAccountName",
            rules_force_remote_download=False,
            allow_draft_releases=False,
            allow_prerelease_releases=False,
            column_namespace=None,
        )
        self.spec_rules.load()

    def test_rule_pass_scenario(self):
        csv_data = """BillingAccountName
"value1"
"""
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)
        
        # Check rule state
        rule_result = results.by_rule_id["BillingAccountName-C-001-M"]
        self.assertTrue(rule_result.get("ok"), f"Rule should PASS but got: {rule_result}")
    
    def test_rule_fail_scenario(self):
        csv_data = """BillingAccountName
0.32
"""
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)
        
        # Check rule state
        rule_result = results.by_rule_id["BillingAccountName-C-001-M"]
        self.assertFalse(rule_result.get("ok"), f"Rule should FAIL but got: {rule_result}")