"""
Unit tests for multiple rule validation scenarios.

These tests validate multiple rule states simultaneously using CSV data 
stored in Python objects.
"""

import unittest
import pandas as pd
from io import StringIO

from focus_validator.rules.spec_rules import SpecRules, ValidationResults


class TestMultipleRulesScenarios(unittest.TestCase):
    """Test multiple rules with various data scenarios."""

    def setUp(self):
        """Set up test fixtures for multiple rule testing."""
        # Initialize SpecRules for FOCUS 1.2 with multiple rules
        self.spec_rules = SpecRules(
            rule_set_path="focus_validator/rules",
            rules_file_prefix="model-",
            rules_version="1.2",
            rules_file_suffix=".json",
            focus_dataset="CostAndUsage",
            filter_rules="BilledCost-C-001-M,BilledCost-C-002-M,BilledCost-C-003-M,BilledCost-C-005-C",
            rules_force_remote_download=False,
            rules_block_remote_download=True,
            allow_draft_releases=False,
            allow_prerelease_releases=False,
            column_namespace=None,
        )
        self.spec_rules.load()

    def test_billed_cost_basic_validations_pass(self):
        """
        Test multiple BilledCost rules with valid data.
        Expected: All basic validation rules should PASS.
        """
        # CSV data with valid BilledCost values
        csv_data = """InvoiceIssuerName,ResourceID,ProviderName,BilledCost,AvailabilityZone
"AWS","r1","AWS","10.50","us-east-1"
"Azure","r2","Azure","25.75","westus2"
"GCP","r3","GCP","0","us-central1"
"""
        
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)
        
        # Test each expected rule
        expected_passes = [
            "BilledCost-C-001-M",  # Type validation (Decimal)
            "BilledCost-C-002-M",  # Format validation (NumericFormat)
            "BilledCost-C-003-M",  # Not null validation
        ]
        
        for rule_id in expected_passes:
            self.assertIn(rule_id, results.by_rule_id, f"Rule {rule_id} should be present")
            rule_result = results.by_rule_id[rule_id]
            self.assertTrue(rule_result.get("ok"), 
                           f"{rule_id} should PASS but got: {rule_result}")
            self.assertEqual(rule_result["details"]["violations"], 0)

    def test_billed_cost_conditional_rule_behavior(self):
        """
        Test conditional rule behavior with different scenarios.
        """
        # Data that should trigger conditional rule
        csv_data = """InvoiceIssuerName,ResourceID,ProviderName,BilledCost,AvailabilityZone
"marketplace-vendor","r1","AWS","15.50","us-east-1"
"AWS","r2","AWS","10.50","us-east-1"
"""
        
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)
        
        # Basic rules should pass
        basic_rules = ["BilledCost-C-001-M", "BilledCost-C-002-M", "BilledCost-C-003-M"]
        for rule_id in basic_rules:
            rule_result = results.by_rule_id[rule_id]
            self.assertTrue(rule_result.get("ok"), f"{rule_id} should pass with valid data")
        
        # Conditional rule should fail (marketplace-vendor/AWS with non-zero BilledCost)
        conditional_rule = results.by_rule_id["BilledCost-C-005-C"]
        self.assertFalse(conditional_rule.get("ok"), 
                        "BilledCost-C-005-C should FAIL when condition is met but requirement violated")
        self.assertGreater(conditional_rule["details"]["violations"], 0)

    def test_mixed_validation_results(self):
        """
        Test scenario where some rules pass and others fail.
        """
        # Data with null BilledCost (will fail C-003-M) but valid third-party scenario
        csv_data = """InvoiceIssuerName,ResourceID,ProviderName,BilledCost,AvailabilityZone
"third-party","r1","AWS","0","us-east-1"
"AWS","r2","AWS","","us-east-1"
"""
        
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)
        
        # Test specific expectations
        rule_expectations = {
            "BilledCost-C-001-M": True,   # Type validation should handle null gracefully
            "BilledCost-C-002-M": True,   # Format validation should handle null gracefully  
            "BilledCost-C-003-M": False,  # Not null validation should fail with empty string
            "BilledCost-C-005-C": True,   # Conditional rule should pass (0 cost for third-party)
        }
        
        for rule_id, should_pass in rule_expectations.items():
            self.assertIn(rule_id, results.by_rule_id, f"Rule {rule_id} should be present")
            rule_result = results.by_rule_id[rule_id]
            
            if should_pass:
                self.assertTrue(rule_result.get("ok"), 
                               f"{rule_id} should PASS but got: {rule_result}")
                self.assertEqual(rule_result["details"]["violations"], 0)
            else:
                self.assertFalse(rule_result.get("ok"), 
                                f"{rule_id} should FAIL but got: {rule_result}")
                self.assertGreater(rule_result["details"]["violations"], 0)

    def test_validation_summary_metrics(self):
        """
        Test that we can analyze validation results across multiple rules.
        """
        csv_data = """InvoiceIssuerName,ResourceID,ProviderName,BilledCost,AvailabilityZone
"AWS","r1","AWS","10.50","us-east-1"
"third-party","r2","Azure","0","westus2"
"""

        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)

        # Count passes and failures for our specific rules
        target_rules = ["BilledCost-C-001-M", "BilledCost-C-002-M", "BilledCost-C-003-M", "BilledCost-C-005-C"]
        target_passes = sum(1 for rule_id in target_rules 
                           if rule_id in results.by_rule_id and results.by_rule_id[rule_id].get("ok"))
        target_failures = sum(1 for rule_id in target_rules 
                             if rule_id in results.by_rule_id and not results.by_rule_id[rule_id].get("ok"))
        
        # Only count violations from our target rules
        target_violations = sum(results.by_rule_id[rule_id]["details"]["violations"]
                               for rule_id in target_rules 
                               if rule_id in results.by_rule_id)

        # Validate metrics for our target rules
        self.assertGreater(target_passes, 0, "Should have some passing target rules")
        self.assertEqual(target_violations, 0, "Should have no violations in target rules with valid data")
        self.assertEqual(len([r for r in target_rules if r in results.by_rule_id]), 4, 
                        "Should have all 4 target rules in results")

    def test_results_data_access_patterns(self):
        """
        Test various ways to access and analyze validation results.
        """
        csv_data = """InvoiceIssuerName,ResourceID,ProviderName,BilledCost,AvailabilityZone
"AWS","r1","AWS","5.25","us-east-1"
"""
        
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)
        
        # Test accessing results by rule ID
        for rule_id in ["BilledCost-C-001-M", "BilledCost-C-002-M", "BilledCost-C-003-M"]:
            rule_result = results.by_rule_id[rule_id]
            self.assertIsInstance(rule_result, dict)
            self.assertIn("ok", rule_result)
            self.assertIn("details", rule_result)
            self.assertIn("rule_id", rule_result)
            self.assertEqual(rule_result["rule_id"], rule_id)
        
        # Test that by_idx and by_rule_id are consistent
        self.assertGreater(len(results.by_idx), 0, "Should have indexed results")
        
        # Test accessing rule metadata
        self.assertIsInstance(results.rules, dict)
        for rule_id in results.by_rule_id.keys():
            if rule_id in results.rules:
                rule_obj = results.rules[rule_id] 
                self.assertTrue(hasattr(rule_obj, 'validation_criteria'), 
                               f"Rule {rule_id} should have validation criteria")

    def test_empty_data_handling(self):
        """
        Test how validation handles empty datasets for our target BilledCost rules.
        Note: We focus only on BilledCost rules since column presence rules will fail.
        """
        # Empty DataFrame with basic required columns
        csv_data = """InvoiceIssuerName,ResourceID,ProviderName,BilledCost,AvailabilityZone"""

        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)

        # Check that our target BilledCost rules behave appropriately with empty data
        target_rules = ["BilledCost-C-001-M", "BilledCost-C-002-M", "BilledCost-C-003-M", "BilledCost-C-005-C"]
        
        # Verify our target rules are present and pass with empty data
        found_rules = []
        for rule_id in target_rules:
            if rule_id in results.by_rule_id:
                found_rules.append(rule_id)
                rule_result = results.by_rule_id[rule_id]
                # Empty data typically passes BilledCost rules (no data to violate them)
                self.assertTrue(rule_result.get("ok"),
                               f"{rule_id} should PASS with empty data but got: {rule_result}")

        # Ensure we found at least some of our target rules
        self.assertGreater(len(found_rules), 0, f"Should find some target rules, found: {found_rules}")

        # Count violations only in our target rules
        target_violations = sum(results.by_rule_id[rule_id]["details"]["violations"]
                               for rule_id in target_rules 
                               if rule_id in results.by_rule_id)
        self.assertEqual(target_violations, 0, "Should have no violations in target BilledCost rules with empty data")
if __name__ == '__main__':
    unittest.main()