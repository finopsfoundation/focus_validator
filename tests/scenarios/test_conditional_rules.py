import unittest
import pandas as pd
from io import StringIO
from helper import load_rule_data_from_file, SpecRulesFromData
from focus_validator.rules.spec_rules import ValidationResults


class TestConditionalRulesScenarios(unittest.TestCase):
    """Test conditional rules with various data scenarios."""

    def setUp(self):
        """Set up test fixtures for conditional rule testing."""
        # Load base rule data and configure for BilledCost conditional rule
        self.rule_data = load_rule_data_from_file("base_rule_data.json")
        self.rule_data['ModelDatasets'] = {
            "CostAndUsage": {
                "ModelRules": ["BilledCost-C-005-C"]
            }
        }
        
        # Define the BilledCost-C-005-C conditional rule
        self.rule_data["ModelRules"] = self.rule_data.get("ModelRules", {})
        self.rule_data["ModelRules"] = {
            "BilledCost-C-005-C": {
                "Function": "Validation",
                "Reference": "BilledCost",
                "EntityType": "Column",
                "Notes": "",
                "ModelVersionIntroduced": "1.2",
                "Status": "Active",
                "ApplicabilityCriteria": [],
                "Type": "Static",
                "ValidationCriteria": {
                "MustSatisfy": "BilledCost MUST be 0 for charges where payments are received by a third party (e.g., marketplace transactions).",
                "Keyword": "MUST",
                "Requirement": {
                    "CheckFunction": "CheckValue",
                    "ColumnName": "BilledCost",
                    "Value": 0
                },
                "Condition": {
                    "CheckFunction": "CheckNotSameValue",
                    "ColumnAName": "ProviderName",
                    "ColumnBName": "InvoiceIssuerName"
                },
                "Dependencies": [
                    "ProviderName-C-000-M",
                    "InvoiceIssuerName-C-000-M"
                ]
                }
            }
        }
        
        self.spec_rules = SpecRulesFromData(
            rule_data=self.rule_data,
            focus_dataset="CostAndUsage",
            filter_rules="BilledCost-C-005-C",
            applicability_criteria_list=["ALL"]
        )
        self.spec_rules.load()

    def test_billed_cost_conditional_rule_should_pass(self):
        """
        Test BilledCost-C-005-C should PASS when:
        - ProviderName != InvoiceIssuerName (condition met)
        - BilledCost = 0 (requirement satisfied)
        """
        # CSV data as Python string
        csv_data = """InvoiceIssuerName,ResourceID,ProviderName,BilledCost,AvailabilityZone
"marketplace-vendor","r1","AWS",0,"us-east-1"
"third-party-billing","r2","Azure",0,"westus2"
"""
        
        # Convert to DataFrame
        df = pd.read_csv(StringIO(csv_data))
        
        # Run validation
        results = self.spec_rules.validate(focus_data=df)
        
        # Assert BilledCost-C-005-C passes
        self.assertIn("BilledCost-C-005-C", results.by_rule_id)
        rule_result = results.by_rule_id["BilledCost-C-005-C"]
        
        self.assertTrue(rule_result.get("ok"), 
                       f"BilledCost-C-005-C should PASS but got: {rule_result}")
        self.assertEqual(rule_result["details"]["violations"], 0)

    def test_billed_cost_conditional_rule_should_fail_non_zero_cost(self):
        """
        Test BilledCost-C-005-C should FAIL when:
        - ProviderName != InvoiceIssuerName (condition met)
        - BilledCost != 0 (requirement NOT satisfied)
        """
        # CSV data with non-zero BilledCost where condition applies
        csv_data = """InvoiceIssuerName,ResourceID,ProviderName,BilledCost,AvailabilityZone
"marketplace-vendor","r1","AWS",15.50,"us-east-1"
"third-party-billing","r2","Azure",25.75,"westus2"
"""
        
        # Convert to DataFrame
        df = pd.read_csv(StringIO(csv_data))
        
        # Run validation
        results = self.spec_rules.validate(focus_data=df)
        
        # Assert BilledCost-C-005-C fails
        self.assertIn("BilledCost-C-005-C", results.by_rule_id)
        rule_result = results.by_rule_id["BilledCost-C-005-C"]
        
        self.assertFalse(rule_result.get("ok"), 
                        f"BilledCost-C-005-C should FAIL but got: {rule_result}")
        self.assertGreater(rule_result["details"]["violations"], 0)

    def test_billed_cost_conditional_rule_condition_not_met(self):
        """
        Test BilledCost-C-005-C should PASS when:
        - ProviderName == InvoiceIssuerName (condition NOT met)
        - BilledCost != 0 (requirement irrelevant since condition not met)
        """
        # CSV data where ProviderName == InvoiceIssuerName
        csv_data = """InvoiceIssuerName,ResourceID,ProviderName,BilledCost,AvailabilityZone
"AWS","r1","AWS",100.50,"us-east-1"
"Azure","r2","Azure",75.25,"westus2"
"""
        
        # Convert to DataFrame
        df = pd.read_csv(StringIO(csv_data))
        
        # Run validation
        results = self.spec_rules.validate(focus_data=df)
        
        # Assert BilledCost-C-005-C passes (condition not met, so requirement doesn't apply)
        self.assertIn("BilledCost-C-005-C", results.by_rule_id)
        rule_result = results.by_rule_id["BilledCost-C-005-C"]
        
        self.assertTrue(rule_result.get("ok"), 
                       f"BilledCost-C-005-C should PASS when condition not met but got: {rule_result}")
        self.assertEqual(rule_result["details"]["violations"], 0)

    def test_billed_cost_conditional_rule_mixed_scenarios(self):
        """
        Test BilledCost-C-005-C with mixed data:
        - Some rows meet condition with BilledCost = 0 (should pass)
        - Some rows meet condition with BilledCost != 0 (should fail)
        - Some rows don't meet condition (should pass regardless of BilledCost)
        """
        # Mixed CSV data
        csv_data = """InvoiceIssuerName,ResourceID,ProviderName,BilledCost,AvailabilityZone
"marketplace-vendor","r1","AWS",0,"us-east-1"
"AWS","r2","AWS",100.50,"us-east-1"
"third-party-billing","r3","Azure",15.75,"westus2"
"Azure","r4","Azure",0,"westus2"
"""
        
        # Convert to DataFrame
        df = pd.read_csv(StringIO(csv_data))
        
        # Run validation
        results = self.spec_rules.validate(focus_data=df)
        
        # Assert BilledCost-C-005-C fails (one row violates: third-party-billing/Azure with BilledCost=15.75)
        self.assertIn("BilledCost-C-005-C", results.by_rule_id)
        rule_result = results.by_rule_id["BilledCost-C-005-C"]
        
        self.assertFalse(rule_result.get("ok"), 
                        f"BilledCost-C-005-C should FAIL due to mixed violations but got: {rule_result}")
        # Should have exactly 1 violation (row 3: third-party-billing/Azure with BilledCost=15.75)
        self.assertEqual(rule_result["details"]["violations"], 1)

    def test_billed_cost_conditional_rule_null_values(self):
        """
        Test BilledCost-C-005-C with null values in key columns.
        """
        # CSV data with null values
        csv_data = """InvoiceIssuerName,ResourceID,ProviderName,BilledCost,AvailabilityZone
"","r1","AWS",10.50,"us-east-1"
"Azure","r2","",25.75,"westus2"
"""
        
        # Convert to DataFrame
        df = pd.read_csv(StringIO(csv_data))
        
        # Run validation
        results = self.spec_rules.validate(focus_data=df)
        
        # Assert rule behavior with null values
        self.assertIn("BilledCost-C-005-C", results.by_rule_id)
        rule_result = results.by_rule_id["BilledCost-C-005-C"]
        
        # The rule should pass because null values don't meet the condition 
        # (ProviderName IS NOT NULL AND InvoiceIssuerName IS NOT NULL AND ProviderName <> InvoiceIssuerName)
        self.assertTrue(rule_result.get("ok"), 
                       f"BilledCost-C-005-C should PASS with null values but got: {rule_result}")

    def test_validation_results_structure(self):
        """Test that ValidationResults structure is correct."""
        # Simple test data
        csv_data = """InvoiceIssuerName,ResourceID,ProviderName,BilledCost,AvailabilityZone
"AWS","r1","AWS",0,"us-east-1"
"""
        
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)
        
        # Verify ValidationResults structure
        self.assertIsInstance(results, ValidationResults)
        self.assertTrue(hasattr(results, 'by_idx'))
        self.assertTrue(hasattr(results, 'by_rule_id'))
        self.assertTrue(hasattr(results, 'rules'))
        
        # Verify rule appears in both indices
        self.assertIn("BilledCost-C-005-C", results.by_rule_id)
        
        # Verify results contain expected fields
        rule_result = results.by_rule_id["BilledCost-C-005-C"]
        self.assertIn("ok", rule_result)
        self.assertIn("details", rule_result)
        self.assertIn("rule_id", rule_result)
        self.assertEqual(rule_result["rule_id"], "BilledCost-C-005-C")


if __name__ == '__main__':
    unittest.main()