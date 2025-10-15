"""Comprehensive tests for ModelRule and ValidationCriteria models."""

import unittest
from unittest.mock import Mock, patch
import json
from typing import Dict, Any, List, Optional

# Mock dependencies that might not be available
import sys
from unittest.mock import MagicMock
sys.modules['sqlglot'] = MagicMock()
sys.modules['sqlglot.exp'] = MagicMock()

from focus_validator.config_objects.rule import (
    ModelRule,
    ValidationCriteria,
    InvalidRule,
    CompositeCheck,
    ChecklistObject,
)
from focus_validator.config_objects.common import ChecklistObjectStatus


class TestValidationCriteria(unittest.TestCase):
    """Test ValidationCriteria model."""

    def setUp(self):
        """Set up test fixtures."""
        self.valid_criteria_data = {
            "MustSatisfy": "All rows",
            "Keyword": "CheckValue", 
            "Requirement": {"operator": "equals", "value": "test"},
            "Condition": {"column": "status", "value": "active"},
            "Dependencies": ["MODEL-001", "MODEL-002"]
        }

    def test_valid_creation_with_aliases(self):
        """Test creating ValidationCriteria with field aliases."""
        criteria = ValidationCriteria(**self.valid_criteria_data)
        
        self.assertEqual(criteria.must_satisfy, "All rows")
        self.assertEqual(criteria.keyword, "CheckValue")
        self.assertEqual(criteria.requirement, {"operator": "equals", "value": "test"})
        self.assertEqual(criteria.condition, {"column": "status", "value": "active"})
        self.assertEqual(criteria.dependencies, ["MODEL-001", "MODEL-002"])

    def test_valid_creation_with_field_names(self):
        """Test creating ValidationCriteria with field names instead of aliases."""
        field_name_data = {
            "must_satisfy": "All rows",
            "keyword": "CheckValue",
            "requirement": {"operator": "equals", "value": "test"},
            "condition": {"column": "status", "value": "active"},
            "dependencies": ["MODEL-001", "MODEL-002"]
        }
        
        criteria = ValidationCriteria(**field_name_data)
        
        self.assertEqual(criteria.must_satisfy, "All rows")
        self.assertEqual(criteria.keyword, "CheckValue")
        self.assertEqual(criteria.requirement, {"operator": "equals", "value": "test"})

    def test_empty_dependencies(self):
        """Test ValidationCriteria with empty dependencies list."""
        data = self.valid_criteria_data.copy()
        data["Dependencies"] = []
        
        criteria = ValidationCriteria(**data)
        self.assertEqual(criteria.dependencies, [])

    def test_complex_requirement(self):
        """Test ValidationCriteria with complex requirement structure."""
        data = self.valid_criteria_data.copy()
        data["Requirement"] = {
            "checks": [
                {"type": "format", "format_type": "datetime"},
                {"type": "value_in", "values": ["USD", "EUR", "GBP"]}
            ],
            "logic": "AND"
        }
        
        criteria = ValidationCriteria(**data)
        self.assertIn("checks", criteria.requirement)
        self.assertEqual(criteria.requirement["logic"], "AND")

    def test_precondition_property(self):
        """Test precondition property getter and setter."""
        criteria = ValidationCriteria(**self.valid_criteria_data)
        
        # Initially None
        self.assertIsNone(criteria.precondition)
        
        # Set precondition
        precond = {"column": "active", "value": True}
        criteria.precondition = precond
        self.assertEqual(criteria.precondition, precond)

    def test_precondition_immutable_after_set(self):
        """Test that precondition cannot be modified after being set."""
        criteria = ValidationCriteria(**self.valid_criteria_data)
        
        criteria.precondition = {"column": "test", "value": "initial"}
        
        with self.assertRaises(ValueError) as cm:
            criteria.precondition = {"column": "test", "value": "modified"}
        
        self.assertIn("already set and cannot be modified", str(cm.exception))

    def test_precondition_type_validation(self):
        """Test that precondition must be dict or None."""
        criteria = ValidationCriteria(**self.valid_criteria_data)
        
        with self.assertRaises(TypeError) as cm:
            criteria.precondition = "invalid_string"
        
        self.assertIn("must be a dict or None", str(cm.exception))

        with self.assertRaises(TypeError) as cm:
            criteria.precondition = 123
        
        self.assertIn("must be a dict or None", str(cm.exception))

    def test_model_dump(self):
        """Test model serialization."""
        criteria = ValidationCriteria(**self.valid_criteria_data)
        data = criteria.model_dump()
        
        # Should use field names in output
        expected_keys = {"must_satisfy", "keyword", "requirement", "condition", "dependencies"}
        self.assertEqual(set(data.keys()), expected_keys)

    def test_model_dump_by_alias(self):
        """Test model serialization with aliases."""
        criteria = ValidationCriteria(**self.valid_criteria_data)
        data = criteria.model_dump(by_alias=True)
        
        # Should use aliases in output
        expected_keys = {"MustSatisfy", "Keyword", "Requirement", "Condition", "Dependencies"}
        self.assertEqual(set(data.keys()), expected_keys)


class TestModelRule(unittest.TestCase):
    """Test ModelRule model."""

    def setUp(self):
        """Set up test fixtures."""
        self.valid_rule_data = {
            "Function": "CheckValue",
            "Reference": "FOCUS-1.0#billing_account_id",
            "EntityType": "Column",
            "ModelVersionIntroduced": "1.0",
            "Status": "Active",
            "ApplicabilityCriteria": ["CostAndUsage"],
            "Type": "Static",
            "ValidationCriteria": {
                "MustSatisfy": "All rows",
                "Keyword": "CheckValue",
                "Requirement": {"operator": "not_equals", "value": None},
                "Condition": {},
                "Dependencies": []
            }
        }

    def test_valid_creation_with_aliases(self):
        """Test creating ModelRule with field aliases."""
        rule = ModelRule(**self.valid_rule_data)
        
        self.assertEqual(rule.function, "CheckValue")
        self.assertEqual(rule.reference, "FOCUS-1.0#billing_account_id")
        self.assertEqual(rule.entity_type, "Column")
        self.assertEqual(rule.model_version_introduced, "1.0")
        self.assertEqual(rule.status, "Active")
        self.assertEqual(rule.applicability_criteria, ["CostAndUsage"])
        self.assertEqual(rule.type, "Static")
        self.assertIsInstance(rule.validation_criteria, ValidationCriteria)

    def test_optional_notes_field(self):
        """Test ModelRule with optional notes field."""
        data = self.valid_rule_data.copy()
        data["Notes"] = "Additional information about this rule"
        
        rule = ModelRule(**data)
        self.assertEqual(rule.notes, "Additional information about this rule")

    def test_rule_id_property(self):
        """Test rule_id property getter and setter."""
        rule = ModelRule(**self.valid_rule_data)
        
        # Initially None
        self.assertIsNone(rule.rule_id)
        
        # Set rule_id
        rule.rule_id = "MODEL-001"
        self.assertEqual(rule.rule_id, "MODEL-001")

    def test_rule_id_immutable_after_set(self):
        """Test that rule_id cannot be modified after being set."""
        rule = ModelRule(**self.valid_rule_data)
        
        rule.rule_id = "MODEL-001"
        
        with self.assertRaises(ValueError) as cm:
            rule.rule_id = "MODEL-002"
        
        self.assertIn("already set and cannot be modified", str(cm.exception))

    def test_rule_id_type_validation(self):
        """Test that rule_id must be string."""
        rule = ModelRule(**self.valid_rule_data)
        
        with self.assertRaises(TypeError) as cm:
            rule.rule_id = 123
        
        self.assertIn("must be a string", str(cm.exception))

    def test_with_rule_id_method(self):
        """Test with_rule_id fluent interface method."""
        rule = ModelRule(**self.valid_rule_data)
        
        result = rule.with_rule_id("MODEL-123")
        
        # Should return the same instance
        self.assertIs(result, rule)
        self.assertEqual(rule.rule_id, "MODEL-123")

    def test_is_active_method(self):
        """Test is_active method."""
        # Active rule
        rule = ModelRule(**self.valid_rule_data)
        self.assertTrue(rule.is_active())
        
        # Inactive rule
        inactive_data = self.valid_rule_data.copy()
        inactive_data["Status"] = "Deprecated"
        inactive_rule = ModelRule(**inactive_data)
        self.assertFalse(inactive_rule.is_active())

    def test_is_dynamic_method(self):
        """Test is_dynamic method."""
        # Static rule
        rule = ModelRule(**self.valid_rule_data)
        self.assertFalse(rule.is_dynamic())
        
        # Dynamic rule
        dynamic_data = self.valid_rule_data.copy()
        dynamic_data["Type"] = "Dynamic"
        dynamic_rule = ModelRule(**dynamic_data)
        self.assertTrue(dynamic_rule.is_dynamic())

    def test_is_composite_method(self):
        """Test is_composite method."""
        # Non-composite rule
        rule = ModelRule(**self.valid_rule_data)
        self.assertFalse(rule.is_composite())
        
        # Composite rule
        composite_data = self.valid_rule_data.copy()
        composite_data["Function"] = "Composite"
        composite_rule = ModelRule(**composite_data)
        self.assertTrue(composite_rule.is_composite())

    def test_model_dump(self):
        """Test model serialization."""
        rule = ModelRule(**self.valid_rule_data)
        data = rule.model_dump()
        
        # Should contain all expected fields
        expected_fields = {
            "function", "reference", "entity_type", "model_version_introduced",
            "status", "applicability_criteria", "type", "validation_criteria"
        }
        self.assertTrue(expected_fields.issubset(set(data.keys())))

    def test_nested_validation_criteria(self):
        """Test that ValidationCriteria is properly nested and validated."""
        rule = ModelRule(**self.valid_rule_data)
        
        criteria = rule.validation_criteria
        self.assertIsInstance(criteria, ValidationCriteria)
        self.assertEqual(criteria.must_satisfy, "All rows")
        self.assertEqual(criteria.keyword, "CheckValue")


class TestInvalidRule(unittest.TestCase):
    """Test InvalidRule model."""

    def test_valid_creation(self):
        """Test creating InvalidRule with valid data."""
        invalid_rule = InvalidRule(
            rule_path="rules/model-001.json",
            error="Missing required field 'Function'",
            error_type="ValidationError"
        )
        
        self.assertEqual(invalid_rule.rule_path, "rules/model-001.json")
        self.assertEqual(invalid_rule.error, "Missing required field 'Function'")
        self.assertEqual(invalid_rule.error_type, "ValidationError")

    def test_empty_strings(self):
        """Test InvalidRule with empty strings."""
        invalid_rule = InvalidRule(
            rule_path="",
            error="",
            error_type=""
        )
        
        self.assertEqual(invalid_rule.rule_path, "")
        self.assertEqual(invalid_rule.error, "")
        self.assertEqual(invalid_rule.error_type, "")

    def test_model_dump(self):
        """Test model serialization."""
        invalid_rule = InvalidRule(
            rule_path="test/path",
            error="Test error",
            error_type="TestError"
        )
        
        data = invalid_rule.model_dump()
        expected = {
            "rule_path": "test/path",
            "error": "Test error", 
            "error_type": "TestError"
        }
        self.assertEqual(data, expected)


class TestCompositeCheck(unittest.TestCase):
    """Test CompositeCheck model."""

    def test_valid_creation_and_logic(self):
        """Test creating CompositeCheck with AND logic."""
        check = CompositeCheck(
            logic_operator="AND",
            dependency_rule_ids=["MODEL-001", "MODEL-002", "MODEL-003"]
        )
        
        self.assertEqual(check.logic_operator, "AND")
        self.assertEqual(check.dependency_rule_ids, ["MODEL-001", "MODEL-002", "MODEL-003"])

    def test_valid_creation_or_logic(self):
        """Test creating CompositeCheck with OR logic."""
        check = CompositeCheck(
            logic_operator="OR",
            dependency_rule_ids=["MODEL-001", "MODEL-002"]
        )
        
        self.assertEqual(check.logic_operator, "OR")
        self.assertEqual(check.dependency_rule_ids, ["MODEL-001", "MODEL-002"])

    def test_empty_dependency_list(self):
        """Test CompositeCheck with empty dependency list."""
        check = CompositeCheck(
            logic_operator="AND",
            dependency_rule_ids=[]
        )
        
        self.assertEqual(check.dependency_rule_ids, [])

    def test_model_dump(self):
        """Test model serialization."""
        check = CompositeCheck(
            logic_operator="OR",
            dependency_rule_ids=["MODEL-A", "MODEL-B"]
        )
        
        data = check.model_dump()
        expected = {
            "logic_operator": "OR",
            "dependency_rule_ids": ["MODEL-A", "MODEL-B"]
        }
        self.assertEqual(data, expected)


class TestChecklistObject(unittest.TestCase):
    """Test ChecklistObject model."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_rule = Mock(spec=ModelRule)
        self.mock_rule.rule_id = "MODEL-001"
        self.mock_rule.function = "CheckValue"

    def test_valid_creation_with_model_rule(self):
        """Test creating ChecklistObject with ModelRule."""
        checklist_obj = ChecklistObject(
            check_name="billing_account_id_not_null",
            rule_id="MODEL-001",
            friendly_name="Billing Account ID Check",
            status=ChecklistObjectStatus.PASSED,
            rule_ref=self.mock_rule
        )
        
        self.assertEqual(checklist_obj.check_name, "billing_account_id_not_null")
        self.assertEqual(checklist_obj.rule_id, "MODEL-001")
        self.assertEqual(checklist_obj.friendly_name, "Billing Account ID Check")
        self.assertEqual(checklist_obj.status, ChecklistObjectStatus.PASSED)
        self.assertEqual(checklist_obj.rule_ref, self.mock_rule)

    def test_valid_creation_with_invalid_rule(self):
        """Test creating ChecklistObject with InvalidRule."""
        invalid_rule = InvalidRule(
            rule_path="rules/broken.json",
            error="Parse error",
            error_type="JSONError"
        )
        
        checklist_obj = ChecklistObject(
            check_name="broken_check",
            rule_id="MODEL-BROKEN",
            status=ChecklistObjectStatus.ERRORED,
            rule_ref=invalid_rule,
            error="Rule parsing failed"
        )
        
        self.assertEqual(checklist_obj.status, ChecklistObjectStatus.ERRORED)
        self.assertEqual(checklist_obj.error, "Rule parsing failed")
        self.assertIsInstance(checklist_obj.rule_ref, InvalidRule)

    def test_optional_fields(self):
        """Test ChecklistObject with optional fields."""
        checklist_obj = ChecklistObject(
            check_name="simple_check",
            rule_id="MODEL-002",
            status=ChecklistObjectStatus.SKIPPED,
            rule_ref=self.mock_rule,
            reason="Condition not met"
        )
        
        self.assertIsNone(checklist_obj.friendly_name)
        self.assertIsNone(checklist_obj.error)
        self.assertEqual(checklist_obj.reason, "Condition not met")

    def test_different_status_values(self):
        """Test ChecklistObject with different status values."""
        status_values = [
            ChecklistObjectStatus.PASSED,
            ChecklistObjectStatus.FAILED,
            ChecklistObjectStatus.ERRORED,
            ChecklistObjectStatus.SKIPPED,
            ChecklistObjectStatus.PENDING
        ]
        
        for status in status_values:
            with self.subTest(status=status):
                checklist_obj = ChecklistObject(
                    check_name=f"check_{status.value}",
                    rule_id=f"MODEL-{status.value.upper()}",
                    status=status,
                    rule_ref=self.mock_rule
                )
                self.assertEqual(checklist_obj.status, status)

    def test_model_dump(self):
        """Test model serialization."""
        # Create a real InvalidRule instead of using Mock for serialization
        invalid_rule = InvalidRule(
            rule_path="test/path/rule.json",
            error="Test validation error",
            error_type="ValidationError"
        )
        
        checklist_obj = ChecklistObject(
            check_name="test_check",
            rule_id="MODEL-TEST",
            friendly_name="Test Check",
            status=ChecklistObjectStatus.FAILED,
            rule_ref=invalid_rule,
            error="Validation failed",
            reason="Value out of range"
        )
        
        data = checklist_obj.model_dump()
        
        self.assertEqual(data["check_name"], "test_check")
        self.assertEqual(data["rule_id"], "MODEL-TEST")
        self.assertEqual(data["friendly_name"], "Test Check")
        self.assertEqual(data["status"], ChecklistObjectStatus.FAILED)
        self.assertEqual(data["error"], "Validation failed")
        self.assertEqual(data["reason"], "Value out of range")
        # Verify that rule_ref was serialized as a dict
        self.assertIsInstance(data["rule_ref"], dict)


class TestIntegrationScenarios(unittest.TestCase):
    """Test integration scenarios combining multiple models."""

    def test_complete_rule_lifecycle(self):
        """Test complete rule creation and validation lifecycle."""
        # Create ValidationCriteria
        criteria_data = {
            "MustSatisfy": "All rows",
            "Keyword": "CheckValue",
            "Requirement": {"operator": "not_equals", "value": None},
            "Condition": {"column": "status", "value": "active"},
            "Dependencies": []
        }
        criteria = ValidationCriteria(**criteria_data)
        
        # Create ModelRule with the criteria
        rule_data = {
            "Function": "CheckValue",
            "Reference": "FOCUS-1.0#test_column",
            "EntityType": "Column",
            "ModelVersionIntroduced": "1.0",
            "Status": "Active",
            "ApplicabilityCriteria": ["CostAndUsage"],
            "Type": "Static",
            "ValidationCriteria": criteria_data
        }
        rule = ModelRule(**rule_data)
        rule.rule_id = "MODEL-TEST-001"
        
        # Create ChecklistObject for the rule
        checklist_obj = ChecklistObject(
            check_name="test_column_not_null",
            rule_id="MODEL-TEST-001",
            friendly_name="Test Column Validation",
            status=ChecklistObjectStatus.PASSED,
            rule_ref=rule
        )
        
        # Verify the complete chain works
        self.assertTrue(rule.is_active())
        self.assertFalse(rule.is_dynamic())
        self.assertFalse(rule.is_composite())
        self.assertEqual(checklist_obj.rule_ref.rule_id, "MODEL-TEST-001")
        self.assertEqual(checklist_obj.status, ChecklistObjectStatus.PASSED)

    def test_serialization_roundtrip(self):
        """Test that complex models can be serialized and recreated."""
        # Create a complex ModelRule
        rule_data = {
            "Function": "Composite",
            "Reference": "FOCUS-1.0#complex_check",
            "EntityType": "Row",
            "ModelVersionIntroduced": "1.0",
            "Status": "Active",
            "ApplicabilityCriteria": ["CostAndUsage", "BillingExport"],
            "Type": "Dynamic",
            "ValidationCriteria": {
                "MustSatisfy": "Any row",
                "Keyword": "CompositeCheck",
                "Requirement": {
                    "logic_operator": "OR",
                    "checks": [
                        {"type": "format", "format_type": "datetime"},
                        {"type": "value_in", "values": ["A", "B", "C"]}
                    ]
                },
                "Condition": {"column": "record_type", "value": "billing"},
                "Dependencies": ["MODEL-001", "MODEL-002"]
            },
            "Notes": "Complex validation rule for testing"
        }
        
        # Create and verify
        original_rule = ModelRule(**rule_data)
        
        # Serialize to dict
        serialized = original_rule.model_dump()
        
        # Recreate from serialized data
        recreated_rule = ModelRule(**serialized)
        
        # Verify key properties match
        self.assertEqual(original_rule.function, recreated_rule.function)
        self.assertEqual(original_rule.is_composite(), recreated_rule.is_composite())
        self.assertEqual(original_rule.is_dynamic(), recreated_rule.is_dynamic())
        self.assertEqual(original_rule.validation_criteria.must_satisfy, 
                        recreated_rule.validation_criteria.must_satisfy)

    def test_error_handling_scenarios(self):
        """Test various error handling scenarios."""
        # Test rule with invalid status but valid structure
        try:
            invalid_status_rule = {
                "Function": "CheckValue",
                "Reference": "FOCUS-1.0#test",
                "EntityType": "Column", 
                "ModelVersionIntroduced": "1.0",
                "Status": "InvalidStatus",  # Not a valid status
                "ApplicabilityCriteria": ["CostAndUsage"],
                "Type": "Static",
                "ValidationCriteria": {
                    "MustSatisfy": "All rows",
                    "Keyword": "CheckValue",
                    "Requirement": {"operator": "equals", "value": "test"},
                    "Condition": {},
                    "Dependencies": []
                }
            }
            
            # This should work since we don't validate enum values in the current model
            rule = ModelRule(**invalid_status_rule)
            self.assertEqual(rule.status, "InvalidStatus")
            self.assertFalse(rule.is_active())  # Only "Active" returns True
            
        except Exception as e:
            # If validation is added later, this documents the expected behavior
            self.fail(f"Unexpected error: {e}")


if __name__ == '__main__':
    unittest.main()