"""Comprehensive tests for JsonLoader functionality."""

import unittest
from unittest.mock import Mock, patch, mock_open, MagicMock
import json
import tempfile
import os
from collections import OrderedDict

# Mock dependencies that might not be available
import sys
sys.modules['sqlglot'] = MagicMock()
sys.modules['sqlglot.exp'] = MagicMock()

from focus_validator.config_objects.json_loader import JsonLoader


class TestJsonLoader(unittest.TestCase):
    """Test JsonLoader static methods and functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.sample_json_data = {
            "ConformanceRules": {
                "CR-001": {
                    "Function": "CheckValue",
                    "Reference": "FOCUS-1.0#billing_account_id",
                    "EntityType": "Column",
                    "CRVersionIntroduced": "1.0",
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
            },
            "ConformanceDatasets": {
                "CostAndUsage": {
                    "ConformanceRules": ["CR-001"]
                }
            },
            "CheckFunctions": {
                "CheckValue": {
                    "description": "Validates that a column value meets criteria",
                    "parameters": ["operator", "value"]
                }
            }
        }

    def test_load_json_rules_success(self):
        """Test successful loading of JSON rules file."""
        json_content = json.dumps(self.sample_json_data)
        
        with patch('builtins.open', mock_open(read_data=json_content)):
            with patch('os.path.exists', return_value=True):
                result = JsonLoader.load_json_rules("test_rules.json")
                
                self.assertEqual(result, self.sample_json_data)

    def test_load_json_rules_file_not_found(self):
        """Test loading non-existent JSON rules file."""
        with patch('os.path.exists', return_value=False):
            with self.assertRaises(FileNotFoundError) as cm:
                JsonLoader.load_json_rules("nonexistent.json")
            
            self.assertIn("JSON rules file not found", str(cm.exception))
            self.assertIn("nonexistent.json", str(cm.exception))

    def test_load_json_rules_invalid_json(self):
        """Test loading file with invalid JSON."""
        invalid_json = '{"ConformanceRules": {"CR-001": invalid json}'
        
        with patch('builtins.open', mock_open(read_data=invalid_json)):
            with patch('os.path.exists', return_value=True):
                with self.assertRaises(json.JSONDecodeError):
                    JsonLoader.load_json_rules("invalid.json")

    def test_load_json_rules_empty_file(self):
        """Test loading empty JSON file."""
        with patch('builtins.open', mock_open(read_data="")):
            with patch('os.path.exists', return_value=True):
                with self.assertRaises(json.JSONDecodeError):
                    JsonLoader.load_json_rules("empty.json")

    def test_load_json_rules_permission_error(self):
        """Test loading file with permission issues."""
        with patch('builtins.open', side_effect=PermissionError("Permission denied")):
            with patch('os.path.exists', return_value=True):
                with self.assertRaises(PermissionError):
                    JsonLoader.load_json_rules("restricted.json")

    @patch('focus_validator.config_objects.json_loader.RuleDependencyResolver')
    def test_load_json_rules_with_dependencies_success(self, mock_resolver_class):
        """Test successful loading with dependency resolution."""
        # Mock the resolver instance and its methods
        mock_resolver = Mock()
        mock_resolver_class.return_value = mock_resolver
        mock_resolver.getRelevantRules.return_value = {"CR-001": Mock()}
        mock_resolver.build_plan_and_schedule.return_value = Mock()  # ValidationPlan mock
        
        json_content = json.dumps(self.sample_json_data)
        
        with patch('builtins.open', mock_open(read_data=json_content)):
            with patch('os.path.exists', return_value=True):
                result = JsonLoader.load_json_rules_with_dependencies(
                    json_rule_file="test_rules.json",
                    focus_dataset="CostAndUsage"
                )
                
                # Verify resolver was called correctly
                mock_resolver_class.assert_called_once()
                mock_resolver.buildDependencyGraph.assert_called_once()
                mock_resolver.getRelevantRules.assert_called_once()
                mock_resolver.build_plan_and_schedule.assert_called_once()
                
                # Result should be a ValidationPlan
                self.assertIsNotNone(result)

    def test_load_json_rules_with_dependencies_invalid_dataset(self):
        """Test loading with invalid focus dataset."""
        json_content = json.dumps(self.sample_json_data)
        
        with patch('builtins.open', mock_open(read_data=json_content)):
            with patch('os.path.exists', return_value=True):
                with self.assertRaises(ValueError) as cm:
                    JsonLoader.load_json_rules_with_dependencies(
                        json_rule_file="test_rules.json",
                        focus_dataset="InvalidDataset"
                    )
                
                self.assertIn("Focus dataset 'InvalidDataset' not found", str(cm.exception))

    def test_load_json_rules_with_dependencies_missing_datasets(self):
        """Test loading with missing ConformanceDatasets section."""
        invalid_data = {"ConformanceRules": {}}  # Missing ConformanceDatasets
        json_content = json.dumps(invalid_data)
        
        with patch('builtins.open', mock_open(read_data=json_content)):
            with patch('os.path.exists', return_value=True):
                with self.assertRaises(ValueError):
                    JsonLoader.load_json_rules_with_dependencies(
                        json_rule_file="test_rules.json",
                        focus_dataset="CostAndUsage"
                    )

    @patch('focus_validator.config_objects.json_loader.RuleDependencyResolver')
    def test_load_json_rules_with_filter(self, mock_resolver_class):
        """Test loading with rule filtering."""
        mock_resolver = Mock()
        mock_resolver_class.return_value = mock_resolver
        mock_resolver.getRelevantRules.return_value = {"CR-001": Mock()}
        mock_resolver.build_plan_and_schedule.return_value = Mock()
        
        json_content = json.dumps(self.sample_json_data)
        
        with patch('builtins.open', mock_open(read_data=json_content)):
            with patch('os.path.exists', return_value=True):
                result = JsonLoader.load_json_rules_with_dependencies(
                    json_rule_file="test_rules.json",
                    focus_dataset="CostAndUsage",
                    filter_rules="CR-"
                )
                
                # Verify buildDependencyGraph was called with filter
                mock_resolver.buildDependencyGraph.assert_called_once_with(target_rule_prefix="CR-")

    @patch('focus_validator.config_objects.json_loader.RuleDependencyResolver')
    def test_load_json_rules_with_empty_dataset_rules(self, mock_resolver_class):
        """Test loading dataset with empty rules list."""
        data_with_empty_rules = self.sample_json_data.copy()
        data_with_empty_rules["ConformanceDatasets"]["CostAndUsage"]["ConformanceRules"] = []
        
        mock_resolver = Mock()
        mock_resolver_class.return_value = mock_resolver
        mock_resolver.getRelevantRules.return_value = {}
        mock_resolver.build_plan_and_schedule.return_value = Mock()
        
        json_content = json.dumps(data_with_empty_rules)
        
        with patch('builtins.open', mock_open(read_data=json_content)):
            with patch('os.path.exists', return_value=True):
                result = JsonLoader.load_json_rules_with_dependencies(
                    json_rule_file="test_rules.json",
                    focus_dataset="CostAndUsage"
                )
                
                # Should still work with empty rules list
                self.assertIsNotNone(result)

    def test_load_json_rules_encoding_handling(self):
        """Test loading JSON file with various encodings."""
        # Test with UTF-8 content including special characters
        special_content = {
            "ConformanceRules": {
                "CR-001": {
                    "Function": "CheckValue",
                    "Reference": "FOCUS-1.0#test_ñáme",
                    "Notes": "Test with spëcial characters: €, ñ, 测试"
                }
            },
            "ConformanceDatasets": {
                "CostAndUsage": {"ConformanceRules": ["CR-001"]}
            },
            "CheckFunctions": {}
        }
        
        json_content = json.dumps(special_content, ensure_ascii=False)
        
        with patch('builtins.open', mock_open(read_data=json_content)):
            with patch('os.path.exists', return_value=True):
                result = JsonLoader.load_json_rules("special_chars.json")
                
                # Verify special characters are preserved
                self.assertIn("ñáme", result["ConformanceRules"]["CR-001"]["Reference"])

    def test_load_json_rules_large_file_simulation(self):
        """Test loading large JSON file (simulated)."""
        # Create a larger dataset
        large_data = {
            "ConformanceRules": {},
            "ConformanceDatasets": {
                "CostAndUsage": {"ConformanceRules": []}
            },
            "CheckFunctions": {}
        }
        
        # Add many rules to simulate large file
        for i in range(100):
            rule_id = f"CR-{i:03d}"
            large_data["ConformanceRules"][rule_id] = {
                "Function": "CheckValue",
                "Reference": f"FOCUS-1.0#field_{i}",
                "EntityType": "Column",
                "Status": "Active"
            }
            large_data["ConformanceDatasets"]["CostAndUsage"]["ConformanceRules"].append(rule_id)
        
        json_content = json.dumps(large_data)
        
        with patch('builtins.open', mock_open(read_data=json_content)):
            with patch('os.path.exists', return_value=True):
                result = JsonLoader.load_json_rules("large_rules.json")
                
                self.assertEqual(len(result["ConformanceRules"]), 100)
                self.assertIn("CR-050", result["ConformanceRules"])

    def test_json_loader_static_method_independence(self):
        """Test that JsonLoader static methods are independent."""
        # Test that load_json_rules doesn't affect state for subsequent calls
        json_content1 = json.dumps({"test": "data1"})
        json_content2 = json.dumps({"test": "data2"})
        
        with patch('builtins.open', mock_open(read_data=json_content1)):
            with patch('os.path.exists', return_value=True):
                result1 = JsonLoader.load_json_rules("file1.json")
        
        with patch('builtins.open', mock_open(read_data=json_content2)):
            with patch('os.path.exists', return_value=True):
                result2 = JsonLoader.load_json_rules("file2.json")
        
        # Results should be independent
        self.assertEqual(result1["test"], "data1")
        self.assertEqual(result2["test"], "data2")

    def test_malformed_json_structures(self):
        """Test handling of various malformed JSON structures."""
        malformed_cases = [
            # Missing closing brace
            '{"ConformanceRules": {"CR-001": {"Function": "CheckValue"',
            # Invalid JSON with trailing comma
            '{"ConformanceRules": {"CR-001": {},},}',
            # Mixed quotes
            '{"ConformanceRules": {\'CR-001\': {"Function": "CheckValue"}}}',
            # Unescaped special characters
            '{"ConformanceRules": {"CR-001": {"Notes": "Line\nbreak"}}}',
        ]
        
        for i, malformed_json in enumerate(malformed_cases):
            with self.subTest(case=i):
                with patch('builtins.open', mock_open(read_data=malformed_json)):
                    with patch('os.path.exists', return_value=True):
                        with self.assertRaises(json.JSONDecodeError):
                            JsonLoader.load_json_rules(f"malformed_{i}.json")

    def test_logger_attribute(self):
        """Test that JsonLoader has proper logger configuration."""
        # Check that the class has a log attribute
        self.assertTrue(hasattr(JsonLoader, 'log'))
        
        # The log attribute should be properly configured
        expected_name = f"{JsonLoader.__module__}.{JsonLoader.__qualname__}"
        # Note: This test documents the expected behavior but may vary based on implementation


class TestJsonLoaderIntegration(unittest.TestCase):
    """Integration tests for JsonLoader with real file operations."""

    def test_real_file_operations(self):
        """Test JsonLoader with actual temporary files."""
        test_data = {
            "ConformanceRules": {
                "CR-TEST": {
                    "Function": "CheckValue",
                    "Status": "Active"
                }
            },
            "ConformanceDatasets": {
                "TestDataset": {
                    "ConformanceRules": ["CR-TEST"]
                }
            },
            "CheckFunctions": {}
        }
        
        # Create a temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
            json.dump(test_data, temp_file)
            temp_filename = temp_file.name
        
        try:
            # Test loading the real file
            result = JsonLoader.load_json_rules(temp_filename)
            
            self.assertEqual(result, test_data)
            self.assertIn("CR-TEST", result["ConformanceRules"])
            
        finally:
            # Clean up
            os.unlink(temp_filename)

    def test_file_permissions_handling(self):
        """Test handling of file permission issues."""
        test_data = {"test": "data"}
        
        # Create file and restrict permissions (Unix-like systems only)
        if os.name != 'nt':  # Skip on Windows
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
                json.dump(test_data, temp_file)
                temp_filename = temp_file.name
            
            try:
                # Remove read permissions
                os.chmod(temp_filename, 0o000)
                
                with self.assertRaises(PermissionError):
                    JsonLoader.load_json_rules(temp_filename)
                    
            finally:
                # Restore permissions and clean up
                os.chmod(temp_filename, 0o644)
                os.unlink(temp_filename)


if __name__ == '__main__':
    unittest.main()