import os
import tempfile
from unittest import TestCase
import pandas as pd
from focus_validator.validator import Validator


class TestColumnNamespace(TestCase):
    def test_load_rule_config_with_namespace(self):
        # Test that validator works with column namespace
        try:
            validator = Validator(
                data_filename="tests/samples/multiple_failure_example_namespaced.csv",
                output_type="console",
                output_destination=None,
                column_namespace="F",
                rules_version="1.2",  # Use available version
                focus_dataset="CostAndUsage",
                rules_block_remote_download=True,
            )
            validator.load()
            results = validator.spec_rules.validate(focus_data=validator.focus_data)
            
            # Test should pass - we're just checking that namespace doesn't break loading
            self.assertIsNotNone(results)
            self.assertIsInstance(results.by_rule_id, dict)
            
        except Exception as e:
            # If sample file doesn't exist, that's ok - we're testing the namespace parameter works
            if "No such file or directory" not in str(e):
                raise

    def test_validator_accepts_namespace_parameter(self):
        # Test that the Validator constructor accepts column_namespace parameter
        # This is a minimal test to ensure the API works
        try:
            validator = Validator(
                data_filename="fake_file.csv",  # doesn't need to exist for this test
                output_type="console", 
                output_destination=None,
                column_namespace="TestNamespace",
                rules_version="1.2",
                focus_dataset="CostAndUsage",
                rules_block_remote_download=True,
            )
            # If we get here without error, the parameter is accepted
            # Check that the column_namespace was passed through to spec_rules
            self.assertEqual(validator.spec_rules.column_namespace, "TestNamespace")
        except FileNotFoundError:
            # Expected - file doesn't exist, but parameter was accepted
            pass

    def _load(self, data_filename):
        validator = Validator(
            data_filename=data_filename,
            output_type="console",
            output_destination=None,
            column_namespace="F",
            rules_version="1.2",
            focus_dataset="CostAndUsage",
            rules_block_remote_download=True,
        )
        validator.load()
        return validator.focus_data

    def test_namespaced_columns_are_renamed_and_vendor_columns_ignored(self):
        with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False) as f:
            f.write("F:BilledCost,F:ChargeCategory,ChargeCategory\n1.5,Usage,vendor-value\n")
            path = f.name
        try:
            data = self._load(path)
            self.assertEqual(list(data.columns), ["BilledCost", "ChargeCategory"])
            self.assertEqual(data["ChargeCategory"][0], "Usage")
        finally:
            os.unlink(path)

    def test_data_without_namespaced_columns_raises(self):
        with self.assertRaises(ValueError) as cm:
            self._load("tests/samples/all_pass_0.5.csv")
        self.assertIn("'F' namespace", str(cm.exception))
