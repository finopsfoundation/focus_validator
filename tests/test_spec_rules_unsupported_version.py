from unittest import TestCase
from unittest.mock import MagicMock

from focus_validator.exceptions import UnsupportedVersion
from focus_validator.rules.spec_rules import SpecRules


class TestSpecRulesUnsupportedVersion(TestCase):
    def test_load_unsupported_version(self):
        with self.assertRaises(UnsupportedVersion) as cm:
            SpecRules(
                rule_set_path="focus_validator/rules",
                rules_file_prefix="model-",
                rules_version="0.1",
                rules_file_suffix=".json",
                focus_dataset="CostAndUsage",
                filter_rules=None,
                rules_force_remote_download=False,
                rules_block_remote_download=True,
                allow_draft_releases=False,
                allow_prerelease_releases=False,
                column_namespace=None,
            )
        self.assertIn("FOCUS version 0.1 not supported.", str(cm.exception))


class TestSpecRulesVersionMatching(TestCase):
    """Test version-matching helper methods."""

    def setUp(self):
        """Create a SpecRules instance for testing helper methods."""
        self.spec = SpecRules(
            rule_set_path="focus_validator/rules",
            rules_file_prefix="model-",
            rules_version="1.2",  # Use 1.2 which will match to available 1.2.0.1
            rules_file_suffix=".json",
            focus_dataset="CostAndUsage",
            filter_rules=None,
            rules_force_remote_download=False,
            rules_block_remote_download=True,
            allow_draft_releases=False,
            allow_prerelease_releases=False,
            column_namespace=None,
        )

    def test_parse_version_from_filename_valid(self):
        """Test extracting version from valid filenames."""
        test_cases = [
            ("model-1.2.json", "1.2"),
            ("model-1.2.0.1.json", "1.2.0.1"),
            ("model-2.0.0.json", "2.0.0"),
        ]
        for filename, expected in test_cases:
            with self.subTest(filename=filename):
                result = self.spec._parse_version_from_filename(filename)
                self.assertEqual(result, expected)

    def test_parse_version_from_filename_invalid(self):
        """Test that invalid filenames return None."""
        test_cases = [
            "requirements-1.3.json",  # Wrong prefix
            "model-1.3.txt",  # Wrong suffix
            "model-.json",  # Empty version
            "random.json",  # No match
        ]
        for filename in test_cases:
            with self.subTest(filename=filename):
                result = self.spec._parse_version_from_filename(filename)
                self.assertIsNone(result)

    def test_parse_version_tuple_valid(self):
        """Test semantic version tuple parsing for valid versions."""
        test_cases = [
            ("1", (1,)),
            ("1.2", (1, 2)),
            ("1.2.0", (1, 2, 0)),
            ("1.2.0.1", (1, 2, 0, 1)),
            ("2.0.0.0", (2, 0, 0, 0)),
        ]
        for version, expected in test_cases:
            with self.subTest(version=version):
                result = self.spec._parse_version_tuple(version)
                self.assertEqual(result, expected)

    def test_parse_version_tuple_comparison(self):
        """Test that version tuples compare correctly."""
        versions = ["1.2", "1.2.0", "1.2.0.1", "1.3", "2.0"]
        tuples = [self.spec._parse_version_tuple(v) for v in versions]

        # Verify they're in ascending order
        for i in range(len(tuples) - 1):
            with self.subTest(comparison=f"{versions[i]} < {versions[i + 1]}"):
                self.assertLess(tuples[i], tuples[i + 1])

    def test_parse_version_tuple_malformed_logs_warning(self):
        """Test that malformed versions log warnings and return (0,)."""
        # Mock the logger to capture warnings
        self.spec.log = MagicMock()

        malformed_versions = [
            "1.2.abc",  # Non-numeric component
            "1.2.3.x",  # Non-numeric component
            "invalid",  # Completely invalid
        ]

        for version in malformed_versions:
            with self.subTest(version=version):
                result = self.spec._parse_version_tuple(version)
                self.assertEqual(result, (0,))
                # Verify warning was logged
                self.spec.log.warning.assert_called()
                # Check warning message mentions the malformed version
                call_args = str(self.spec.log.warning.call_args)
                self.assertIn(version, call_args)

    def test_find_best_version_match_exact(self):
        """Test finding exact version matches."""
        available = {
            "1.2": {},
            "1.3": {},
            "2.0": {},
        }

        test_cases = [
            ("1.2", "1.2"),
            ("1.3", "1.3"),
            ("2.0", "2.0"),
        ]

        for requested, expected in test_cases:
            with self.subTest(requested=requested):
                result = self.spec._find_best_version_match(requested, available)
                self.assertEqual(result, expected)

    def test_find_best_version_match_prefix(self):
        """Test finding highest version matching a prefix."""
        available = {
            "1.2": {},
            "1.2.0": {},
            "1.2.0.1": {},
            "1.2.0.2": {},
            "1.3": {},
        }

        test_cases = [
            ("1.2", "1.2.0.2", "Should find highest 1.2.x version"),
            ("1.2.0", "1.2.0.2", "Should find highest 1.2.0.x version"),
            ("1.3", "1.3", "Should find exact match"),
        ]

        for requested, expected, description in test_cases:
            with self.subTest(description=description):
                result = self.spec._find_best_version_match(requested, available)
                self.assertEqual(result, expected)

    def test_find_best_version_match_ignores_higher_minor(self):
        """Test that 1.2.0 request ignores 1.2.1 and higher."""
        available = {
            "1.2.0": {},
            "1.2.0.1": {},
            "1.2.0.2": {},
            "1.2.1": {},  # Should be ignored
            "1.2.1.1": {},  # Should be ignored
        }

        result = self.spec._find_best_version_match("1.2.0", available)
        self.assertEqual(result, "1.2.0.2")

    def test_find_best_version_match_short_prefix(self):
        """Test matching with very short prefixes like '1'."""
        available = {
            "1.0": {},
            "1.2": {},
            "1.2.0.1": {},
            "1.3": {},
            "2.0": {},  # Should be ignored
        }

        result = self.spec._find_best_version_match("1", available)
        self.assertEqual(result, "1.3")  # Highest 1.x version

    def test_find_best_version_match_no_match(self):
        """Test that non-existent versions return None."""
        available = {
            "1.2": {},
            "1.3": {},
        }

        test_cases = [
            "2.0",  # Major version doesn't exist
            "1.4",  # Minor version doesn't exist
            "1.2.1",  # Patch version doesn't exist
        ]

        for requested in test_cases:
            with self.subTest(requested=requested):
                result = self.spec._find_best_version_match(requested, available)
                self.assertIsNone(result)

    def test_filter_to_highest_versions_mixed(self):
        """Test filtering to highest versions with mixed subversions."""
        versions = ["1.2", "1.2.0", "1.2.0.1", "1.3", "1.3.0.1"]
        result = self.spec._filter_to_highest_versions(versions)
        self.assertEqual(result, ["1.2.0.1", "1.3.0.1"])

    def test_filter_to_highest_versions_no_subversions(self):
        """Test filtering when no subversions exist."""
        versions = ["1.2", "1.3", "2.0"]
        result = self.spec._filter_to_highest_versions(versions)
        self.assertEqual(result, ["1.2", "1.3", "2.0"])

    def test_filter_to_highest_versions_multiple_patches(self):
        """Test filtering with multiple patch levels."""
        versions = ["1.2", "1.2.0", "1.2.0.1", "1.2.0.2", "1.2.1", "1.2.1.1"]
        result = self.spec._filter_to_highest_versions(versions)
        # Should keep highest for 1.2.0.x and highest for 1.2.1.x
        # But since we group by major.minor only, should keep overall highest 1.2.x
        # which is 1.2.1.1
        self.assertEqual(result, ["1.2.1.1"])

    def test_filter_to_highest_versions_preserves_order(self):
        """Test that filtered versions are sorted semantically."""
        versions = ["2.0", "1.3.0.1", "1.2.0.1", "1.2"]
        result = self.spec._filter_to_highest_versions(versions)
        # Should return semantically sorted
        expected = sorted([v for v in result], key=self.spec._parse_version_tuple)
        self.assertEqual(result, expected)
