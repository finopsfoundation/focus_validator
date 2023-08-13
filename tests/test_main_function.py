from unittest import TestCase
from unittest.mock import patch

from focus_validator.main import main
from focus_validator.validator import Validator


class TestMainFunction(TestCase):
    @patch.object(Validator, "validate")
    def test_required_data_file(self, *_args):
        with patch("sys.argv", ["file.py", "-h"]):
            with self.assertRaises(SystemExit) as cm:
                main()
            self.assertEqual(cm.exception.code, 0)

    def test_supported_versions(self):
        with patch("sys.argv", ["prog", "--supported-versions"]):
            try:
                main()
            except SystemExit as e:
                self.assertNotEqual(e.code, 2)

    @patch.object(Validator, "validate")
    def test_data_file(self, *_args):
        with patch("sys.argv", ["prog", "--data-file", "path/to/data.csv"]):
            try:
                main()
            except SystemExit as e:
                self.assertNotEqual(e.code, 2)

    @patch.object(Validator, "validate")
    def test_column_namespace(self, *_args):
        with patch(
            "sys.argv",
            [
                "prog",
                "--data-file",
                "path/to/data.csv",
                "--column-namespace",
                "namespace",
            ],
        ):
            try:
                main()
            except SystemExit as e:
                self.assertNotEqual(e.code, 2)

    @patch.object(Validator, "validate")
    def test_override_file(self, *_args):
        with patch(
            "sys.argv",
            [
                "prog",
                "--data-file",
                "path/to/data.csv",
                "--override-file",
                "path/to/override.yaml",
            ],
        ):
            try:
                main()
            except SystemExit as e:
                self.assertNotEqual(e.code, 2)

    @patch.object(Validator, "validate")
    def test_output_format(self, *_args):
        with patch(
            "sys.argv",
            ["prog", "--data-file", "path/to/data.csv", "--output-format", "json"],
        ):
            try:
                main()
            except SystemExit as e:
                self.assertNotEqual(e.code, 2)
