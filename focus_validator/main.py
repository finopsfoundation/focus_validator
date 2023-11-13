import argparse
import os
import sys

from focus_validator.validator import Validator


def main():
    parser = argparse.ArgumentParser(description="FOCUS specification validator.")
    parser.add_argument(
        "--data-file",
        help="Path to the data file (CSV)",
        required="--supported-versions" not in sys.argv,
    )
    parser.add_argument(
        "--column-namespace",
        help="Column namespace to differentiate focus columns from vendor columns",
    )
    parser.add_argument("--override-file", help="Path to the override file (YAML)")
    parser.add_argument(
        "--output-format", default="text", help="Path to the output report file"
    )
    parser.add_argument(
        "--supported-versions",
        action="store_true",
        default=False,
        help="Return the supported FOCUS versions for validation",
    )
    parser.add_argument(
        "--transitional",
        action="store_true",
        default=False,
        help="Allow transitional rules in validation",
    )
    parser.add_argument(
        "--validate-version", default="1.0", help="Version of FOCUS to validate against"
    )
    parser.add_argument(
        "--rule-set-path",
        default=os.path.join("focus_validator", "rules", "version_sets"),
        help="Path to rules definitions",
    )
    parser.add_argument(
        "--output-type",
        default="console",
        help="What type of output you would like",
        choices=["console", "unittest"],
    )
    parser.add_argument(
        "--output-destination",
        default=None,
        help="filename of where to output the rules",
    )

    args = parser.parse_args()

    if args.output_type != "console" and args.output_destination is None:
        parser.error("--output-destination required {}".format(args.output_type))
        sys.exit(1)

    validator = Validator(
        data_filename=args.data_file,
        override_filename=args.override_file,
        rule_set_path=args.rule_set_path,
        rules_version=args.validate_version,
        output_type=args.output_type,
        output_destination=args.output_destination,
        column_namespace=args.column_namespace,
    )
    if args.supported_versions:
        for version in validator.get_supported_versions():
            print(version)
    else:
        validator.validate()


if __name__ == "__main__":
    main()
