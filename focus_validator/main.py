import argparse
import sys

from focus_validator.validator import DEFAULT_VERSION_SETS_PATH, Validator
from focus_validator.utils.download_focus_spec import downloadFocusSpec


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
        default=DEFAULT_VERSION_SETS_PATH,
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
    parser.add_argument(
        "--conformance-dataset",
        default=None,
        help="Specify and validate one of the ConformanceDatasets instead of all",
    )
    parser.add_argument(
        "--visualize",
        action="store_true",
        default=False,
        help="Generate and open visualization of validation results showing passed/failed checks and dependencies",
    )
    parser.add_argument(
        "--force-download",
        action="store_true",
        default=False,
        help="Force download the FOCUS spec JSON from GitHub for the specified version",
    )

    args = parser.parse_args()

    if args.output_type != "console" and args.output_destination is None:
        parser.error("--output-destination required {}".format(args.output_type))
        sys.exit(1)

    # Handle force download before creating validator
    if args.force_download:
        success = downloadFocusSpec(args.validate_version, args.rule_set_path)
        if not success:
            sys.exit(1)

    validator = Validator(
        data_filename=args.data_file,
        rule_set_path=args.rule_set_path,
        rules_version=args.validate_version,
        output_type=args.output_type,
        output_destination=args.output_destination,
        column_namespace=args.column_namespace,
        rule_prefix=args.conformance_dataset
    )
    if args.supported_versions:
        for version in validator.get_supported_versions():
            print(version)
    else:
        results = validator.validate()

        if args.visualize:
            import os
            import subprocess
            from validation_results_visualizer import visualizeValidationResults

            filename = "visualize.svg"

            try:
                visualizeValidationResults(
                    validationResult=results,
                    svgFilename=filename,
                    showPassed=True
                )

                if os.name == 'nt':  # Windows
                    os.startfile(filename)
                elif os.name == 'posix':  # macOS and Linux
                    subprocess.run(['open', filename] if sys.platform == 'darwin' else ['xdg-open', filename])

            except Exception as e:
                print(f"Failed to generate visualization: {e}")


if __name__ == "__main__":
    main()
