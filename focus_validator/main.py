import argparse
import sys
import logging
import logging.config
import yaml
import os
from focus_validator.validator import DEFAULT_VERSION_SETS_PATH, Validator

with open("logging.yaml") as f:
    logging.config.dictConfig(yaml.safe_load(f))
log = logging.getLogger(__name__)

def main():
    log = logging.getLogger(__name__)
    log.debug("Starting FOCUS Validator from main")
    log.debug("Arguments: %s", sys.argv)
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
    parser.add_argument(
        "--allow-draft-releases",
        action="store_true",
        default=False,
        help="Allow downloading draft releases of the FOCUS spec JSON from GitHub",
    )
    parser.add_argument(
        "--allow-prerelease-releases",
        action="store_true",
        default=False,
        help="Allow downloading prerelease versions of the FOCUS spec JSON from GitHub",
    )

    args = parser.parse_args()

    if args.output_type != "console" and args.output_destination is None:
        parser.error("--output-destination required {}".format(args.output_type))
        sys.exit(1)

    validator = Validator(
        data_filename=args.data_file,
        rule_set_path=args.rule_set_path,
        rules_version=args.validate_version,
        output_type=args.output_type,
        output_destination=args.output_destination,
        column_namespace=args.column_namespace,
        rule_prefix=args.conformance_dataset,
        rules_force_remote_download=args.force_download,
        allow_draft_releases=args.allow_draft_releases,
        allow_prerelease_releases=args.allow_prerelease_releases,
    )
    if args.supported_versions:
        local, remote = validator.get_supported_versions()
        print("Supported local versions:", local)
        print("Supported remote versions:", remote)
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
