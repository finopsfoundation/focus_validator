from __future__ import annotations
import argparse
import io
import logging
import logging.config
import os
import sys
import time
import yaml
from focus_validator.validator import DEFAULT_VERSION_SETS_PATH, Validator
from importlib import resources as ir


def setup_logging(config_path: str | None = None) -> None:
    """
    Tries, in order:
      1) explicit path argument
      2) LOGGING_CONFIG env var
      3) package resource: focus_validator/config/logging.yaml or .ini
      4) fallback basicConfig
    Supports YAML (dictConfig) and INI (fileConfig).
    """
    # 1) explicit path
    if config_path:
        _load_config_path(config_path)
        return

    # 2) env var
    env = os.getenv("LOGGING_CONFIG")
    if env:
        _load_config_path(env)
        return

    # 3) package resource (preferred location)
    for name in ("logging.yaml", "logging.yml", "logging.ini"):
        try:
            res = ir.files("focus_validator.config").joinpath(name)  # package path
            if res.is_file():
                if name.endswith((".yaml", ".yml")):
                    cfg = yaml.safe_load(res.read_text(encoding="utf-8"))
                    logging.config.dictConfig(cfg)
                    return
                else:
                    # INI can be read from a file-like object
                    logging.config.fileConfig(
                        io.StringIO(res.read_text(encoding="utf-8")),
                        defaults={"logfilename": os.getenv("APP_LOG", "app.log")},
                        disable_existing_loggers=False,
                    )
                    return
        except Exception:
            # try next candidate
            pass

    # 4) last-resort fallback
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname).1s %(name)s:%(lineno)d — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )

def _load_config_path(path: str) -> None:
    if path.lower().endswith((".yaml", ".yml")):
        with open(path, "r", encoding="utf-8") as f:
            logging.config.dictConfig(yaml.safe_load(f))
    else:
        # If your INI uses args=(sys.stdout,), ensure sys is imported
        logging.config.fileConfig(
            path,
            defaults={"logfilename": os.getenv("APP_LOG", "app.log")},
            disable_existing_loggers=False,
        )

def main() -> None:
    setup_logging()
    log = logging.getLogger(__name__)

    log.info("=== FOCUS Validator Starting ===")
    log.info("Python version: %s", sys.version.split()[0])
    log.info("Command line: %s", " ".join(sys.argv))
    log.debug("Full arguments: %s", sys.argv)
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
        "--focus-dataset",
        default='CostAndUsage',
        help="Specify which FOCUS datasets to validate against (CostAndUsage)",
    )
    parser.add_argument(
        "--filter-rules",
        default=None,
        help="Filter rules to only those containing this string",
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

    # Log parsed configuration
    log.info("Configuration parsed:")
    log.info("  Data file: %s", args.data_file)
    log.info("  Validation version: %s", args.validate_version)
    log.info("  Focus dataset: %s", args.focus_dataset)
    log.info("  Output type: %s", args.output_type)
    log.info("  Output destination: %s", args.output_destination)
    log.info("  Rule set path: %s", args.rule_set_path)
    log.info("  Transitional rules: %s", args.transitional)
    log.info("  Visualization: %s", args.visualize)
    if args.filter_rules:
        log.info("  Filter rules: %s", args.filter_rules)
    if args.column_namespace:
        log.info("  Column namespace: %s", args.column_namespace)

    if args.output_type != "console" and args.output_destination is None:
        log.error("Output destination required for output type: %s", args.output_type)
        parser.error("--output-destination required {}".format(args.output_type))
        sys.exit(1)

    validator = Validator(
        data_filename=args.data_file,
        rule_set_path=args.rule_set_path,
        rules_version=args.validate_version,
        output_type=args.output_type,
        output_destination=args.output_destination,
        column_namespace=args.column_namespace,
        focus_dataset=args.focus_dataset,
        filter_rules=args.filter_rules,
        rules_force_remote_download=args.force_download,
        allow_draft_releases=args.allow_draft_releases,
        allow_prerelease_releases=args.allow_prerelease_releases,
    )
    if args.supported_versions:
        log.info("Retrieving supported versions...")
        local, remote = validator.get_supported_versions()
        log.info("Local versions: %s", local)
        log.info("Remote versions: %s", remote)
        print("Supported local versions:", local)
        print("Supported remote versions:", remote)
    else:
        log.info("Starting validation process...")
        startTime = time.time()

        try:
            results = validator.validate()
            duration = time.time() - startTime

            # Log validation summary
            if hasattr(results, 'checklist') and results.checklist:
                totalRules = len(results.checklist)
                passedRules = sum(1 for check in results.checklist.values()
                                if hasattr(check, 'status') and check.status.name == 'PASS')
                failedRules = totalRules - passedRules

                log.info("=== Validation Completed ===")
                log.info("Duration: %.3f seconds", duration)
                log.info("Total rules checked: %d", totalRules)
                log.info("Passed: %d", passedRules)
                log.info("Failed: %d", failedRules)
                log.info("Success rate: %.1f%%", (passedRules / totalRules * 100) if totalRules > 0 else 0)
            else:
                log.info("Validation completed in %.3f seconds", duration)

        except Exception as e:
            duration = time.time() - startTime
            log.error("Validation failed after %.3f seconds: %s", duration, str(e))
            raise

        if args.visualize:
            import os
            import subprocess
            from validation_results_visualizer import visualizeValidationResults

            filename = "visualize.svg"

            log.info("Generating visualization: %s", filename)
            try:
                visualizeValidationResults(
                    validationResult=results,
                    svgFilename=filename,
                    showPassed=True,
                    spec_rules_path=validator.get_spec_rules_path(),
                )

                log.info("Visualization generated successfully: %s", filename)

                # Open visualization
                if os.name == 'nt':  # Windows
                    log.debug("Opening visualization with Windows default handler")
                    os.startfile(filename)
                elif os.name == 'posix':  # macOS and Linux
                    openCmd = ['open', filename] if sys.platform == 'darwin' else ['xdg-open', filename]
                    log.debug("Opening visualization with command: %s", openCmd)
                    subprocess.run(openCmd)

            except Exception as e:
                log.error("Failed to generate visualization: %s", str(e))
                print(f"Failed to generate visualization: {e}")

    log.info("=== FOCUS Validator Finished ===")


if __name__ == "__main__":
    main()
