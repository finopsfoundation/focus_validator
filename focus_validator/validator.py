import importlib.resources
import logging
import time
import os
from focus_validator.data_loaders import data_loader
from focus_validator.outputter.outputter import Outputter
from focus_validator.rules.spec_rules import SpecRules
from focus_validator.utils.performance_logging import logPerformance

try:
    DEFAULT_VERSION_SETS_PATH = str(
        importlib.resources.files("focus_validator").joinpath("rules")
    )
except AttributeError:
    # for compatibility with python 3.8, which does not support files api in importlib
    from pkg_resources import resource_filename

    DEFAULT_VERSION_SETS_PATH = resource_filename(
        "focus_validator", "rules"
    )


class Validator:
    def __init__(
        self,
        data_filename,
        output_destination,
        output_type,
        rule_set_path=DEFAULT_VERSION_SETS_PATH,
        focus_dataset=None,
        filter_rules=None,
        rules_file_prefix='cr-',
        rules_version=None,
        rules_file_suffix='.json',
        rules_force_remote_download=False,
        allow_draft_releases=False,
        allow_prerelease_releases=False,
        column_namespace=None,
    ):
        self.log = logging.getLogger(f"{__name__}.{self.__class__.__qualname__}")
        self.data_filename = data_filename
        self.focus_data = None

        # Log validator initialization
        self.log.info("Initializing FOCUS Validator")
        self.log.debug("Data file: %s", data_filename)
        self.log.debug("Rule set path: %s", rule_set_path)
        self.log.debug("Rules version: %s", rules_version)
        self.log.debug("Focus dataset: %s", focus_dataset)
        self.log.debug("Output type: %s, destination: %s", output_type, output_destination)

        if filter_rules:
            self.log.info("Rule filtering enabled: %s", filter_rules)
        if column_namespace:
            self.log.info("Column namespace: %s", column_namespace)
        if rules_force_remote_download:
            self.log.info("Force remote download enabled")

        self.rules_version = rules_version
        self.spec_rules = SpecRules(
            rule_set_path=rule_set_path,
            rules_file_prefix=rules_file_prefix,
            rules_version=self.rules_version,
            rules_file_suffix=rules_file_suffix,
            focus_dataset=focus_dataset,
            filter_rules=filter_rules,
            rules_force_remote_download=rules_force_remote_download,
            allow_draft_releases=allow_draft_releases,
            allow_prerelease_releases=allow_prerelease_releases,
            column_namespace=column_namespace,
        )
        self.outputter = Outputter(
            output_type=output_type, output_destination=output_destination
        )

        self.log.debug("Validator initialization completed")

    def get_spec_rules_path(self):
        return self.spec_rules.get_spec_rules_path()

    @logPerformance("validator.load", includeArgs=True)
    def load(self):
        self.log.info("Loading validation data and rules...")

        # Load data
        self.log.debug("Loading data from: %s", self.data_filename)
        if self.data_filename and os.path.exists(self.data_filename):
            file_size = os.path.getsize(self.data_filename)
            self.log.info("Data file size: %.2f MB", file_size / 1024 / 1024)

        dataLoader = data_loader.DataLoader(data_filename=self.data_filename)
        self.focus_data = dataLoader.load()

        if self.focus_data is not None:
            try:
                row_count = len(self.focus_data)
                col_count = len(self.focus_data.columns) if hasattr(self.focus_data, 'columns') else 'unknown'
                self.log.info("Data loaded successfully: %s rows, %s columns", row_count, col_count)
                self.log.debug("Column names: %s", list(self.focus_data.columns) if hasattr(self.focus_data, 'columns') else 'N/A')
            except Exception as e:
                self.log.warning("Could not determine data dimensions: %s", e)

        # Load rules
        self.log.debug("Loading specification rules...")
        self.spec_rules.load()
        self.log.info("Data and rules loading completed")

    @logPerformance("validator.validate", includeArgs=True)
    def validate(self):
        self.log.info("Starting validation process...")
        self.load()

        # Validate
        self.log.debug("Executing rule validation...")
        results = self.spec_rules.validate(self.focus_data)

        # Output results
        self.log.debug("Writing validation results...")
        self.outputter = self.outputter.write(results)

        self.log.info("Validation process completed")
        return results

    def get_supported_versions(self):
        self.log.debug("Retrieving supported versions...")
        local_versions = self.spec_rules.supported_local_versions()
        remote_versions = self.spec_rules.supported_remote_versions()
        self.log.debug("Found %d local versions, %d remote versions", len(local_versions), len(remote_versions))
        return local_versions, remote_versions
