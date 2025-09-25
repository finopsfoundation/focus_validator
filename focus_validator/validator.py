import importlib.resources
import logging
from focus_validator.data_loaders import data_loader
from focus_validator.outputter.outputter import Outputter
from focus_validator.rules.spec_rules import SpecRules

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
        rule_prefix=None,
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

        self.rules_version = rules_version
        self.spec_rules = SpecRules(
            rule_set_path=rule_set_path,
            rules_file_prefix=rules_file_prefix,
            rules_version=self.rules_version,
            rules_file_suffix=rules_file_suffix,
            rule_prefix=rule_prefix,
            rules_force_remote_download=rules_force_remote_download,
            allow_draft_releases=allow_draft_releases,
            allow_prerelease_releases=allow_prerelease_releases,
            column_namespace=column_namespace,
        )
        self.outputter = Outputter(
            output_type=output_type, output_destination=output_destination
        )

    def get_spec_rules_path(self):
        return self.spec_rules.get_spec_rules_path()

    def load(self):
        self.focus_data = data_loader.DataLoader(
            data_filename=self.data_filename
        ).load()
        self.spec_rules.load()

    def validate(self):
        self.load()
        results = self.spec_rules.validate(self.focus_data)
        self.outputter = self.outputter.write(results)
        return results

    def get_supported_versions(self):
        return self.spec_rules.supported_local_versions(), self.spec_rules.supported_remote_versions()
