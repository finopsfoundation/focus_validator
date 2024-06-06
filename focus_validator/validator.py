import importlib.resources

from focus_validator.data_loaders import data_loader
from focus_validator.outputter.outputter import Outputter
from focus_validator.rules.spec_rules import SpecRules

try:
    DEFAULT_VERSION_SETS_PATH = str(
        importlib.resources.files("focus_validator.rules").joinpath("version_sets")
    )
except AttributeError:
    # for compatibility with python 3.8, which does not support files api in importlib
    from pkg_resources import resource_filename

    DEFAULT_VERSION_SETS_PATH = resource_filename(
        "focus_validator.rules", "version_sets"
    )


class Validator:
    def __init__(
        self,
        data_filename,
        output_destination,
        output_type,
        rule_set_path=DEFAULT_VERSION_SETS_PATH,
        rules_version="0.5",
        override_filename=None,
        column_namespace=None,
    ):
        self.data_filename = data_filename
        self.focus_data = None
        self.override_filename = override_filename

        self.rules_version = rules_version
        self.spec_rules = SpecRules(
            override_filename=override_filename,
            rule_set_path=rule_set_path,
            rules_version=rules_version,
            column_namespace=column_namespace,
        )
        self.outputter = Outputter(
            output_type=output_type, output_destination=output_destination
        )

    def load(self):
        self.focus_data = data_loader.DataLoader(
            data_filename=self.data_filename
        ).load()
        self.spec_rules.load()

    def validate(self):
        self.load()
        results = self.spec_rules.validate(self.focus_data)
        self.outputter = self.outputter.write(results)

    def get_supported_versions(self):
        return self.spec_rules.supported_versions()
