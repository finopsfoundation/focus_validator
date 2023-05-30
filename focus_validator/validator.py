from focus_validator.data_loaders import data_loader
from focus_validator.outputter.outputter import Outputter
from focus_validator.rules.spec_rules_loader import SpecRulesLoader


class Validator:
    def __init__(self, data_filename, rule_set_path, rules_version, output_destination, override_filename=None):
        self.data_filename = data_filename
        self.focus_data = None
        self.override_filename = override_filename

        self.rules_version = rules_version
        self.spec_rules_loader = SpecRulesLoader(override_filename=override_filename,
                                                 rule_set_path=rule_set_path,
                                                 rules_version=rules_version)
        self.output_destination = output_destination
        self.outputter = Outputter(self.output_destination)

    def load(self):
        self.focus_data = data_loader.DataLoader(data_filename=self.data_filename).load()
        self.spec_rules_loader.load()

    def validate(self):
        self.load()
        results = {}
        for rule in self.spec_rules_loader.rules:
            results[rule.check_id] = rule.validate(self.focus_data)
            results[rule.check_id]['rule'] = rule
        self.outputter = self.outputter.write(results)

    def get_supported_versions(self):
        return self.spec_rules_loader.supported_versions()
