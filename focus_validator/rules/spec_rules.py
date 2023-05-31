import os

from focus_validator.exceptions import UnsupportedVersion
from focus_validator.config_objects import Rule, Override

class SpecRules:
    def __init__(self, override_filename, rule_set_path, rules_version):
        self.override_filename = override_filename
        self.override_config = None
        self.rules_version = rules_version
        self.rule_set_path = rule_set_path
        if self.rules_version not in self.supported_versions():
            raise UnsupportedVersion(f'FOCUS version {self.rules_version} not supported.')
        self.rules_path = os.path.join(self.rule_set_path, self.rules_version)
        self.rules = []

    def supported_versions(self):
        return sorted([x for x in os.walk(self.rule_set_path)][0][1])

    def load(self):
        self.load_overrides()
        self.load_rules()
    
    def load_overrides(self):
        if not self.override_filename:
            return {}
        self.override_config =  Override.load_yaml(self.override_filename)



    def load_rules(self):
        for rule_path in self.get_rule_paths():
            self.rules.append(Rule.load_yaml(rule_path))

    def get_rule_paths(self):
        for root, dirs, files in os.walk(self.rules_path, topdown=False):
            for name in files:
                yield os.path.join(root, name)

    def validate(self):
        # TODO: Generate schema from Rules objects then validate, Return resultset that can be parsed by outputters
        pass
