from focus_validator.exceptions import UnsupportedVersion
from focus_validator.config_validators.override_config import ValidationOverrideConfig
import os
import yaml

class SpecRulesLoader:
    def __init__(self, override_filename, rule_set_path, rules_version):
        self.override_filename = override_filename
        self.override_config = self.load_overrides()
        self.rules_version = rules_version
        self.rule_set_path = rule_set_path
        if self.rules_version not in self.supported_versions():
            raise UnsupportedVersion(f'FOCUS version {self.rules_version} not supported.')
        self.rules_path = os.path.join(self.rule_set_path, self.rules_version)
        self.rules = []

    def load_overrides(self):
        if not self.override_filename:
            return {}
        with open(self.override_filename, 'r') as file:
            override_obj = yaml.safe_load(file)
        override_config = ValidationOverrideConfig.parse_obj(override_obj)
        return override_config

    def supported_versions(self):
        return sorted([x for x in os.walk(self.rule_set_path)][0][1])

    def load(self):
        for rule_path in self.get_rule_paths():
            self.rules.append(Rule(rule_path=rule_path, override_config=self.override_config))

    def get_rule_paths(self):
        rule_paths = []
        for root, dirs, files in os.walk(self.rules_path, topdown=False):
            for name in files:
                rule_paths.append(os.path.join(root, name))
        return rule_paths


class Rule:
    def __init__(self, rule_path, override_config=None):
        self.rule_path = rule_path
        self.meta_data = None
        self.load()
        self.parse_friendly_name()
        self.skipped = False
        self.handle_overrides(override_config)

    def load(self):
        with open(self.rule_path, 'r') as f:
            self.meta_data = yaml.safe_load(f)
        self.__dict__.update(self.meta_data)
        self.validation_config.update(self.validation_config)

    def parse_friendly_name(self):
        if 'value_in' in self.validation_config['check']:
            self.check_friendly_name = self.check_friendly_name.replace('{values}', str(self.validation_config['check']['value_in']))

    def handle_overrides(self, override_config):
        if not override_config:
            return
        if self.check_id in override_config.overrides.skip:
            self.skipped = True

    def validate(self, dataset):
        # TODO: Determine how this step works
        return {'result': True, 'skipped': self.skipped}

        # schema = generate_check(version=0.5, override_config=override_config)


        # try:
        #     schema.validate(pd.DataFrame(data), lazy=True)
        # except SchemaErrors as e:
        #     print("data:")
        #     __pretty_print__(e.data)
        #     print()
        #     print("failure_cases:")
        #     __pretty_print__(reformat_failure_cases_df(e.failure_cases))