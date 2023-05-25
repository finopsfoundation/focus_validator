import os
from pathlib import Path
from typing import List, Dict

import yaml

from focus.config_validators import CheckConfigs, ValidationOverrideConfig

resource_package = __name__
default_checks_file_path = '/'.join(('focus', 'checks'))


def __parse_check_yaml__(check_file_path):
    with open(check_file_path, 'r') as file:
        check_obj = yaml.safe_load(file)
    check_obj['check_name'] = Path(check_file_path).stem
    return CheckConfigs.parse_obj(check_obj)


def load_default_config() -> Dict[float, List[CheckConfigs]]:
    checks = {}
    for version in os.listdir(default_checks_file_path):
        __checks__ = []
        version_path = os.path.join(default_checks_file_path, version)
        for check_file_name in os.listdir(version_path):
            check_file_path = os.path.join(version_path, check_file_name)
            __checks__.append(__parse_check_yaml__(
                check_file_path=check_file_path
            ))
        checks[float(version)] = __checks__
    return checks


def load_override_config(override_file):
    with open(override_file, 'r') as file:
        override_obj = yaml.safe_load(file)
    override_config = ValidationOverrideConfig.parse_obj(override_obj)
    return override_config
