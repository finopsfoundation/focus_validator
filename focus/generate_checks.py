from .config_validators import CheckConfigs
from .dimension_configurations import load_default_config


def generate_check(version: float, override_config=None):
    default_config = load_default_config()
    checks = default_config[version]
    return CheckConfigs.generate_schema(checks, override_config=override_config)
