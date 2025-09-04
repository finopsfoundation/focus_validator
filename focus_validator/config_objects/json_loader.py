import json
import os
from collections import OrderedDict
from typing import Dict, Any


class JsonLoader:

    @staticmethod
    def load_json_rules(json_file_path: str) -> tuple[Dict[str, Any], OrderedDict[str, Any]]:
        if not os.path.exists(json_file_path):
            raise FileNotFoundError(f"JSON rules file not found: {json_file_path}")

        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        rules_dict = data.get('ConformanceRules', {})

        checkfunctions_data = data.get('CheckFunctions', {})
        checkfunctions_ordered_dict = OrderedDict(checkfunctions_data)

        return rules_dict, checkfunctions_ordered_dict
