from typing import List

import yaml
from pydantic.v1 import BaseModel


class Override(BaseModel):
    overrides: List[str]

    @staticmethod
    def load_yaml(override_filename):
        with open(override_filename, "r") as file:
            override_obj = yaml.safe_load(file)
        return Override.parse_obj(override_obj)
