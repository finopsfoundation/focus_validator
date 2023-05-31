from typing import List

import yaml
from pydantic import BaseModel


class Override(BaseModel):
    overrides: List[str]

    @staticmethod
    def load_yaml(self, override_filename):
        with open(self.override_filename, "r") as file:
            override_obj = yaml.safe_load(file)
        return Override.parse_obj(override_obj)
