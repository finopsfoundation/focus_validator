from typing import List
from pydantic import BaseModel
import yaml


class Override(BaseModel):
    overrides: List[str]

    @staticmethod
    def load_yaml(self, override_filename):
        with open(self.override_filename, 'r') as file:
            override_obj = yaml.safe_load(file)
        return Override.parse_obj(override_obj)
