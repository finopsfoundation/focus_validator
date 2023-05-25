from typing import Union

import pandera as pa
from pydantic import BaseModel, validator

from .check_options import AllowNullsCheck, SIMPLE_CHECKS, ValueIn
from ..checks import not_null
from ..exceptions import FocusNotImplementedError


class CheckConfig05(BaseModel):
    version: float = 0.5
    check: Union[str, AllowNullsCheck, ValueIn]

    @validator('check')
    def validate_checks(cls, check):
        if isinstance(check, str):
            assert check in SIMPLE_CHECKS
            return check
        else:
            return check

    def generate_pandera_rule(self, check_name, friendly_name):
        check = self.check
        error_string = "{}: {}".format(check_name, friendly_name)

        if isinstance(check, str):
            if check == "check_unique":
                return pa.Check.check_unique(error=error_string)
            else:
                raise FocusNotImplementedError(msg="Check type: {} not implemented.".format(check))
        elif isinstance(check, ValueIn):
            return pa.Check.check_value_in(allowed_values=check.value_in, error=error_string)
        elif isinstance(check, AllowNullsCheck):
            return pa.Check(not_null, error="Dimension should have unique values")
        else:
            raise FocusNotImplementedError(msg="Check type: {} not implemented.".format(type(check)))
