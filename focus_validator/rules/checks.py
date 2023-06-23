from datetime import datetime

import pandas as pd
from pandera import extensions

from focus_validator.utils.download_currency_codes import get_currency_codes


def is_camel_case(column_name):
    return (
        column_name != column_name.lower()
        and column_name != column_name.upper()
        and "_" not in column_name
    )


@extensions.register_check_method()
def check_not_null(pandas_obj: pd.Series):
    return ~pandas_obj.isnull()


@extensions.register_check_method()
def check_unique(pandas_obj: pd.Series):
    return ~pandas_obj.duplicated()


@extensions.register_check_method()
def check_value_in(pandas_obj: pd.Series, allowed_values):
    return pandas_obj.isin(allowed_values)


@extensions.register_check_method()
def check_datetime_dtype(pandas_obj: pd.Series):
    def __validate_date_obj__(value: str):
        if not isinstance(value, str) or not value.endswith("Z"):
            return False

        try:
            datetime.strptime(value[:-1], "%Y-%m-%dT%H:%M:%S")
            return True
        except ValueError:
            return False

    return pd.Series(map(__validate_date_obj__, pandas_obj.values))


@extensions.register_check_method()
def check_currency_code_dtype(pandas_obj: pd.Series):
    currency_codes = set(get_currency_codes())
    return pd.Series(
        map(lambda v: isinstance(v, str) and v in currency_codes, pandas_obj.values)
    )
