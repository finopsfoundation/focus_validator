import pandas as pd
from pandera import extensions


def is_camel_case(column_name):
    return column_name != column_name.lower() and column_name != column_name.upper() and "_" not in column_name


def not_null(values):
    return len([value for value in values if pd.isna(value)]) == 0


@extensions.register_check_method()
def check_unique(pandas_obj: pd.Series):
    return ~pandas_obj.duplicated()


@extensions.register_check_method()
def check_value_in(pandas_obj: pd.Series):
    return ""
