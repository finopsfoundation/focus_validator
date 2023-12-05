import json
from datetime import datetime
from typing import Union

import numpy as np
import pandas as pd
import pandasql
import pandera as pa
from pandera import extensions
from pandera.errors import SchemaError

from focus_validator.utils.download_currency_codes import get_currency_codes


def is_camel_case(column_name):
    return (
        column_name != column_name.lower()
        and column_name != column_name.upper()
        and "_" not in column_name
    )


@extensions.register_check_method()
def check_not_null(pandas_obj: pd.Series, allow_nulls: bool):
    # TODO: works for string type, need to verify for other data types
    check_values = pandas_obj.isnull() | (pandas_obj == "")
    if not allow_nulls:
        check_values = check_values | (pandas_obj == "NULL")
    return ~check_values


@extensions.register_check_method()
def check_unique(pandas_obj: pd.Series):
    return ~pandas_obj.duplicated()


@extensions.register_check_method()
def check_value_in(pandas_obj: pd.Series, allowed_values):
    return pandas_obj.isin(allowed_values)


@extensions.register_check_method(check_type="groupby")
def check_sql_query(df_groups, sql_query, column_alias):
    grouped_elements = []
    for values in list(df_groups):
        row_obj = {}
        for column_name, value in zip(column_alias + ["index"], values):
            row_obj[column_name] = value
        grouped_elements.append(row_obj)

    df = pd.DataFrame(grouped_elements)
    check_output = pandasql.sqldf(sql_query, locals())["check_output"]

    # Getting the index of rows where the series values are False
    false_indexes = [i for i, val in enumerate(check_output) if not val]
    if false_indexes:
        # Extracting those rows from the dataframe
        extracted_rows = df.loc[false_indexes].to_dict("records")[:3]
        false_indexes = [i for i, val in enumerate(check_output) if not val]

        # for the given indexes in false_indexes list, we are extracting the rows from the dataframe and
        # add column_alias value to failure_case column and index to index column
        failure_cases = df[df.index.isin(false_indexes)].copy()
        failure_cases.loc[:, "failure_case"] = [
            ",".join([f"{column}:{row[column]}" for column in column_alias])
            for _, row in failure_cases.iterrows()
        ]

        raise SchemaError(
            schema=pa.DataFrameSchema(),
            data=None,
            message="",
            failure_cases=failure_cases,
        )

    return True


@extensions.register_check_method()
def check_datetime_dtype(pandas_obj: pd.Series):
    def __validate_date_obj__(value: Union[str, datetime]):
        if isinstance(value, str):
            # fix of python 3.10 and lower, strings ending with Z are not parsed automatically
            if value.endswith("Z"):
                value = value.replace("Z", "+00:00")

            try:
                value = datetime.fromisoformat(value)
            except ValueError:
                # failed to parse iso string
                return False

        if isinstance(value, datetime):
            return value.tzname() == "UTC"
        else:
            return isinstance(value, np.datetime64)

    return pd.Series(map(__validate_date_obj__, pandas_obj.values))


@extensions.register_check_method()
def check_currency_code_dtype(pandas_obj: pd.Series):
    currency_codes = set(get_currency_codes())
    return pd.Series(
        map(lambda v: isinstance(v, str) and v in currency_codes, pandas_obj.values)
    )


@extensions.register_check_method()
def check_stringified_json_object_dtype(pandas_obj: pd.Series):
    def __validate_stringified_json_object__(value: str):
        try:
            parsed = json.loads(value)
            return isinstance(parsed, dict)
        except Exception:
            return False

    return pd.Series(map(__validate_stringified_json_object__, pandas_obj.values))
