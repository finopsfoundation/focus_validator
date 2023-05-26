import pandas as pd


def reformat_failure_cases_df(failure_cases: pd.DataFrame):
    failure_cases = failure_cases.rename(columns={'column': 'Dimension', 'index': 'Row #', 'failure_case': 'Values'})
    failure_cases[['Check Name', 'Description']] = failure_cases['check'].str.split(':', expand=True)
    failure_cases = failure_cases.drop('check', axis=1)
    failure_cases = failure_cases.drop('check_number', axis=1)
    failure_cases = failure_cases.drop('schema_context', axis=1)

    failure_cases = failure_cases.rename_axis('#')
    failure_cases.index = failure_cases.index + 1

    failure_cases['Row #'] = failure_cases['Row #'] + 1
    failure_cases = failure_cases[['Dimension', 'Check Name', 'Description', 'Values', 'Row #']]

    return failure_cases
