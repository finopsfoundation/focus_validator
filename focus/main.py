import argparse

import pandas as pd
from pandera.errors import SchemaErrors
from tabulate import tabulate

from focus.generate_checks import generate_check
from .dimension_configurations import load_override_config


def __pretty_print__(df: pd.DataFrame):
    print(tabulate(df, headers='keys', tablefmt='psql'))


def process_data(data_file, override_file=None, report_path=None, error_csv_path=None):
    override_config = None
    if override_file:
        override_config = load_override_config(override_file)

    schema = generate_check(version=0.5, override_config=override_config)

    data = pd.read_csv(data_file)

    try:
        schema.validate(pd.DataFrame(data), lazy=True)
    except SchemaErrors as e:
        print("data:")
        __pretty_print__(e.data)
        print()
        print("failure_cases:")
        __pretty_print__(e.failure_cases)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='FOCUS specification validator.'
    )
    parser.add_argument('--data_file', help='Path to the data file (CSV)', required=True)
    parser.add_argument('--override_file', help='Path to the override file (YAML)')
    parser.add_argument('--report_path', help='Path to the output report file')
    parser.add_argument('--error_csv_path', help='Path to the output error CSV file')

    parser.add_argument('--stack', help='Path to the output error CSV file')

    args = parser.parse_args()

    process_data(args.data_file, args.override_file, args.report_path, args.error_csv_path)
