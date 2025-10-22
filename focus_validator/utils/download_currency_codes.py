import argparse
import xml.etree.ElementTree as ET

import pandas as pd
import requests

DATAHUB_URL = "https://www.six-group.com/dam/download/financial-information/data-center/iso-currrency/lists/list-one.xml"


def download_currency_codes(output_file: str):  # pragma: no cover
    r = requests.get(DATAHUB_URL)
    root = ET.fromstring(r.content.decode())

    currency_codes = []
    for child in root.iter():
        if child.tag == "Ccy":
            currency_codes.append(child.text)

    # Filter out None values and create DataFrame with unique currency codes
    valid_currency_codes = [code for code in currency_codes if code is not None]
    df = pd.DataFrame(set(valid_currency_codes), columns=["currency_codes"])
    df.to_csv(output_file)


def get_currency_codes(code_file: str) -> set:  # pragma: no cover
    df = pd.read_csv(code_file)
    return set(df["currency_codes"].values)


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(description="FOCUS specification validator.")
    parser.add_argument(
        "--output-file",
        help="Path to save the downloaded currency codes CSV file.",
        default="focus_validator/rules/currency_codes.csv",
        required=False,
    )
    args = parser.parse_args()
    download_currency_codes(args.output_file)
