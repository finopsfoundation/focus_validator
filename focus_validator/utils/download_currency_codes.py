import xml.etree.ElementTree as ET

import pandas as pd
import requests
import pkg_resources

DATAHUB_URL = "https://www.six-group.com/dam/download/financial-information/data-center/iso-currrency/lists/list-one.xml"
CURRENCY_CODE_CSV_PATH = "currency_codes.csv"

currency_code_csv_file = pkg_resources.resource_filename(__name__, CURRENCY_CODE_CSV_PATH)


def download_currency_codes():  # pragma: no cover
    r = requests.get(DATAHUB_URL)
    root = ET.fromstring(r.content.decode())

    currency_codes = []
    for child in root.iter():
        if child.tag == "Ccy":
            currency_codes.append(child.text)

    df = pd.DataFrame(set(currency_codes), columns=["currency_codes"])
    df.to_csv(currency_code_csv_file)


def get_currency_codes():
    df = pd.read_csv(currency_code_csv_file)
    return set(df["currency_codes"].values)


if __name__ == "__main__":
    download_currency_codes()

