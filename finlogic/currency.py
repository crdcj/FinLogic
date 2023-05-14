"""This module provides tools for processing currency dataframes.

It handles the following tasks:
- Create interim folder for storing CSV currency files if it doesn't exist.
- Load currency dataframe or create an empty one if file doesn't exist.
- Process, merge and format data into a dataframe with appropriate structure.

Functions:
- process_currency_df: Fetch currency exchange rate data from BCB's website,
process and merge it into a dataframe.
"""

from pathlib import Path
import pandas as pd
from datetime import datetime
from . import config as cfg
from . import fl_duckdb as fdb

INTERIM_DIR = cfg.DATA_PATH / "interim"
CURRENCY_DF_PATH = INTERIM_DIR / "currencies.csv"

# Create interim folder if itdoes not exist.
Path.mkdir(INTERIM_DIR, parents=True, exist_ok=True)

# Start/load currency file data
if CURRENCY_DF_PATH.is_file():
    currency_df = pd.read_csv(CURRENCY_DF_PATH)
else:
    currency_df = pd.DataFrame()


def process_currency_df():
    """Process currency dataframe."""

    # Selected currencies and their BCB codes
    dict_bcb_code = {
        "ARS": "156",
        "CLP": "158",
        "CNY": "178",
        "EUR": "222",
        "GBP": "115",
        "INR": "193",
        "JPY": "101",
        "MXN": "165",
        "RUB": "187",
        "USD": "61",
        "ZAR": "176",
    }

    # Get first and last statement dates
    query = "SELECT MIN(period_end) FROM reports"
    first_statement = str(fdb.execute(query, "df").iloc[0][0]).split("-")
    query = "SELECT MAX(period_end) FROM reports"
    last_statement = str(fdb.execute(query, "df").iloc[0][0]).split("-")

    # Iterate through currencies, fetch data from BCB's website and merge into
    # a single dataframe
    df_currencies = pd.DataFrame(columns=["date"])
    for moeda in dict_bcb_code.keys():
        URL_CURRENCY = f"https://ptax.bcb.gov.br/ptax_internet/consultaBoletim.do?method=gerarCSVFechamentoMoedaNoPeriodo&ChkMoeda={dict_bcb_code[moeda]}&DATAINI={first_statement[2][:2]}/{first_statement[1]}/{first_statement[0]}&DATAFIM={last_statement[2][:2]}/{last_statement[1]}/{last_statement[0]}"

        df_moeda = pd.read_csv(
            URL_CURRENCY,
            sep=";",
            decimal=",",
            thousands=".",
            header=None,
            dtype={0: str},
        )
        df_moeda[0] = pd.to_datetime(df_moeda[0], format="%d%m%Y")
        df_moeda.rename(columns={0: "date"}, inplace=True)
        df_moeda[f"{moeda}"] = (df_moeda[4] + df_moeda[5]) / 2
        df_moeda.drop([1, 2, 3, 4, 5, 6, 7], axis=1, inplace=True)

        df_currencies = pd.merge(df_currencies, df_moeda, on="date", how="outer")

    # Sort by date and save to CSV file
    df_currencies = df_currencies.sort_values(by="date")
    df_currencies.to_csv(CURRENCY_DF_PATH, index=False)
