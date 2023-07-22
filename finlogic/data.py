"""Finlogic manager module.

This module provides upper level functions to handle financial data from the CVM
Portal. It allows updating, processing and consolidating financial statements,
as well as searching for company names in the FinLogic Database and retrieving
information about the database itself.
"""
from typing import Literal
import pandas as pd
from . import indicators as ind

CHECKMARK = "\033[32m\u2714\033[0m"
# Load last session data
TRADE_DATA_URL = (
    "https://raw.githubusercontent.com/crdcj/FinLogic/main/data/trades.csv.gz"
)
TRADED_FINANCIALS_URL = "https://raw.githubusercontent.com/crdcj/FinLogic/main/data/traded_companies_financials.csv.gz"
NOT_TRADED_FINANCIALS_URL = "https://raw.githubusercontent.com/crdcj/FinLogic/main/data/not_traded_companies_financials.csv.gz"
LANGUAGE_DATA_URL = (
    "https://raw.githubusercontent.com/crdcj/FinLogic/main/data/pten_df.csv.gz"
)
FINANCIALS_DF = pd.DataFrame()
TRADES_DF = pd.DataFrame()
LANGUAGE_DF = pd.DataFrame()


def load(is_traded: bool = True, min_volume: int = 100_000):
    """Verify changes in CVM files and update Finlogic Database if necessary.

    Args:
        is_traded (bool, optional): If True, only currently traded companies are
        loaded.
        min_volume (int): The minimum daily volume of the stock. Defaults
            to R$ 100k (aprox. USD 20k), which is a reasonable value to remove
            extremely illiquid stocks.

    Returns:
        None
    """
    global LANGUAGE_DF
    global TRADES_DF
    global FINANCIALS_DF
    print('Loading "language" data...')
    LANGUAGE_DF = pd.read_csv(LANGUAGE_DATA_URL)
    print("Loading trading data...")
    TRADES_DF = pd.read_csv(TRADE_DATA_URL)
    print("Loading financials data...")
    date_cols = ["period_begin", "period_end"]
    FINANCIALS_DF = pd.read_csv(TRADED_FINANCIALS_URL, parse_dates=date_cols)
    if not is_traded:
        FINANCIALS_DF = pd.concat(
            [FINANCIALS_DF, pd.read_csv(NOT_TRADED_FINANCIALS_URL)], ignore_index=True
        )
    TRADES_DF = TRADES_DF.query("volume >= @min_volume")
    TRADED_CVM_IDS = TRADES_DF["cvm_id"].unique()  # noqa
    FINANCIALS_DF = FINANCIALS_DF.query("cvm_id in @TRADED_CVM_IDS").reset_index(
        drop=True
    )
    print("Building indicators data...")
    global INDICATORS_DF
    INDICATORS_DF = ind.build_indicators(FINANCIALS_DF)
    print(f"{CHECKMARK} FinLogic is ready!")


def info() -> pd.DataFrame:
    """Print a concise summary of FinLogic available data.

    This function returns a dataframe containing main information about
    FinLogic Database, such as the database path, file size, last update call,
    last modified dates, size in memory, number of accounting rows, unique
    accounting codes, companies, unique financial statements, first financial
    statement date and last financial statement date.

    Args:
        return_dict (bool, optional): If True, returns a dictionary with the
            database information and do not print it.

    Returns: None
    """
    info = {}
    if FINANCIALS_DF.empty:
        return pd.DataFrame()

    info["data_url"] = f"{TRADED_FINANCIALS_URL}"
    data_size = (
        FINANCIALS_DF.memory_usage(deep=True).sum()
        + TRADES_DF.memory_usage(deep=True).sum()
    )
    info["memory_usage"] = f"{data_size / 1024**2:.1f} MB"
    # info["updated_on"] = db_last_modified.strftime("%Y-%m-%d %H:%M:%S")

    info["accounting_entries"] = FINANCIALS_DF.shape[0]

    report_cols = ["cvm_id", "is_annual", "period_end"]
    info["number_of_reports"] = FINANCIALS_DF[report_cols].drop_duplicates().shape[0]
    info["first_report"] = FINANCIALS_DF["period_end"].min().strftime("%Y-%m-%d")
    info["last_report"] = FINANCIALS_DF["period_end"].max().strftime("%Y-%m-%d")

    info["number_of_companies"] = FINANCIALS_DF["cvm_id"].nunique()

    s = pd.Series(info)
    s.name = "FinLogic Info"

    return s.to_frame()


def search_segment(search_value: str):
    series = TRADES_DF["segment"].drop_duplicates().sort_values(ignore_index=True)
    mask = series.str.contains(search_value)
    return series[mask].reset_index(drop=True)


def search_company(
    search_value: str,
    search_by: Literal["name_id", "cvm_id", "tax_id", "segment"] = "name_id",
) -> pd.DataFrame:
    """Search for a company name in FinLogic Database.

    This function searches the specified column in the FinLogic Database for
    company names that contain the provided expression. It returns a DataFrame
    containing the search results, with each row representing a unique company
    that matches the search criteria.

    Args:
        search_value (str): The search expression.
        search_by (str): The column where the id search will be performed. Valid
            values are 'name_id', 'cvm_id', and 'tax_id'. Defaults to 'name_id'.

    Returns:
        pd.DataFrame: A DataFrame containing the search results, with columns
            'name_id', 'cvm_id', and 'tax_id' for each unique company that
            matches the search criteria.
    """
    search_cols = ["name_id", "cvm_id", "tax_id"]
    df = FINANCIALS_DF[search_cols].drop_duplicates(
        subset=["cvm_id"], ignore_index=True
    )
    df = pd.merge(df, TRADES_DF, on="cvm_id")
    match search_by:
        case "name_id":
            # Company name is stored in uppercase in the database
            df.query(f"name_id.str.contains('{search_value.upper()}')", inplace=True)
        case "cvm_id":
            df.query(f"cvm_id == {search_value}", inplace=True)
        case "tax_id":
            df.query(f"tax_id == '{search_value}'", inplace=True)
        case "segment":
            df.query(f"segment.str.contains('{search_value}')", inplace=True)
        case _:
            raise ValueError("Invalid value for 'search_by' argument.")

    show_cols = [
        "name_id",
        "cvm_id",
        "tax_id",
        "segment",
        "is_restructuring",
        "most_traded_stock",
    ]
    return df[show_cols].reset_index(drop=True)


def rank(
    segment: str = None, n: int = 10, rank_by: str = "operating_margin"
) -> pd.DataFrame:
    """Rank companies by a given indicator.

    This function returns a DataFrame containing the top n companies in the
    specified segment, ranked by the given indicator.

    Args:
        segment (str): The segment to be ranked. Defaults to None, which
            returns the top n companies in all segments.
        n (int): The number of companies to be returned. Defaults to 10.
        rank_by (str): The indicator to be used for ranking. Defaults to
            'operating_margin'. Valid values are 'total_assets', 'equity',
            'revenues', 'gross_profit', 'ebit', 'ebt', 'net_income',
            'operating_cash_flow', 'eps', 'total_cash', 'total_debt',
            'net_debt', 'ebitda', 'gross_margin', 'ebitda_margin',
            'operating_margin', 'net_margin', 'return_on_assets',
            'return_on_equity', 'roic'.
    """
    show_cols = [
        "name_id",
        "cvm_id",
        "most_traded_stock",
        "segment",
        "is_restructuring",
        "period_end",
        rank_by,
    ]
    df = (
        ind.get_indicators()
        .sort_values(by=["cvm_id", "period_end", "is_consolidated"], ignore_index=True)
        # .query("cvm_id == 922")
        .drop_duplicates(subset=["cvm_id"], keep="last")
        .merge(TRADES_DF, on="cvm_id")
        .query("segment.str.contains(@segment)")
        .sort_values(by=[rank_by], ascending=False, ignore_index=True)
        .head(n)[show_cols]
    )

    return df
