"""Finlogic manager module.

This module provides upper level functions to handle financial data from the CVM
Portal. It allows updating, processing and consolidating financial statements,
as well as searching for company names in the FinLogic Database and retrieving
information about the database itself.
"""

from typing import Literal

import polars as pl

from . import indicators as ind

# URLs for the data files
DATA_REPO = "https://raw.githubusercontent.com/crdcj/finlogic-data/main/"
TRADE_DATA_URL = f"{DATA_REPO}trades.csv.gz"
TRADED_FINANCIALS_URL = f"{DATA_REPO}traded_companies_financials.csv.gz"
NOT_TRADED_FINANCIALS_URL = f"{DATA_REPO}not_traded_companies_financials.csv.gz"
LANGUAGE_DATA_URL = f"{DATA_REPO}pten_df.csv.gz"

FINANCIALS_DF = pl.DataFrame()
TRADES_DF = pl.DataFrame()
LANGUAGE_DF = pl.DataFrame()
INDICATORS_DF = pl.DataFrame()


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
    print('✔ Loading "language" data...')
    LANGUAGE_DF = pl.read_csv(LANGUAGE_DATA_URL)
    print("✔ Loading trading data...")
    TRADES_DF = pl.read_csv(TRADE_DATA_URL)
    print("✔ Loading financials data...")
    date_cols = ["period_begin", "period_end"]
    FINANCIALS_DF = pl.read_csv(TRADED_FINANCIALS_URL).with_columns(
        [pl.col(c).str.to_date() for c in date_cols]
    )
    if not is_traded:
        df_not_traded = pl.read_csv(NOT_TRADED_FINANCIALS_URL).with_columns(
            [pl.col(c).str.to_date() for c in date_cols]
        )
        FINANCIALS_DF = pl.concat([FINANCIALS_DF, df_not_traded])
    TRADES_DF = TRADES_DF.filter(pl.col("volume") >= min_volume)
    traded_cvm_ids = TRADES_DF["cvm_id"].to_list()
    FINANCIALS_DF = FINANCIALS_DF.filter(pl.col("cvm_id").is_in(traded_cvm_ids))
    print("✔ Building indicators data...")
    global INDICATORS_DF
    INDICATORS_DF = ind.build_indicators(FINANCIALS_DF)
    print("✔ FinLogic is ready!")


def info() -> pl.DataFrame:
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
    if FINANCIALS_DF.is_empty():
        return pl.DataFrame()

    data_size = FINANCIALS_DF.estimated_size() + TRADES_DF.estimated_size()
    report_cols = ["cvm_id", "is_annual", "period_end"]

    info_data = {
        "data_url": TRADED_FINANCIALS_URL,
        "memory_usage": f"{data_size / 1024**2:.1f} MB",
        "accounting_entries": str(FINANCIALS_DF.height),
        "number_of_reports": str(FINANCIALS_DF.select(report_cols).unique().height),
        "first_report": str(FINANCIALS_DF["period_end"].min()),
        "last_report": str(FINANCIALS_DF["period_end"].max()),
        "number_of_companies": str(FINANCIALS_DF["cvm_id"].n_unique()),
    }
    return pl.DataFrame(
        {"key": list(info_data.keys()), "FinLogic Info": list(info_data.values())}
    )


def search_segment(search_value: str) -> pl.Series:
    series = TRADES_DF["segment"].unique().sort()
    return series.filter(series.str.contains(search_value))


def search_company(
    search_value: str,
    search_by: Literal["name_id", "cvm_id", "tax_id", "segment"] = "name_id",
) -> pl.DataFrame:
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
        pl.DataFrame: A DataFrame containing the search results, with columns
            'name_id', 'cvm_id', and 'tax_id' for each unique company that
            matches the search criteria.
    """
    search_cols = ["name_id", "cvm_id", "tax_id"]
    df = (
        FINANCIALS_DF.select(search_cols)
        .unique(subset=["cvm_id"], maintain_order=True)
        .join(TRADES_DF, on="cvm_id")
    )
    match search_by:
        case "name_id":
            df = df.filter(pl.col("name_id").str.contains(search_value.upper()))
        case "cvm_id":
            df = df.filter(pl.col("cvm_id") == int(search_value))
        case "tax_id":
            df = df.filter(pl.col("tax_id") == search_value)
        case "segment":
            df = df.filter(pl.col("segment").str.contains(search_value))
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
    return df.select(show_cols)


def rank(
    segment: str | None = None,
    n: int = 10,
    rank_by: str = "operating_margin",
    is_consolidated: bool = True,
) -> pl.DataFrame:
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
        "most_traded_stock",
        "cvm_id",
        "is_restructuring",
        "is_consolidated",
        "segment",
        "period_end",
        rank_by,
    ]
    seg_filter = (
        pl.lit(True) if segment is None else pl.col("segment").str.contains(segment)
    )
    df = (
        FINANCIALS_DF.sort(["cvm_id", "period_end", "is_consolidated"])
        .unique(subset=["cvm_id"], keep="last", maintain_order=True)
        .join(TRADES_DF, on="cvm_id")
        .join(
            INDICATORS_DF.select(["cvm_id", rank_by, "is_consolidated", "period_end"]),
            on=["cvm_id", "period_end", "is_consolidated"],
        )
        .filter(seg_filter & (pl.col("is_consolidated") == is_consolidated))
        .sort(rank_by, descending=True)
        .head(n)
        .select(show_cols)
    )
    return df
