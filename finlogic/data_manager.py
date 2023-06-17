"""Finlogic manager module.

This module provides upper level functions to handle financial data from the CVM
Portal. It allows updating, processing and consolidating financial statements,
as well as searching for company names in the FinLogic Database and retrieving
information about the database itself.
"""
from pathlib import Path
from typing import Literal
from datetime import datetime
import pandas as pd
from . import config as cfg
from . import cvm
from . import language as lng
from . import reports as rep
from . import indicators as ind

CHECKMARK = "\033[32m\u2714\033[0m"


def get_filepaths_to_process(df1: pd.DataFrame, df2: pd.DataFrame) -> list[Path]:
    """Return a list of CVM files that has to be processed by comparing
    the files mtimes from the raw folder.
    """
    df = pd.concat([df1, df2]).drop_duplicates(keep=False)
    file_sources = sorted(df["file_source"].drop_duplicates())
    return [cfg.CVM_RAW_DIR / file_source for file_source in file_sources]


def update(rebuild: bool = False, is_listed: bool = True):
    """Verify changes in CVM files and update Finlogic Database if necessary.

    Args:
        rebuild (bool, optional): If True, process all CVM files and rebuilds
            the database. Defaults to False.
        is_listed (bool, optional): If True, only currently listed companies are
        processed.
    Returns:
        None
    """
    # Language files
    print('\nUpdating "language" database...')
    lng.process_language_df()

    # CVM raw files
    # Get files mtimes from the raw folder before updating
    df_raw1 = cvm.get_raw_file_mtimes()
    urls = cvm.get_all_file_urls()
    updated_raw_filepaths = cvm.update_raw_files(urls)
    print(f"Number of CVM files updated = {len(updated_raw_filepaths)}")
    # Get files mtimes from the raw folder after updating
    df_raw2 = cvm.get_raw_file_mtimes()

    # CVM processed files
    if rebuild:
        # Process all files
        filepaths_to_process = sorted(cfg.CVM_RAW_DIR.glob("*.zip"))
    else:
        # Process only updated files
        filepaths_to_process = get_filepaths_to_process(df1=df_raw1, df2=df_raw2)
    print(f"Number of new files to process = {len(filepaths_to_process)}")

    cvm.process_files_with_progress(filepaths_to_process, is_listed)

    # FinLogic Database
    print("\nBuilding FinLogic main DataFrame...")
    rep.save_reports_df()
    ind.save_indicators()
    print(f"{CHECKMARK} FinLogic updated!")


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
    df = rep.get_reports()
    if df.empty:
        return pd.DataFrame()

    info["data_path"] = f"{cfg.DATA_PATH}"
    info["data_size"] = f"{cfg.REPORTS_PATH.stat().st_size / 1024**2:.1f} MB"
    db_last_modified = datetime.fromtimestamp(cfg.REPORTS_PATH.stat().st_mtime)
    info["updated_on"] = db_last_modified.strftime("%Y-%m-%d %H:%M:%S")

    info["accounting_entries"] = df.shape[0]

    report_cols = ["cvm_id", "is_annual", "period_end"]
    info["number_of_reports"] = df[report_cols].drop_duplicates().shape[0]
    info["first_report"] = df["period_end"].min().strftime("%Y-%m-%d")
    info["last_report"] = df["period_end"].max().strftime("%Y-%m-%d")

    info["number_of_companies"] = df["cvm_id"].nunique()

    s = pd.Series(info)
    s.name = "FinLogic Info"

    return s.to_frame()


def search_segment(search_value: str):
    series = (
        cfg.LAST_SESSION_DF["segment"].drop_duplicates().sort_values(ignore_index=True)
    )
    mask = series.str.contains(search_value)
    return series[mask].reset_index(drop=True)


def search_company(
    search_value: str,
    search_by: Literal["name_id", "cvm_id", "tax_id", "segment"] = "name_id",
    min_volume: int = 100_000,
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
        min_volume (int): The minimum daily volume of the stock. Defaults
            to 100,000, which is a reasonable value to remove extremely illiquid
            stocks.

    Returns:
        pd.DataFrame: A DataFrame containing the search results, with columns
            'name_id', 'cvm_id', and 'tax_id' for each unique company that
            matches the search criteria.
    """
    search_cols = ["name_id", "cvm_id", "tax_id"]
    df = rep.get_reports()[search_cols].drop_duplicates(
        subset=["cvm_id"], ignore_index=True
    )
    df = pd.merge(df, cfg.LAST_SESSION_DF, on="cvm_id")
    df = df[df["volume"] >= min_volume].reset_index(drop=True)
    drop_cols = ["stock_type", "close_price", "trading_date", "volume"]
    df.drop(columns=drop_cols, inplace=True)
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

    return df.reset_index(drop=True)


def rank(segment: str = None, n: int = 10, rank_by: str = "roic") -> pd.DataFrame:
    fixed_cols = ["cvm_id", "name_id", "is_annual", "is_consolidated", "period_end"]
    show_cols = fixed_cols + ["operating_margin"]
    drop_cols = [
        "b3_id",
        "stock_type",
        "close_price",
        "trading_date",
        "volume",
        "is_annual",
        "is_consolidated",
    ]
    df = (
        ind.get_indicators()
        .sort_values(by=["cvm_id", "period_end", "is_consolidated"], ignore_index=True)
        # .query("cvm_id == 922")
        .drop_duplicates(subset=["cvm_id"], keep="last")
        .sort_values(by=["operating_margin"], ascending=False, ignore_index=True)[
            show_cols
        ]
        .merge(cfg.LAST_SESSION_DF, on="cvm_id")
        .drop(columns=drop_cols)
        .query("segment.str.contains(@segment)")
        .head(n)
    )

    return df
