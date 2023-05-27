"""Finlogic Database module.

This module provides functions to handle financial data from the CVM Portal. It
allows updating, processing and consolidating financial statements, as well as
searching for company names in the FinLogic Database and retrieving information
about the database itself.
"""
from pathlib import Path
from typing import Literal
from datetime import datetime
import pandas as pd
from . import config as cfg
from . import cvm
from . import language as lng
from . import builder as bld

CHECKMARK = "\033[32m\u2714\033[0m"


def get_main_df() -> pd.DataFrame:
    """Return a DataFrame with all accounting data"""
    if cfg.DF_PATH.is_file():
        df = pd.read_pickle(cfg.DF_PATH, compression="zstd")
    else:
        df = pd.DataFrame()

    return df


def get_filepaths_to_process(df1: pd.DataFrame, df2: pd.DataFrame) -> list[Path]:
    """Return a list of CVM files that has to be processed by comparing
    the files mtimes from the raw folder.
    """
    df = pd.concat([df1, df2]).drop_duplicates(keep=False)
    file_sources = sorted(df["file_source"].drop_duplicates())
    return [cfg.CVM_RAW_DIR / file_source for file_source in file_sources]


def update(rebuild: bool = False):
    """Verify changes in CVM files and update Finlogic Database if necessary.

    Args:
        rebuild (bool, optional): If True, processes all CVM files and rebuilds
            the database. Defaults to False.
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

    cvm.process_files_with_progress(filepaths_to_process)

    # FinLogic Database
    print("\nBuilding FinLogic main DataFrame...")
    bld.build_main_df()
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
    df = get_main_df()
    if df.empty:
        return pd.DataFrame()

    info["data_path"] = f"{cfg.DATA_PATH}"
    info["data_size"] = f"{cfg.DF_PATH.stat().st_size / 1024**2:.1f} MB"
    db_last_modified = datetime.fromtimestamp(cfg.DF_PATH.stat().st_mtime)
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


def search_company(
    search_value: str, search_by: Literal["name_id", "cvm_id", "tax_id"] = "name_id"
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
    df = get_main_df()[search_cols].drop_duplicates(ignore_index=True)
    match search_by:
        case "name_id":
            # Company name is stored in uppercase in the database
            df.query(f"name_id.str.contains('{search_value.upper()}')", inplace=True)
        case "cvm_id":
            df.query(f"cvm_id == {search_value}", inplace=True)
        case "tax_id":
            df.query(f"tax_id == '{search_value}'", inplace=True)
        case _:
            raise ValueError("Invalid value for 'search_by' argument.")

    return df.reset_index(drop=True)
