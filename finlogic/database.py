"""Finlogic Database module.

This module provides functions to handle financial data from the CVM Portal. It
allows updating, processing and consolidating financial statements, as well as
searching for company names in the FinLogic Database and retrieving information
about the database itself.
"""

import os
import shutil
from pathlib import Path

import pandas as pd
from . import config as cf
from . import cvm as cv


def consolidate_main_df(processed_filenames: str):
    # Guard clause: if no raw file was update, there is nothing to consolidate
    if not processed_filenames:
        return

    for filename in processed_filenames:
        updated_df = pd.read_pickle(cf.PROCESSED_DIR / filename)
        cf.main_df = pd.concat([cf.main_df, updated_df], ignore_index=True)
    cf.main_df = cf.main_df.astype("category")
    # Keep only the newest 'report_version' in df if values are repeated
    cols = [
        "cvm_id",
        "report_type",
        "period_reference",
        "report_version",
        "period_order",
        "acc_method",
        "acc_code",
    ]
    cf.main_df.sort_values(by=cols, ignore_index=True, inplace=True)
    cols = list(cf.main_df.columns)
    cols_remove = ["report_version", "acc_value", "acc_fixed"]
    [cols.remove(col) for col in cols_remove]
    # Ascending order --> last is the newest report_version
    cf.main_df.drop_duplicates(cols, keep="last", inplace=True)
    cf.main_df.to_pickle(cf.MAIN_DF_PATH)


def update_database(
    reset_data: bool = False, asynchronous: bool = False, cpu_usage: float = 0.75
):
    """Verify changes in remote files and update them in Finlogic Database.

    Args:
        reset_data: Delete all raw files and force a full database
            recompilation. Default is False.
        asynchronous: Generate the database by processing raw files
            asynchronously. Works only on Linux and Mac. Default is False.
        cpu_usage: A number between 0 and 1, where 1 represents 100% CPU usage.
            This argument will define the number of cpu cores used for data
            processing when function asynchronous mode is set to 'True'. Default
            is 0.75.

    Returns:
        None
    """
    # Parameter 'reset_data'-> delete, if they exist, data folders.
    if reset_data:
        if Path.exists(cf.DATA_PATH):
            shutil.rmtree(cf.DATA_PATH)
        cf.main_df = pd.DataFrame()
    # Create data folders if they do not exist.
    Path.mkdir(cf.RAW_DIR, parents=True, exist_ok=True)
    Path.mkdir(cf.PROCESSED_DIR, parents=True, exist_ok=True)
    # Define the number of cpu cores for parallel data processing.
    workers = int(os.cpu_count() * cpu_usage)
    if workers < 1:
        workers = 1
    print("Updating financial statements...")
    urls = cv.list_urls()
    # urls = urls[:1]  # Test
    raw_paths = cv.update_raw_files(urls)
    print(f"Number of financial statements updated = {len(raw_paths)}")
    print("\nProcessing financial statements...")
    processed_filenames = cv.process_raw_files(
        workers, raw_paths, asynchronous=asynchronous
    )
    print("\nConsolidating processed files...")
    consolidate_main_df(processed_filenames)
    print('Updating "language" database...')
    process_language_df()
    print(f"{cf.CHECKMARK} FinLogic database updated!")


def database_info() -> dict:
    """Returns general information about FinLogic Database.

    This function generates a dictionary containing main information about
    FinLogic Database, such as the database path, file size, last update call,
    last modified dates, size in memory, number of accounting rows, unique
    accounting codes, companies, unique financial statements, first financial
    statement date and last financial statement date.

    Returns:
        A dictionary containing the FinLogic Database information.
    """
    if cf.main_df.empty:
        print("Finlogic Database is empty")
        return

    file_date_unix = round(cf.MAIN_DF_PATH.stat().st_mtime, 0)
    memory_size = cf.main_df.memory_usage(index=True, deep=True).sum()
    statements_cols = ["cvm_id", "report_version", "report_type", "period_reference"]
    statements_num = len(cf.main_df.drop_duplicates(subset=statements_cols).index)
    first_statement = cf.main_df["period_end"].astype("datetime64[ns]").min()
    last_statement = cf.main_df["period_end"].astype("datetime64[ns]").max()

    info_dict = {
        "Path": cf.DATA_PATH,
        "File size (MB)": round(cf.MAIN_DF_PATH.stat().st_size / 1024**2, 1),
        "Last update call": cf.cvm_df.index.max().round("1s").isoformat(),
        "Last modified": pd.Timestamp.fromtimestamp(file_date_unix).isoformat(),
        "Last updated data": cf.cvm_df["last_modified"].max().isoformat(),
        "Memory size (MB)": round(memory_size / 1024**2, 1),
        "Accounting rows": len(cf.main_df.index),
        "Unique accounting codes": cf.main_df["acc_code"].nunique(),
        "Number of companies": cf.main_df["cvm_id"].nunique(),
        "Unique financial statements": statements_num,
        "First financial statement": first_statement.strftime("%Y-%m-%d"),
        "Last financial statement": last_statement.strftime("%Y-%m-%d"),
    }

    return info_dict


def search_company(expression: str) -> pd.DataFrame:
    """Search for company names in the FinLogic Database.

    This function searches the 'co_name' column in the FinLogic Database for
    company names that contain the provided expression. It returns a DataFrame
    containing the search results, with each row representing a unique company
    that matches the search criteria.

    Args:
        expression (str): A string to search for in the FinLogic Database
            'co_name' column.

    Returns:
        pd.DataFrame: A DataFrame containing the search results, with columns
            'co_name', 'cvm_id', and 'fiscal_id' for each unique company that
            matches the search criteria.
    """
    expression = expression.upper()
    df = (
        cf.main_df.query("co_name.str.contains(@expression)")
        .sort_values(by="co_name")
        .drop_duplicates(subset="cvm_id", ignore_index=True)[
            ["co_name", "cvm_id", "fiscal_id"]
        ]
    )
    return df


def process_language_df():
    """Process language dataframe."""
    language_df = pd.read_csv(cf.URL_LANGUAGE)
    Path.mkdir(cf.INTERIM_DIR, parents=True, exist_ok=True)
    language_df.to_csv(cf.LANGUAGE_DF_PATH, compression="zstd", index=False)
