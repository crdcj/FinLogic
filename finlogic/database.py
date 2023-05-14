"""Finlogic Database module.

This module provides functions to handle financial data from the CVM Portal. It
allows updating, processing and consolidating financial statements, as well as
searching for company names in the FinLogic Database and retrieving information
about the database itself.
"""
from typing import Literal
import pandas as pd
from . import config as cfg
from . import cvm
from . import language as lng
from . import finprint as fpr
from . import fl_duckdb as fdb

CHECKMARK = "\033[32m\u2714\033[0m"


def get_filepaths_to_process() -> list[str]:
    """Return a list of files in raw folder that must be processed."""
    filenames_in_dir = cvm.get_raw_files_mtime()
    filenames_in_db = fdb.get_db_files_mtime()
    for key, value in filenames_in_db.items():
        if key in filenames_in_dir and filenames_in_dir[key] == value:
            del filenames_in_dir[key]
    filenames_to_process = list(filenames_in_dir.keys())
    return [cfg.CVM_RAW_DIR / filename for filename in filenames_to_process]


def update_database(reset: bool = False):
    """Verify changes in CVM files and update Finlogic Database if necessary.

    Args:
        reset (bool, optional): If True, delete the database file and create a
            new one. Defaults to False.
    Returns:
        None
    """
    # Language files
    print('\nUpdating "language" database...')
    lng.process_language_df()

    # CVM raw files
    print("Updating CVM files...")
    urls = cvm.get_all_file_urls()
    # urls = urls[:1]  # Test
    updated_raw_filepaths = cvm.update_raw_files(urls)
    print(f"Number of CVM files updated = {len(updated_raw_filepaths)}")

    # CVM processed files
    print("\nProcessing CVM files...")
    filepaths_to_process = get_filepaths_to_process()
    print(f"Number of new files to process = {len(filepaths_to_process)}")
    if filepaths_to_process:
        [cvm.process_file(filepath) for filepath in filepaths_to_process]

    # FinLogic Database
    fdb.build()
    print(f"\n{CHECKMARK} FinLogic Database updated!")


def database_info():
    """Print a concise summary of FinLogic Database.

    This function prints a dictionary containing main information about
    FinLogic Database, such as the database path, file size, last update call,
    last modified dates, size in memory, number of accounting rows, unique
    accounting codes, companies, unique financial statements, first financial
    statement date and last financial statement date.

    Args:
        return_dict (bool, optional): If True, returns a dictionary with the
            database information and do not print it.

    Returns: None
    """
    info_dict = fdb.get_info()
    if not info_dict:
        print("FinLogic Database has no data.")
        return
    fpr.print_dict(info_dict=info_dict, table_name="FinLogic Database Info")


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
    match search_by:
        case "name_id":
            # Company name is stored in uppercase in the database
            sql_condition = f"LIKE '%{search_value.upper()}%'"
        case "cvm_id":
            sql_condition = f"= {search_value}"
        case "tax_id":
            sql_condition = f"LIKE '%{search_value}%'"
        case _:
            raise ValueError("Invalid value for 'search_by' argument.")

    query = f"""
        SELECT DISTINCT name_id, cvm_id, tax_id
          FROM reports
         WHERE {search_by} {sql_condition}
         ORDER BY cvm_id;
    """
    return fdb.execute(query, "df")
