"""Finlogic Database module.

This module provides functions to handle financial data from the CVM Portal. It
allows updating, processing and consolidating financial statements, as well as
searching for company names in the FinLogic Database and retrieving information
about the database itself.
"""
from typing import Literal, Dict
import duckdb
import pandas as pd
from . import config as cfg
from . import cvm
from . import language as lng
from . import finprint as fpr

CHECKMARK = "\033[32m\u2714\033[0m"


def build_db():
    """Build FinLogic Database from processed CVM files."""
    print("Building FinLogic Database...")
    # Close database connection
    cfg.fldb.close()
    # Delete database file
    cfg.FINLOGIC_DB_PATH.unlink(missing_ok=True)
    # Create a new database file and connect to it
    cfg.fldb = duckdb.connect(database=f"{cfg.FINLOGIC_DB_PATH}")
    # Create a table with all processed CVM files
    sql = f"""
        CREATE OR REPLACE TABLE reports AS SELECT * FROM '{cvm.PROCESSED_DIR}/*.parquet'
    """
    cfg.fldb.execute(sql)


# Get a dicionary with the filenames and their respective modified times in database
def get_db_files_mtime() -> Dict[str, float]:
    """Return a dictionary with the file sources and their respective modified times in
    database."""
    sql = """
        SELECT DISTINCT file_source, file_mtime FROM reports
        ORDER BY file_source
    """
    df = cfg.fldb.execute(sql).df()
    return df.set_index("file_source")["file_mtime"].to_dict()


def get_filepaths_to_process() -> list[str]:
    """Return a list of files in raw folder that must be processed."""
    filenames_in_dir = cvm.get_raw_files_mtime()
    filenames_in_db = get_db_files_mtime()
    for key, value in filenames_in_db.items():
        if key in filenames_in_dir and filenames_in_dir[key] == value:
            del filenames_in_dir[key]
    filenames_to_process = list(filenames_in_dir.keys())
    return [cvm.RAW_DIR / filename for filename in filenames_to_process]


def update_database():
    """Verify changes in CVM files and update Finlogic Database if necessary.

    Args:

    Returns:
        None
    """
    print('\nUpdating "language" database...')
    lng.process_language_df()

    print("Updating CVM files...")
    urls = cvm.get_all_file_urls()
    # urls = urls[:1]  # Test
    updated_raw_filepaths = cvm.update_raw_files(urls)
    print(f"Number of CVM files updated = {len(updated_raw_filepaths)}")

    filepaths_to_process = get_filepaths_to_process()
    print(f"Number of new files to process = {len(filepaths_to_process)}")

    if filepaths_to_process:
        [cvm.process_file(filepath) for filepath in filepaths_to_process]

    print()
    build_db()
    print(f"\n{CHECKMARK} FinLogic Database updated!")


def database_info(return_dict: bool = False):
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
    number_of_rows = cfg.fldb.execute("SELECT COUNT(*) FROM reports").fetchall()[0][0]
    if number_of_rows == 0:
        print("Finlogic Database is empty")
        return

    fldb_file_date_unix = round(cfg.FINLOGIC_DB_PATH.stat().st_mtime, 0)
    fldb_file_date = pd.Timestamp.fromtimestamp(fldb_file_date_unix)
    query = """
        SELECT DISTINCT cvm_id, report_version, report_type, period_reference
          FROM reports;
    """
    statements_num = cfg.fldb.execute(query).df().shape[0]
    query = "SELECT MIN(period_end) FROM reports"
    first_statement = cfg.fldb.execute(query).fetchall()[0][0]
    query = "SELECT MAX(period_end) FROM reports"
    last_statement = cfg.fldb.execute(query).fetchall()[0][0]
    query = "SELECT COUNT(DISTINCT cvm_id) FROM reports"
    number_of_companies = cfg.fldb.execute(query).fetchall()[0][0]

    info_dict = {
        "File path": f"{cfg.FINLOGIC_DB_PATH}",
        "File size (MB)": round(cfg.FINLOGIC_DB_PATH.stat().st_size / 1024**2, 1),
        "Last modified": f"{fldb_file_date}",
        "Accounting rows": number_of_rows,
        "Number of companies": number_of_companies,
        "Number of financial statements": statements_num,
        "First financial statement": first_statement.strftime("%Y-%m-%d"),
        "Last financial statement": last_statement.strftime("%Y-%m-%d"),
    }
    if return_dict:
        return info_dict
    else:
        fpr.print_dict(info_dict=info_dict, table_name="FinLogic Database Info")
        return None


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
         ORDER BY name_id;
    """
    return cfg.fldb.execute(query).df()
