"""Finlogic Database module.

This module provides functions to handle financial data from the CVM Portal. It
allows updating, processing and consolidating financial statements, as well as
searching for company names in the FinLogic Database and retrieving information
about the database itself.
"""
from typing import Literal
import duckdb
import pandas as pd
from . import config as cfg
from . import cvm
from . import language as lng

CHECKMARK = "\033[32m\u2714\033[0m"

# Initialize FinLogic Database reports table.
SQL_CREATE_REPORTS_TABLE = """
    CREATE OR REPLACE TABLE reports (
        co_name VARCHAR,
        co_id UINTEGER NOT NULL,
        co_fiscal_id VARCHAR,
        report_type VARCHAR NOT NULL,
        report_version UTINYINT NOT NULL,
        period_reference DATE NOT NULL,
        period_begin DATE,
        period_end DATE NOT NULL,
        period_order VARCHAR NOT NULL,
        acc_method VARCHAR NOT NULL,
        acc_code VARCHAR NOT NULL,
        acc_name VARCHAR,
        acc_fixed BOOLEAN NOT NULL,
        acc_value DOUBLE,
        equity_statement_column VARCHAR,
        source_file VARCHAR NOT NULL
    )
"""

# Create reports table in case it does not exist.
table_names = cfg.fldb.execute("PRAGMA show_tables").df()["name"].tolist()
if "reports" not in table_names:
    cfg.fldb.execute(SQL_CREATE_REPORTS_TABLE)

SQL_CREATE_TMP_TABLE = SQL_CREATE_REPORTS_TABLE.replace(
    "TABLE reports", "TEMP TABLE tmp_table"
)


def load_cvm_file(filename: str):
    """Process and load a cvm file in FinLogic Database."""
    df = cvm.process_cvm_file(filename)  # noqa
    # Insert the data in the database
    cfg.fldb.execute("INSERT INTO reports SELECT * FROM df")


def update_cvm_data(filename: str):
    """Proceses and load new cvm data in FinLogic Database."""
    cfg.fldb.execute(SQL_CREATE_TMP_TABLE)

    df = cvm.process_cvm_file(filename)  # noqa
    # Insert the dataframe in the database
    sql_update_data = """
        INSERT    INTO tmp_table
        SELECT    *
        FROM      df;

        INSERT    INTO reports
        SELECT    *
        FROM      tmp_table
        EXCEPT   
        SELECT    *
        FROM      reports;

        DROP      TABLE tmp_table;
    """
    cfg.fldb.execute(sql_update_data)


def build_db():
    """Build FinLogic Database from scratch."""
    print("Building FinLogic Database...")
    filenames_in_raw_folder = [filepath.name for filepath in cvm.CVM_DIR.glob("*.zip")]
    filenames_in_raw_folder.sort()
    for filename in filenames_in_raw_folder:
        load_cvm_file(filename)
        print(f"    {CHECKMARK} {filename} loaded.")


def update_database(rebuild: bool = False):
    """Verify changes in CVM files and update them in Finlogic Database.

    Args:
        rebuild (bool, optional): If True, rebuilds the database from scratch.

    Returns:
        None
    """
    if rebuild:
        # Close database connection
        cfg.fldb.close()
        # Delete database file
        cfg.FINLOGIC_DB_PATH.unlink(missing_ok=True)
        # Create a new database file and connect to it
        cfg.fldb = duckdb.connect(database=f"{cfg.FINLOGIC_DB_PATH}")
        # Create tables
        cfg.fldb.execute(SQL_CREATE_REPORTS_TABLE)

    print('\nUpdating "language" database...')
    lng.process_language_df()

    print("Updating CVM files...")
    urls = cvm.get_all_files_urls()
    # urls = urls[:1]  # Test
    updated_filenames = cvm.update_cvm_files(urls)
    print(f"Number of CVM files updated = {len(updated_filenames)}")
    if not updated_filenames:
        print("All files were already updated.")

    print()
    db_size = cfg.FINLOGIC_DB_PATH.stat().st_size / 1024**2
    # Rebuilt database when it is smaller than 1 MB
    if db_size < 1:
        print("FinLogic Database is empty.")
        build_db()
    else:
        if not updated_filenames:
            print("No new CVM files to load.")
        else:
            print("Load new CVM data in FinLogic Database...")
            for filename in updated_filenames:
                update_cvm_data(filename)
                print(f"    {CHECKMARK} {filename} loaded in FinLogic Database.")

    print(f"\n{CHECKMARK} FinLogic Database updated!")


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
    number_of_rows = cfg.fldb.execute("SELECT COUNT(*) FROM reports").fetchall()[0][0]
    if number_of_rows == 0:
        print("Finlogic Database is empty")
        return

    fldb_file_date_unix = round(cfg.FINLOGIC_DB_PATH.stat().st_mtime, 0)
    fldb_file_date = pd.Timestamp.fromtimestamp(fldb_file_date_unix)
    query = """
        SELECT DISTINCT co_id, report_version, report_type, period_reference
        FROM reports;
    """
    statements_num = cfg.fldb.execute(query).df().shape[0]
    query = "SELECT MIN(period_end) FROM reports"
    first_statement = cfg.fldb.execute(query).fetchall()[0][0]
    query = "SELECT MAX(period_end) FROM reports"
    last_statement = cfg.fldb.execute(query).fetchall()[0][0]
    query = "SELECT COUNT(DISTINCT co_id) FROM reports"
    number_of_companies = cfg.fldb.execute(query).fetchall()[0][0]

    info_dict = {
        "File path": f"{cfg.FINLOGIC_DB_PATH}",
        "File size (MB)": round(cfg.FINLOGIC_DB_PATH.stat().st_size / 1024**2, 1),
        "Last modified": f"{fldb_file_date}",
        "Accounting rows": number_of_rows,
        "Number of companies": number_of_companies,
        "Unique financial statements": statements_num,
        "First financial statement": first_statement.strftime("%Y-%m-%d"),
        "Last financial statement": last_statement.strftime("%Y-%m-%d"),
    }

    return info_dict


def search_company(
    search_value: str, search_by: Literal["name", "id", "fiscal_id"] = "name"
) -> pd.DataFrame:
    """Search for a company name in FinLogic Database.

    This function searches the specified column in the FinLogic Database for
    company names that contain the provided expression. It returns a DataFrame
    containing the search results, with each row representing a unique company
    that matches the search criteria.

    Args:
        search_value (str): The search expression.
        search_by (str): The column where the search will be performed. Valid values
            are 'name', 'id', and 'fiscal_id'. Defaults to 'name'.

    Returns:
        pd.DataFrame: A DataFrame containing the search results, with columns
            'name', 'id', and 'fiscal_id' for each unique company that
            matches the search criteria.
    """
    match search_by:
        case "id":
            sql_condition = f"= {search_value}"
        case "fiscal_id":
            sql_condition = f"LIKE '%{search_value}%'"
        case "name":
            # Company name is stored in uppercase in the database
            sql_condition = f"LIKE '%{search_value.upper()}%'"
        case _:
            raise ValueError("Invalid value for 'search_by' argument.")

    query = f"""
        SELECT DISTINCT co_name AS name, co_id AS id, co_fiscal_id AS fiscal_id
        FROM reports
        WHERE co_{search_by} {sql_condition}
        ORDER BY co_name;
    """
    return cfg.fldb.execute(query).df()
