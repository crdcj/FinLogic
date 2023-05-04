"""Finlogic Database module.

This module provides functions to handle financial data from the CVM Portal. It
allows updating, processing and consolidating financial statements, as well as
searching for company names in the FinLogic Database and retrieving information
about the database itself.
"""
from typing import List, Literal
from pathlib import Path
import pandas as pd
import duckdb as ddb
from . import config as cfg
from . import cvm

# Start FinLogic Database connection
FINLOGIC_DB_PATH = cfg.DATA_PATH / "finlogic.db"
con = ddb.connect(database=f"{FINLOGIC_DB_PATH}")
# Initialize FinLogic Database main table.
SQL_CREATE_MAIN_TABLE = """
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
# Create reports table if there is no table in the database
table_names = con.execute("PRAGMA show_tables;").fetchall()
if not table_names:
    con.execute(SQL_CREATE_MAIN_TABLE)


def get_filenames_to_load(filenames_updated) -> List[str]:
    # Get existing filestems in raw folder
    filenames_in_raw_folder = [filepath.name for filepath in cfg.CVM_DIR.glob("*.zip")]
    # Get filenames in finlogic database
    sql = "SELECT DISTINCT source_file FROM reports"
    filenames_in_db = con.execute(sql).df()["source_file"].tolist()
    filenames_not_in_db = set(filenames_in_raw_folder) - set(filenames_in_db)
    filenames_to_process = list(set(filenames_updated) | filenames_not_in_db)
    filenames_to_process.sort()
    return filenames_to_process


def load_cvm_file(filename: str):
    """Read, format and load a cvm file in FinLogic Database."""
    df = cvm.process_cvm_file(filename)  # noqa
    # Insert the data in the database
    con.execute("INSERT INTO reports SELECT * FROM df")


def update_cvm_file(filename: str):
    """Read, format and load a cvm file in FinLogic Database."""
    sql_tmp_table = SQL_CREATE_MAIN_TABLE.replace(
        "TABLE reports", "TEMP TABLE tmp_table"
    )
    con.execute(sql_tmp_table)

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
    con.execute(sql_update_data)


def build_db():
    """Build FinLogic Database from scratch."""
    print("Building FinLogic Database...")
    filenames_in_raw_folder = [filepath.name for filepath in cfg.CVM_DIR.glob("*.zip")]
    filenames_in_raw_folder.sort()
    for filename in filenames_in_raw_folder:
        load_cvm_file(filename)
        print(f"    {cfg.CHECKMARK} {filename} loaded.")


def update_database():
    """Verify changes in CVM files and update them in Finlogic Database.

    Args:

    Returns:
        None
    """
    print("Updating CVM files...")
    urls = cvm.list_urls()
    # urls = urls[:1]  # Test
    updated_cvm_filenames = cvm.update_cvm_files(urls)
    print(f"Number of CVM files updated = {len(updated_cvm_filenames)}")
    if updated_cvm_filenames:
        print("Updated files:")
        for updated_filename in updated_cvm_filenames:
            print(f"    {cfg.CHECKMARK} {updated_filename} updated.")
    else:
        print("All files are up to date.")

    print('\nUpdating "language" database...')
    process_language_df()

    db_size = FINLOGIC_DB_PATH.stat().st_size / 1024**2
    # Rebuilt database when it is smaller than 1 MB
    if db_size < 1:
        print("FinLogic Database is empty.")
        print("Loading all CVM files in FinLogic Database...")
        build_db()

    else:
        print("\nUpdate CVM data in FinLogic Database...")
        filenames_to_load = get_filenames_to_load(updated_cvm_filenames)
        for filename in filenames_to_load:
            update_cvm_file(filename)
            print(f"    {cfg.CHECKMARK} {filename} updated in FinLogic Database.")

    print(f"\n{cfg.CHECKMARK} FinLogic database updated!")


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
    number_of_rows = con.execute("SELECT COUNT(*) FROM reports").fetchall()[0][0]
    if number_of_rows == 0:
        print("Finlogic Database is empty")
        return

    cvm_df = cvm.get_cvm_df()
    file_date_unix = round(FINLOGIC_DB_PATH.stat().st_mtime, 0)
    query = """
        SELECT DISTINCT co_id, report_version, report_type, period_reference
        FROM reports;
    """
    statements_num = con.execute(query).df().shape[0]
    query = "SELECT MIN(period_end) FROM reports"
    first_statement = con.execute(query).fetchall()[0][0]
    query = "SELECT MAX(period_end) FROM reports"
    last_statement = con.execute(query).fetchall()[0][0]
    query = "SELECT COUNT(DISTINCT co_id) FROM reports"
    number_of_companies = con.execute(query).fetchall()[0][0]

    info_dict = {
        "Path": cfg.DATA_PATH,
        "File size (MB)": round(FINLOGIC_DB_PATH.stat().st_size / 1024**2, 1),
        "Last update call": cvm_df.index.max().round("1s").isoformat(),
        "Last modified": pd.Timestamp.fromtimestamp(file_date_unix).isoformat(),
        "Last updated data": cvm_df["last_modified"].max().isoformat(),
        "Accounting rows": number_of_rows,
        "Number of companies": number_of_companies,
        "Unique financial statements": statements_num,
        "First financial statement": first_statement.strftime("%Y-%m-%d"),
        "Last financial statement": last_statement.strftime("%Y-%m-%d"),
    }

    return info_dict


def search_company(
    value: str, by: Literal["name", "id", "fiscal_id"] = "co_name"
) -> pd.DataFrame:
    """Search for a company name in FinLogic Database.

    This function searches the 'co_name' column in the FinLogic Database for
    company names that contain the provided expression. It returns a DataFrame
    containing the search results, with each row representing a unique company
    that matches the search criteria.

    Args:
        company_name (str): A string to search for in the FinLogic Database
            'co_name' column.

    Returns:
        pd.DataFrame: A DataFrame containing the search results, with columns
            'co_name', 'co_id', and 'co_fiscal_id' for each unique company that
            matches the search criteria.
    """
    if by == "id":
        sql_expression = f"= {value}"
    else:
        # Names are stored in uppercase
        sql_expression = f"LIKE '%{value.upper()}%'"
    query = f"""
        SELECT DISTINCT co_name AS name, co_id AS id, co_fiscal_id AS fiscal_id
        FROM reports
        WHERE co_{by} {sql_expression}
        ORDER BY co_name;
    """
    return con.execute(query).df()


def process_language_df():
    """Process language dataframe."""
    language_df = pd.read_csv(cfg.URL_LANGUAGE)
    Path.mkdir(cfg.INTERIM_DIR, parents=True, exist_ok=True)
    language_df.to_csv(cfg.LANGUAGE_DF_PATH, compression="zstd", index=False)
