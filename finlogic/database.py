"""Finlogic Database module.

This module provides functions to handle financial data from the CVM Portal. It
allows updating, processing and consolidating financial statements, as well as
searching for company names in the FinLogic Database and retrieving information
about the database itself.
"""
from typing import List
from pathlib import Path
import pandas as pd
from . import config as cfg
from . import cvm

SQL_CREATE_TABLE = """
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


def db_execute(sql_statement: str):
    """Execute a command in FinLogic Database."""
    cfg.conn.execute(sql_statement)


def build_finlogic_df():
    # Get all files in processed folder
    filepaths = list(cfg.PROCESSED_DIR.glob("*.zst"))
    # Guard clause: if no raw file was update, there is nothing to consolidate
    if not filepaths:
        print("No processed files to build FinLogic Database.")
        return
    # Concatenate all processed files into a single dataframe
    finlogic_df = pd.concat(
        [pd.read_pickle(filepath) for filepath in filepaths],
        ignore_index=True,
    )
    # Most values in datetime and string columns are the same.
    # So these remaining columns can be converted to category.
    columns = finlogic_df.select_dtypes(include=["datetime64[ns]", "object"]).columns
    finlogic_df[columns] = finlogic_df[columns].astype("category")
    # Keep only the newest 'report_version' in df if values are repeated
    cols = [
        "co_id",
        "report_type",
        "period_reference",
        "report_version",
        "period_order",
        "acc_method",
        "acc_code",
    ]
    finlogic_df.sort_values(by=cols, ignore_index=True, inplace=True)
    cols = finlogic_df.columns.tolist()
    cols_remove = ["report_version", "acc_value", "acc_fixed"]
    [cols.remove(col) for col in cols_remove]
    # Ascending order --> last is the newest report_version
    finlogic_df.drop_duplicates(cols, keep="last", inplace=True)
    finlogic_df.to_pickle(cfg.FINLOGIC_DF_PATH)


def get_filenames_to_load(filenames_updated) -> List[str]:
    # Get existing filestems in raw folder
    filenames_in_raw_folder = [filepath.name for filepath in cfg.RAW_DIR.glob("*.zip")]
    # Get filenames in finlogic database
    sql = "SELECT DISTINCT source_file FROM reports"
    filenames_in_db = db_execute(sql).df()["source_file"].tolist()
    filenames_not_in_db = set(filenames_in_raw_folder) - set(filenames_in_db)
    filenames_to_process = list(set(filenames_updated) | filenames_not_in_db)
    filenames_to_process.sort()
    return filenames_to_process


def reset_db():
    """Create FinLogic Database if it doesn't exist."""
    cfg.con.execute(SQL_CREATE_TABLE)


def load_cvm_data(filename: str):
    """Read, format and load a cvm file in FinLogic Database."""
    df = cvm.process_cvm_file(filename)
    # Insert the data in the database
    cfg.con.register("annual_dataframe", df)
    db_execute("INSERT INTO reports SELECT * FROM annual_dataframe")


def update_cvm_data(filename: str):
    """Read, format and load a cvm file in FinLogic Database."""
    sql_tmp_table = SQL_CREATE_TABLE.replace("reports", "tmp_table")
    db_execute(sql_tmp_table)

    df = cvm.process_cvm_file(filename)
    # Insert the dataframe in the database
    sql_update_db = """
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
    db_execute(sql_update_db)


def build_db():
    """Build FinLogic Database from scratch."""
    print("Building FinLogic Database...")
    filenames_in_raw_folder = [filepath.name for filepath in cfg.RAW_DIR.glob("*.zip")]
    for filename in filenames_in_raw_folder:
        load_cvm_data(filename)
        print(f"    {cfg.CHECKMARK} {filename} loaded in FinLogic Database.")
    print(f"{cfg.CHECKMARK} FinLogic Database built!")


def update_database():
    """Verify changes in remote files and update them in Finlogic Database.

    Args:

    Returns:
        None
    """
    print("Updating CVM files...")
    urls = cvm.list_urls()
    # urls = urls[:1]  # Test
    updated_cvm_filenames = cvm.update_raw_files(urls)
    print(f"Number of CVM files updated = {len(updated_cvm_filenames)}")
    if updated_cvm_filenames:
        print("Updated files:")
        for updated_filepath in updated_cvm_filenames:
            print(f"    {cfg.CHECKMARK} {updated_filepath.stem} updated.")
    else:
        print("All files are up to date.")

    print('Updating "language" database...')
    process_language_df()

    db_size = cfg.FINLOGIC_DB_PATH.stat().st_size / 1024**2
    # Rebuilt database when it is smaller than 1 MB
    if db_size < 1:
        print("FinLogic Database is empty and will be rebuilt.")
        reset_db()
        print("Loading all files in FinLogic Database...")
        build_db()

    else:
        print("\nUpdate CVM data in FinLogic Database...")
        filenames_to_load = get_filenames_to_load(updated_cvm_filenames)
        for filename in filenames_to_load:
            update_cvm_data(filename)
            print(f"    {cfg.CHECKMARK} {filename} updated in FinLogic Database.")

    print(f"{cfg.CHECKMARK} FinLogic database updated!")


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
    number_of_rows = db_execute("SELECT COUNT(*) FROM reports").fetchall()[0][0]
    if number_of_rows == 0:
        print("Finlogic Database is empty")
        return

    cvm_df = cvm.get_cvm_df()
    file_date_unix = round(cfg.FINLOGIC_DF_PATH.stat().st_mtime, 0)
    memory_size = finlogic_df.memory_usage(index=True, deep=True).sum()
    statements_cols = ["co_id", "report_version", "report_type", "period_reference"]
    statements_num = len(finlogic_df.drop_duplicates(subset=statements_cols).index)
    first_statement = finlogic_df["period_end"].astype("datetime64[ns]").min()
    last_statement = finlogic_df["period_end"].astype("datetime64[ns]").max()

    info_dict = {
        "Path": cfg.DATA_PATH,
        "File size (MB)": round(cfg.FINLOGIC_DF_PATH.stat().st_size / 1024**2, 1),
        "Last update call": cvm_df.index.max().round("1s").isoformat(),
        "Last modified": pd.Timestamp.fromtimestamp(file_date_unix).isoformat(),
        "Last updated data": cvm_df["last_modified"].max().isoformat(),
        "Memory size (MB)": round(memory_size / 1024**2, 1),
        "Accounting rows": len(finlogic_df.index),
        "Unique accounting codes": finlogic_df["acc_code"].nunique(),
        "Number of companies": finlogic_df["co_id"].nunique(),
        "Unique financial statements": statements_num,
        "First financial statement": first_statement.strftime("%Y-%m-%d"),
        "Last financial statement": last_statement.strftime("%Y-%m-%d"),
    }

    return info_dict


def search_company(company_name: str) -> pd.DataFrame:
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

    company_name = company_name.upper()
    df = (
        pd.read_pickle(cfg.FINLOGIC_DF_PATH)
        .query("co_name.str.contains(@company_name)", engine="python")
        .sort_values(by="co_name")
        .drop_duplicates(subset="co_id", ignore_index=True)[
            ["co_name", "co_id", "co_fiscal_id"]
        ]
    )
    return df


def process_language_df():
    """Process language dataframe."""
    language_df = pd.read_csv(cfg.URL_LANGUAGE)
    Path.mkdir(cfg.INTERIM_DIR, parents=True, exist_ok=True)
    language_df.to_csv(cfg.LANGUAGE_DF_PATH, compression="zstd", index=False)
