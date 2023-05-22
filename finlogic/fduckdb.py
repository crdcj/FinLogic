"""FinLogic Database module."""
from typing import Literal
import duckdb
import pandas as pd
from datetime import datetime
from . import config as cfg
from . import cvm

# Start FinLogic Database connection
FINLOGIC_DB_PATH = cfg.DATA_PATH / "finlogic.db"
# Create a new database file and connect to it
con = duckdb.connect(database=f"{FINLOGIC_DB_PATH}")
con.close()


def reset():
    """Delete the database file and create a new one."""
    # Delete database file
    FINLOGIC_DB_PATH.unlink(missing_ok=True)
    # Create a new database file and connect to it
    con = duckdb.connect(database=f"{FINLOGIC_DB_PATH}")
    con.close()


def execute(query: str, convert_to: Literal["df", "fetchall", "fetchone"] = None):
    """Execute a SQL query."""
    results = None
    with duckdb.connect(database=f"{FINLOGIC_DB_PATH}") as con:
        con.execute(query)
        if convert_to == "df":
            results = con.df()
        elif convert_to == "fetchall":
            results = con.fetchall()
        elif convert_to == "fetchone":
            results = con.fetchone()
    return results


def build():
    """Build FinLogic Database from processed CVM files."""
    print("Building FinLogic Database...")
    # Reset database
    reset()
    df = cvm.read_all_processed_files()
    df = cvm.drop_not_last_entries(df)
    df = cvm.drop_unecessary_quarterly_entries(df)
    # Create a table with all processed CVM files
    execute("CREATE TABLE reports AS SELECT * FROM df")


def is_empty() -> bool:
    """Return True if database is considered empty."""
    return FINLOGIC_DB_PATH.stat().st_size / 1024**2 < 10


def get_info() -> dict:
    """Return a dictionary with information about the database."""
    info = {}
    if is_empty():
        return info

    info["db_path"] = f"{FINLOGIC_DB_PATH}"
    info["db_size"] = f"{FINLOGIC_DB_PATH.stat().st_size / 1024**2:.2f} MB"

    db_last_modified = datetime.fromtimestamp(FINLOGIC_DB_PATH.stat().st_mtime)
    info["db_last_modified"] = db_last_modified.strftime("%Y-%m-%d %H:%M:%S")

    query = "SELECT COUNT(*) FROM reports"
    info["number_of_rows"] = execute(query, "fetchone")[0]

    query = """--sql
        SELECT DISTINCT cvm_id, report_type, period_reference
          FROM reports;
    """
    info["number_of_reports"] = execute(query, "df").shape[0]

    query = "SELECT COUNT(DISTINCT cvm_id) FROM reports"
    info["number_of_companies"] = execute(query, "fetchone")[0]

    query = "SELECT MIN(period_end) FROM reports"
    info["first_report"] = execute(query, "fetchone")[0].strftime("%Y-%m-%d")

    query = "SELECT MAX(period_end) FROM reports"
    info["last_report"] = execute(query, "fetchone")[0].strftime("%Y-%m-%d")

    return info


def get_file_mtimes() -> pd.DataFrame:
    """Return a Pandas DataFrame with unique file_source and file_mtime."""
    if is_empty():
        return pd.DataFrame()

    sql = """
        SELECT DISTINCT file_source, file_mtime
          FROM reports
         ORDER BY file_source
    """
    return execute(sql, "df")
