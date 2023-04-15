"""Finlogic Database module.

This module provides functions to handle financial data from the CVM Portal. It
allows updating, processing and consolidating financial statements, as well as
searching for company names in the FinLogic Database and retrieving information
about the database itself.
"""

import os
from pathlib import Path
import shutil
import zipfile
from typing import List
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import requests
import pandas as pd
import numpy as np
from . import config as c

URL_DFP = "http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/"
URL_ITR = "http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/"
URL_LANGUAGE = "https://raw.githubusercontent.com/fe-lipe-c/finlogic_datasets/master/data/pten_df.csv"  # noqa
RAW_DIR = c.DATA_PATH / "raw"
PROCESSED_DIR = c.DATA_PATH / "processed"
INTERIM_DIR = c.DATA_PATH / "interim"
CHECKMARK = "\033[32m\u2714\033[0m"


def list_urls() -> List[str]:
    """Update the CVM Portal file base.
    Urls with CVM raw files:
    http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/
    http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/
    Links example:
    http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/dfp_cia_aberta_2020.zip
    http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/itr_cia_aberta_2020.zip
    Throughout 2021, there are already DFs for the year 2022 because the
    company's social calendar may not necessarily coincide with the official
    calendar. Because of this, 2 is added to the current year (the second limit
    of the range function is exclusive)
    """
    first_year = 2010  # First year avaible at CVM Portal.
    # Next year files will appear during current year.
    last_year = pd.Timestamp.now().year + 1
    years = list(range(first_year, last_year + 1))
    first_year_itr = last_year - 3
    urls = []
    for year in years:
        filename = f"dfp_cia_aberta_{year}.zip"
        url = f"{URL_DFP}{filename}"
        urls.append(url)
        if year >= first_year_itr:
            filename = f"itr_cia_aberta_{year}.zip"
            url = f"{URL_ITR}{filename}"
            urls.append(url)
    return urls


def update_raw_file(url: str) -> Path:
    """Update raw file from CVM portal. Return a Path if file is updated."""
    raw_path = Path(RAW_DIR, url[-23:])  # filename = url final
    with requests.Session() as s:
        r = s.get(url, stream=True)
        if r.status_code != requests.codes.ok:
            return None

    if Path.exists(raw_path):
        local_file_size = raw_path.stat().st_size
    else:
        local_file_size = 0
    url_file_size = int(r.headers["Content-Length"])
    if local_file_size == url_file_size:
        # File is already updated
        return None
    with raw_path.open(mode="wb") as f:
        f.write(r.content)

    # headers["Last-Modified"] -> 'Wed, 23 Jun 2021 12:19:24 GMT'
    ts_server = pd.to_datetime(
        r.headers["Last-Modified"], format="%a, %d %b %Y %H:%M:%S %Z"
    )
    # Store URL files metadata
    c.cvm_df.loc[pd.Timestamp.now()] = [
        raw_path.name,
        r.headers["Content-Length"],
        r.headers["ETag"],
        ts_server,
    ]
    print(f"    {CHECKMARK} {raw_path.name} downloaded.")
    return raw_path


def update_raw_files(urls: str) -> List[Path]:
    """Update local CVM raw files asynchronously."""
    with ThreadPoolExecutor() as executor:
        results = executor.map(update_raw_file, urls)
    c.cvm_df.to_pickle(c.CVM_DF_PATH)
    updated_raw_paths = [r for r in results if r is not None]
    return updated_raw_paths


def process_raw_file(raw_path: Path) -> Path:
    """Read yearly raw file, process it, save the result and return a file path
    object.
    """
    df = pd.DataFrame()
    raw_zipfile = zipfile.ZipFile(raw_path)
    child_filenames = raw_zipfile.namelist()

    for child_filename in child_filenames[1:]:
        child_file = raw_zipfile.open(child_filename)
        raw_child_df = pd.read_csv(
            child_file, sep=";", encoding="iso-8859-1", dtype=str
        )
        # There are two types of CVM files: DFP(annual) and ITR(quarterly).
        if str(raw_path.name)[0:3] == "dfp":
            raw_child_df["report_type"] = "annual"
        else:
            raw_child_df["report_type"] = "quarterly"

        processed_child_df = process_raw_df(raw_child_df)
        df = pd.concat([df, processed_child_df], ignore_index=True)

    # Most values in columns are repeated
    df = df.astype("category")
    processed_path = PROCESSED_DIR / raw_path.with_suffix(".pkl.zst").name
    df.to_pickle(processed_path)
    print(f"    {CHECKMARK} {raw_path.name} processed.")
    return processed_path


def process_raw_files(
    workers: int, raw_paths: List[Path], asynchronous: bool
) -> List[Path]:
    """
    Execute function 'process_raw_file' asynchronously and return
    a list with filenames for the processed files.
    """
    if asynchronous:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            results = executor.map(process_raw_file, raw_paths)
        processed_paths = [r for r in results]
    else:
        processed_paths = []
        for raw_path in raw_paths:
            processed_paths.append(process_raw_file(raw_path))

    return processed_paths


def process_raw_df(df: pd.DataFrame) -> pd.DataFrame:
    """Process a raw dataframe
    Data type scan:
        report_version: [3, 2, 4, 1, 7, 5, 6, 9, 8]
        currency_unit: ['MIL', 'UNIDADE']
        currency: ['REAL']
        acc_fixed: ['S', 'N']
        period_order: ['PENÚLTIMO', 'ÚLTIMO']
    """
    columns_translation = {
        "CD_CVM": "cvm_id",
        "CNPJ_CIA": "fiscal_id",
        "DENOM_CIA": "co_name",
        "VERSAO": "report_version",
        "DT_INI_EXERC": "period_begin",
        "DT_FIM_EXERC": "period_end",
        "DT_REFER": "period_reference",
        "ORDEM_EXERC": "period_order",
        "CD_CONTA": "acc_code",
        "DS_CONTA": "acc_name",
        "ST_CONTA_FIXA": "acc_fixed",
        "VL_CONTA": "acc_value",
        "COLUNA_DF": "equity_statement_column",
        "MOEDA": "currency",
        "ESCALA_MOEDA": "currency_unit",
    }
    df.rename(columns=columns_translation, inplace=True)
    data_types = {
        "cvm_id": np.int32,
        "fiscal_id": str,
        "co_name": str,
        "report_version": np.int8,
        "period_begin": "datetime64[ns]",
        "period_end": "datetime64[ns]",
        "period_reference": "datetime64[ns]",
        "period_order": str,
        "acc_code": str,
        "acc_name": str,
        "acc_fixed": str,
        "acc_value": float,
        "equity_statement_column": str,
        "currency": str,
        "currency_unit": str,
    }
    df = df.astype(data_types)
    df["report_version"] = df["report_version"].astype(np.int8)
    df["cvm_id"] = df["cvm_id"].astype(np.int32)  # max < 600_000
    df["acc_value"] = df["acc_value"].astype(float)
    df.drop(columns=["currency"], inplace=True)
    df["currency_unit"] = df["currency_unit"].map({"MIL": 1000, "UNIDADE": 1})
    df["acc_codes_level"] = df["acc_code"].str[0:4]
    # Do not adjust earnings per share rows (account codes 3.99...)
    df["acc_value"] = np.where(
        df.acc_codes_level == "3.99",
        df["acc_value"],
        df["acc_value"] * df["currency_unit"],
    )
    df.drop(columns=["currency_unit", "acc_codes_level"], inplace=True)
    df["acc_fixed"] = df["acc_fixed"].map({"S": True, "N": False})
    df["period_order"] = df["period_order"].map({"ÚLTIMO": 0, "PENÚLTIMO": -1})
    df["period_order"] = df["period_order"].astype(np.int8)
    df["period_reference"] = pd.to_datetime(df["period_reference"])
    df["period_end"] = pd.to_datetime(df["period_end"])
    # BPA, BPP and DFC files have no period_begin column.
    if "period_begin" in df.columns:
        df["period_begin"] = pd.to_datetime(df["period_begin"])
    else:
        # column_order.remove('period_begin')
        df["period_begin"] = pd.NaT
    if "equity_statement_column" not in df.columns:
        df["equity_statement_column"] = np.nan

    """
    acc_method -> Financial Statemen Type
    Consolidated and Separate Financial Statements (IAS 27/2003)
    df['GRUPO_DFP'].unique() result:
        'DF Consolidado - Balanço Patrimonial Ativo',
        'DF Consolidado - Balanço Patrimonial Passivo',
        'DF Consolidado - Demonstração das Mutações do Patrimônio Líquido',
        'DF Consolidado - Demonstração de Resultado Abrangente',
        'DF Consolidado - Demonstração de Valor Adicionado',
        'DF Consolidado - Demonstração do Fluxo de Caixa (Método Indireto)',
        'DF Consolidado - Demonstração do Resultado',
        'DF Individual - Balanço Patrimonial Ativo',
        'DF Individual - Balanço Patrimonial Passivo',
        'DF Individual - Demonstração das Mutações do Patrimônio Líquido',
        'DF Individual - Demonstração de Resultado Abrangente',
        'DF Individual - Demonstração de Valor Adicionado',
        'DF Individual - Demonstração do Fluxo de Caixa (Método Indireto)',
        'DF Individual - Demonstração do Resultado',
    Hence, with string position 3:6 we can make:
    if == 'Con' -> consolidated statement
    if == 'Ind' -> separate statement
    """
    df["acc_method"] = (
        df["GRUPO_DFP"].str[3:6].map({"Con": "consolidated", "Ind": "separate"})
    )
    # 'GRUPO_DFP' data can be inferred from 'acc_method' and report_type
    df.drop(columns=["GRUPO_DFP"], inplace=True)
    # Correct/harmonize some account texts.
    df.replace(to_replace=["\xa0ON\xa0", "On"], value="ON", inplace=True)
    # Remove duplicated accounts
    cols = list(df.columns)
    cols.remove("acc_value")
    df.drop_duplicates(cols, keep="last", inplace=True)
    columns_order = [
        "co_name",
        "cvm_id",
        "fiscal_id",
        "report_type",
        "report_version",
        "period_reference",
        "period_begin",
        "period_end",
        "period_order",
        "acc_code",
        "acc_name",
        "acc_method",
        "acc_fixed",
        "acc_value",
        "equity_statement_column",
    ]
    df = df[columns_order]
    return df


def consolidate_main_df(processed_filenames: str):
    # Guard clause: if no raw file was update, there is nothing to consolidate
    if not processed_filenames:
        return

    for filename in processed_filenames:
        updated_df = pd.read_pickle(PROCESSED_DIR / filename)
        c.main_df = pd.concat([c.main_df, updated_df], ignore_index=True)
    c.main_df = c.main_df.astype("category")
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
    c.main_df.sort_values(by=cols, ignore_index=True, inplace=True)
    cols = list(c.main_df.columns)
    cols_remove = ["report_version", "acc_value", "acc_fixed"]
    [cols.remove(col) for col in cols_remove]
    # Ascending order --> last is the newest report_version
    c.main_df.drop_duplicates(cols, keep="last", inplace=True)
    c.main_df.to_pickle(c.MAIN_DF_PATH)


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
        if Path.exists(c.DATA_PATH):
            shutil.rmtree(c.DATA_PATH)
        c.main_df = pd.DataFrame()
    # Create data folders if they do not exist.
    Path.mkdir(RAW_DIR, parents=True, exist_ok=True)
    Path.mkdir(PROCESSED_DIR, parents=True, exist_ok=True)
    # Define the number of cpu cores for parallel data processing.
    workers = int(os.cpu_count() * cpu_usage)
    if workers < 1:
        workers = 1
    print("Updating financial statements...")
    urls = list_urls()
    # urls = urls[:1]  # Test
    raw_paths = update_raw_files(urls)
    print(f"Number of financial statements updated = {len(raw_paths)}")
    print("\nProcessing financial statements...")
    processed_filenames = process_raw_files(
        workers, raw_paths, asynchronous=asynchronous
    )
    print("\nConsolidating processed files...")
    consolidate_main_df(processed_filenames)
    print('Updating "language" database...')
    process_language_df()
    print(f"{CHECKMARK} FinLogic database updated!")


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
    if c.main_df.empty:
        print("Finlogic Database is empty")
        return

    file_date_unix = round(c.MAIN_DF_PATH.stat().st_mtime, 0)
    memory_size = c.main_df.memory_usage(index=True, deep=True).sum()
    statements_cols = ["cvm_id", "report_version", "report_type", "period_reference"]
    statements_num = len(c.main_df.drop_duplicates(subset=statements_cols).index)
    first_statement = c.main_df["period_end"].astype("datetime64[ns]").min()
    last_statement = c.main_df["period_end"].astype("datetime64[ns]").max()

    info_dict = {
        "Path": c.DATA_PATH,
        "File size (MB)": round(c.MAIN_DF_PATH.stat().st_size / 1024**2, 1),
        "Last update call": c.cvm_df.index.max().round("1s").isoformat(),
        "Last modified": pd.Timestamp.fromtimestamp(file_date_unix).isoformat(),
        "Last updated data": c.cvm_df["last_modified"].max().isoformat(),
        "Memory size (MB)": round(memory_size / 1024**2, 1),
        "Accounting rows": len(c.main_df.index),
        "Unique accounting codes": c.main_df["acc_code"].nunique(),
        "Number of companies": c.main_df["cvm_id"].nunique(),
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
        c.main_df.query("co_name.str.contains(@expression)")
        .sort_values(by="co_name")
        .drop_duplicates(subset="cvm_id", ignore_index=True)[
            ["co_name", "cvm_id", "fiscal_id"]
        ]
    )
    return df


def process_language_df():
    """Process language dataframe."""
    language_df = pd.read_csv(URL_LANGUAGE)
    Path.mkdir(INTERIM_DIR, parents=True, exist_ok=True)
    LANGUAGE_DF_PATH = INTERIM_DIR / "pten_df.csv.zst"
    language_df.to_csv(LANGUAGE_DF_PATH, compression="zstd", index=False)
    c.language_df = pd.read_csv(LANGUAGE_DF_PATH)
