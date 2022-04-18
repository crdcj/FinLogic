"""Module containing local database functions."""
import os
from pathlib import Path
import shutil
import math
import zipfile
from typing import List
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import requests
import pandas as pd
import numpy as np
from . import config as c

URL_DFP = "http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/"
URL_ITR = "http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/"

RAW_DIR = c.DATA_PATH / "raw"
PROCESSED_DIR = c.DATA_PATH / "processed"


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
    """Update file from CVM portal. Return a Path object if file is updated."""
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
            return None
        with raw_path.open(mode="wb") as f:
            f.write(r.content)
        return raw_path


def update_raw_files(urls: str) -> List[Path]:
    """Update local CVM raw files asynchronously."""
    with ThreadPoolExecutor() as executor:
        results = executor.map(update_raw_file, urls)
    updated_raw_paths = [r for r in results if r is not None]
    return updated_raw_paths


def process_raw_file(raw_path: Path) -> Path:
    """
    Read yearly raw file, process it, save the result and return a file path
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
    return processed_path


def process_yearly_files(workers: int, raw_paths: List[Path]) -> List[Path]:
    """
    Execute function 'process_raw_file' asynchronously and return
    a list with filenames for the processed files.
    """
    with ProcessPoolExecutor(max_workers=workers) as executor:
        results = executor.map(process_raw_file, raw_paths)
    processed_paths = [r for r in results]
    return processed_paths


def consolidate_main_df(processed_filenames: str):
    # Guard clause: if no raw file was update, there is nothing to consolidate
    if not processed_filenames:
        return

    for filename in processed_filenames:
        updated_df = pd.read_pickle(PROCESSED_DIR / filename)
        c.main_df = pd.concat([c.main_df, updated_df], ignore_index=True)
    c.main_df = c.main_df.astype("category")
    # Keep only the newest 'report_version' in df if values are repeated
    # print(len(main_df.index))
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
    # tmp = main_df[main_df.duplicated(cols, keep=False)]
    # Ascending order --> last is the newest report_version
    c.main_df.drop_duplicates(cols, keep="last", inplace=True)
    # print(len(main_df.index))
    c.main_df.to_pickle(c.MAIN_DF_PATH)


def search_company(expression: str) -> pd.DataFrame:
    """
    Search companies names in database that contains the ```expression```

    Parameters
    ----------
    expression : str
        A expression to search in as fi column 'co_name'.

    Returns
    -------
    pd.DataFrame with search results
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


def database_info() -> pd.DataFrame:
    """
    Return information about FinLogic database

    Returns
    -------
    pd.DataFrame
    """
    info_df = pd.DataFrame()
    info_df.index.name = "FinLogic Database Info"
    info_df["Value"] = ""
    info_df.loc["Database Path"] = c.DATA_PATH
    info_df.loc["File Size (MB)"] = round(
        os.path.getsize(c.MAIN_DF_PATH) / (1024 * 1024), 1
    )
    last_modified_python = os.path.getmtime(c.MAIN_DF_PATH)
    # Convert python epoch to Pandas datetime
    last_modified_python = os.path.getmtime(c.MAIN_DF_PATH)
    last_modified_pandas = pd.to_datetime(last_modified_python, unit="s").round("1s")
    info_df.loc["Last Modified (MB)"] = last_modified_pandas
    info_df.loc["Size in Memory (MB)"] = round(
        c.main_df.memory_usage(index=True, deep=True).sum() / (1024 * 1024), 1
    )
    info_df.loc["Accounting Rows"] = len(c.main_df.index)
    info_df.loc["Unique Accounting Codes"] = c.main_df["acc_code"].nunique()
    info_df.loc["Companies"] = c.main_df["cvm_id"].nunique()
    columns_duplicates = ["cvm_id", "report_version", "report_type", "period_reference"]
    info_df.loc["Unique Financial Statements"] = len(
        c.main_df.drop_duplicates(subset=columns_duplicates).index
    )
    info_df.loc["First Financial Statement"] = (
        c.main_df["period_end"].astype("datetime64").min().strftime("%Y-%m-%d")
    )
    info_df.loc["Last Financial Statement"] = (
        c.main_df["period_end"].astype("datetime64").max().strftime("%Y-%m-%d")
    )
    return info_df


def update_database(cpu_usage: float = 0.75, reset_data: bool = False):
    """
    Create/Update all remote files (raw files) and process them for local data
    access.

    Parameters
    ----------
    cpu_usage: float, default 0.75
        A number between 0 and 1, where 1 represents 100% CPU usage. This
        argument will define the number of cpu cores used for data processing.
    reset_data: bool, default True
        Delete all raw files and force a full database recompilation
    Returns
    -------
    None
    """
    # Parameter 'reset_data'-> delete, if they exist, data folders
    if reset_data:
        if Path.exists(c.DATA_PATH):
            shutil.rmtree(c.DATA_PATH)
        c.main_df = pd.DataFrame()

    # create data folders if they do not exist
    Path.mkdir(RAW_DIR, parents=True, exist_ok=True)
    Path.mkdir(PROCESSED_DIR, parents=True, exist_ok=True)

    # Define the number of cpu cores for parallel data processing
    workers = math.trunc(os.cpu_count() * cpu_usage)
    if workers < 1:
        workers = 1

    print("Updating CVM raw files...")
    urls = list_urls()
    raw_paths = update_raw_files(urls)
    print(f"Number of CVM raw files updated = {len(raw_paths)}")
    print("Processing CVM raw files...")
    processed_filenames = process_yearly_files(workers, raw_paths)
    print("Consolidating processed files...")
    consolidate_main_df(processed_filenames)
    print("FinLogic database updated \u2705")


def process_raw_df(df: pd.DataFrame) -> pd.DataFrame:
    """Process a raw dataframe"""

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

    # df['report_version'].unique()
    # ['3', '2', '4', '1', '7', '5', '6', '9', '8']
    df["report_version"] = df["report_version"].astype(np.int8)
    df["cvm_id"] = df["cvm_id"].astype(np.int32)  # max < 600_000
    df["acc_value"] = df["acc_value"].astype(float)

    # df.query("acc_value == 0") -> 10.891.139 rows from 17.674.199
    # Zero values will not be used.
    # df.query("acc_value != 0", inplace=True)

    # df['currency'].value_counts() -> REAL    43391302
    df.drop(columns=["currency"], inplace=True)

    # df['currency_unit'].value_counts()
    #   MIL        40483230
    #   UNIDADE     2908072
    df["currency_unit"] = df["currency_unit"].map({"MIL": 1000, "UNIDADE": 1})

    # Unit base currency.
    df["acc_codes_level"] = df["acc_code"].str[0:4]
    # Do not adjust earnings per share rows (account codes 3.99...)
    df["acc_value"] = np.where(
        df.acc_codes_level == "3.99",
        df["acc_value"],
        df["acc_value"] * df["currency_unit"],
    )
    df.drop(columns=["currency_unit", "acc_codes_level"], inplace=True)

    # df['acc_fixed'].unique() -> ['S', 'N']
    df["acc_fixed"] = df["acc_fixed"].map({"S": True, "N": False})

    # df['period_order'].unique() -> ['PENÚLTIMO', 'ÚLTIMO']
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
