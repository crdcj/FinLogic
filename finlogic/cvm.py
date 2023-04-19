from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import List
import zipfile as zf
from pathlib import Path
import numpy as np
import pandas as pd
import requests
from . import config as cf


URL_DFP = "http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/"
URL_ITR = "http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/"


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


def update_remote_file(url: str) -> Path:
    """Update raw file from CVM portal. Return a Path if file is updated."""
    raw_path = Path(cf.RAW_DIR, url[-23:])  # filename = url final
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
    cf.cvm_df.loc[pd.Timestamp.now()] = [
        raw_path.name,
        r.headers["Content-Length"],
        r.headers["ETag"],
        ts_server,
    ]
    print(f"    {cf.CHECKMARK} {raw_path.name} downloaded.")
    return raw_path


def update_remote_files(urls: str) -> List[Path]:
    """Update local CVM raw files asynchronously."""
    with ThreadPoolExecutor() as executor:
        results = executor.map(update_remote_file, urls)
    cf.cvm_df.to_pickle(cf.CVM_DF_PATH)
    updated_raw_paths = [r for r in results if r is not None]
    return updated_raw_paths


def convert_escala_moeda(escala_str: str) -> int:
    """Convert escala string to integer."""
    if escala_str == "UNIDADE":
        return 1
    elif escala_str == "MIL":
        return 1000
    else:
        raise ValueError(f"Invalid escala string: {escala_str}")


def convert_period_order(period_order_str: str) -> str:
    """Convert period order string to English."""
    if period_order_str == "ÚLTIMO":
        return "LAST"
    elif period_order_str == "PENÚLTIMO":
        return "PREVIOUS"
    else:
        raise ValueError(f"Invalid period order string: {period_order_str}")


def process_zip_file(raw_path: Path) -> Path:
    """Read annual file, process it, save the result and return the file path."""
    df = pd.DataFrame()
    raw_zipfile = zf.ZipFile(raw_path)
    child_filenames = raw_zipfile.namelist()

    df_list = []
    for child_filename in child_filenames[1:]:
        print(f"    {child_filename}")
        child_file = raw_zipfile.open(child_filename)

        raw_child_df = pd.read_csv(
            child_file,
            sep=";",
            encoding="iso-8859-1",
            dtype_backend="numpy_nullable",
            true_values=["S"],
            false_values=["N"],
            converters={
                "ESCALA_MOEDA": convert_escala_moeda,
                "ORDEM_EXERC": convert_period_order,
            },
        )

        # There are two types of CVM files: DFP(annual) and ITR(quarterly).
        if str(raw_path.name)[0:3] == "dfp":
            raw_child_df["reporting_period"] = "ANNUAL"
        else:
            raw_child_df["reporting_period"] = "QUARTERLY"

        df_list.append(raw_child_df)

    df = pd.concat(df_list, ignore_index=True)
    df = format_df(df)
    processed_filepath = cf.PROCESSED_DIR / raw_path.with_suffix(".pkl.zst").name
    df.to_pickle(processed_filepath)
    print(f"    {cf.CHECKMARK} {raw_path.name} processed.")
    return processed_filepath


def process_raw_files(
    workers: int, raw_paths: List[Path], asynchronous: bool
) -> List[Path]:
    """
    Execute function 'process_raw_file' asynchronously and return
    a list with filenames for the processed files.
    """
    if asynchronous:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            results = executor.map(process_zip_file, raw_paths)
        processed_paths = [r for r in results]
    else:
        processed_paths = []
        for raw_path in raw_paths:
            processed_paths.append(process_zip_file(raw_path))

    return processed_paths


def format_df(df: pd.DataFrame) -> pd.DataFrame:
    """Process a raw dataframe
    Data type scan:
        cvm_id: max. value = 600_000
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
    # Most values in columns are repeated
    df = df.astype("category")

    return df
