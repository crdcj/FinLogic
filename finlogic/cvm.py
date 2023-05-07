"""CVM Portal data management."""
import re
from typing import List
import zipfile as zf
from pathlib import Path
import pandas as pd
import requests
from . import config as cfg
from .config import fldb as fldb

CHECKMARK = "\033[32m\u2714\033[0m"
CVM_DIR = cfg.DATA_PATH / "cvm"
# Create CVM_DIR if it does not exist
Path.mkdir(CVM_DIR, parents=True, exist_ok=True)

SQL_CREATE_CVM_TABLE = """
    CREATE OR REPLACE TABLE cvm_files (
        name VARCHAR NOT NULL,
        size UINTEGER NOT NULL,
        etag VARCHAR NOT NULL,
        last_modified TIMESTAMP,
        last_accessed TIMESTAMP NOT NULL,
    )
"""

# Create cvm table in case it does not exist
table_names = fldb.execute("PRAGMA show_tables").df()["name"].tolist()
if "cvm_files" not in table_names:
    fldb.execute(SQL_CREATE_CVM_TABLE)

cvm_df = fldb.execute("SELECT * FROM cvm_files").df()
# Keep only the last_modified files
cvm_df = (
    cvm_df.sort_values("last_modified")
    .drop_duplicates(subset=["name"], keep="last")
    .sort_values("name", ignore_index=True)
)


def get_available_file_urls(cvm_url) -> List[str]:
    """Return a list of available CVM files.

    Urls with CVM raw files:
    https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/
    https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/
    Links example:
    http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/dfp_cia_aberta_2020.zip
    http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/itr_cia_aberta_2020.zip
    """
    available_files = []
    r = requests.get(cvm_url)
    if r.status_code != 200:
        return available_files
    # Use a regular expression to match and extract all the file links
    matches = re.findall(r'href="(.+\.zip)"', r.text)
    # Add matches to the file list
    available_files.extend(matches)
    available_files.sort()
    # Add the base url to the filename
    available_file_urls = [cvm_url + filename for filename in available_files]
    return available_file_urls


def update_cvm_file(url: str, s) -> dict:
    """Update raw file from CVM portal. Return a Path if file is updated."""
    filename = url[-23:]  # filename = end of url
    filepath = Path(CVM_DIR, filename)
    headers = s.head(url).headers

    # headers["Last-Modified"] -> 'Wed, 23 Jun 2021 12:19:24 GMT'
    last_modified = pd.to_datetime(
        headers["Last-Modified"], format="%a, %d %b %Y %H:%M:%S %Z"
    )
    url_data = {
        "name": filename,
        "size": int(headers["Content-Length"]),
        "etag": headers["ETag"],
        "update_done": False,
        "last_modified": last_modified,
        "last_accessed": pd.Timestamp.now(),
    }

    # Get the file dataframe from cvm_df
    file_df = cvm_df.query(f"name == '{filename}'")
    last_etag = ""
    if not file_df.empty:
        # Get the last etag in the file dataframe
        last_etag = file_df["etag"].iloc[-1]
    if last_etag == url_data["etag"]:
        # File is the same, no need to update it
        print(f"    - {filename} already updated.")
        return url_data
    r = s.get(url)
    if r.status_code != requests.codes.ok:
        return None

    # Save the file with Pathlib
    filepath.write_bytes(r.content)
    print(f"    {CHECKMARK} {filename} updated.")
    url_data["update_done"] = True

    return url_data


def update_cvm_files(urls: str) -> List[dict]:
    """Update CVM raw files."""
    s = requests.Session()
    urls_data = []
    for url in urls:
        urls_data.append(update_cvm_file(url, s))
    s.close()
    return urls_data


def read_cvm_file(cvm_filename: str) -> pd.DataFrame:
    """Read annual file, process it, save the result and return the file path."""
    df = pd.DataFrame()
    cvm_filepath = Path(CVM_DIR, cvm_filename)
    annual_zipfile = zf.ZipFile(cvm_filepath)
    child_filenames = annual_zipfile.namelist()

    df_list = []
    for child_filename in child_filenames[1:]:
        child_file = annual_zipfile.open(child_filename)

        # Only "DT_INI_EXERC" and "COLUNA_DF" have missing values.
        child_df = pd.read_csv(
            child_file,
            sep=";",
            encoding="iso-8859-1",
            true_values=["S"],
            false_values=["N"],
        )
        # Currency column has only one value (BRL) so it is not necessary.
        child_df = child_df.drop(columns=["MOEDA"])

        # There are two types of CVM files: DFP (ANNUAL) and ITR (QUARTERLY).
        if cvm_filepath.name.startswith("dfp"):
            child_df["TIPO"] = "ANNUAL"
        else:
            child_df["TIPO"] = "QUARTERLY"

        df_list.append(child_df)

    df = pd.concat(df_list, ignore_index=True)
    df["ARQUIVO"] = cvm_filepath.name
    # Convert string columns to categorical.
    columns = df.select_dtypes(include="object").columns
    df[columns] = df[columns].astype("category")
    return df


def format_cvm_df(df: pd.DataFrame) -> pd.DataFrame:
    """Format a cvm dataframe."""
    columns_translation = {
        "DENOM_CIA": "co_name",
        "CD_CVM": "co_id",
        "CNPJ_CIA": "co_fiscal_id",
        "TIPO": "report_type",
        "VERSAO": "report_version",
        "DT_REFER": "period_reference",
        "DT_INI_EXERC": "period_begin",
        "DT_FIM_EXERC": "period_end",
        "ORDEM_EXERC": "period_order",
        "CD_CONTA": "acc_code",
        "DS_CONTA": "acc_name",
        "ST_CONTA_FIXA": "acc_fixed",
        "VL_CONTA": "acc_value",
        "COLUNA_DF": "equity_statement_column",
        "ARQUIVO": "data_source",
        # Columns below will be dropped after processing.
        "GRUPO_DFP": "report_group",
        "ESCALA_MOEDA": "currency_unit",
    }
    df = df.rename(columns=columns_translation)[columns_translation.values()]

    # currency_unit values are ['MIL', 'UNIDADE']
    map_dict = {"UNIDADE": 1, "MIL": 1000}
    df["currency_unit"] = df["currency_unit"].map(map_dict).astype(int)

    # Do not ajust acc_value for 3.99 codes.
    df["acc_value"] = df["acc_value"].where(
        df["acc_code"].str.startswith("3.99"),
        df["acc_value"] * df["currency_unit"],
    )
    df.drop(columns=["currency_unit"], inplace=True)

    # "period_order" values are: 'ÚLTIMO', 'PENÚLTIMO'
    map_dict = {"ÚLTIMO": "LAST", "PENÚLTIMO": "PREVIOUS"}
    df["period_order"] = df["period_order"].map(map_dict)
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
    map_dict = {"Con": "CONSOLIDATED", "Ind": "SEPARATE"}
    df.insert(9, "acc_method", df["report_group"].str[3:6].map(map_dict))
    # 'GRUPO_DFP' data can be inferred from 'acc_code'
    df.drop(columns=["report_group"], inplace=True)
    # Correct/harmonize some account texts.
    df["acc_name"].replace(to_replace=["\xa0ON\xa0", "On"], value="ON", inplace=True)
    # From 20_636_037 rows 2_383 were duplicated on 2023-04-30 -> remove duplicates
    cols = df.columns.tolist()
    cols.remove("acc_value")
    df.drop_duplicates(subset=cols, keep="last", inplace=True)

    return df


def process_cvm_file(cvm_filename: str) -> pd.DataFrame:
    """Read and format a CVM file."""
    df = read_cvm_file(cvm_filename)
    df = format_cvm_df(df)
    return df
