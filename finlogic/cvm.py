"""Module to download and process CVM data."""
import re
from typing import List
import zipfile as zf
from pathlib import Path
import duckdb
import pandas as pd
import requests
from . import config as cfg

URL_DFP = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/"
URL_ITR = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/"

CHECKMARK = "\033[32m\u2714\033[0m"
RAW_DIR = cfg.DATA_PATH / "cvm" / "raw"
PROCESSED_DIR = cfg.DATA_PATH / "cvm" / "processed"
# Create CVM folders if it does not exist
Path.mkdir(RAW_DIR, parents=True, exist_ok=True)
Path.mkdir(PROCESSED_DIR, parents=True, exist_ok=True)


def get_file_urls(cvm_url) -> List[str]:
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


def get_all_file_urls() -> List[str]:
    """Return a list of all available CVM files."""
    urls_dfp = get_file_urls(URL_DFP)
    urls_itr = get_file_urls(URL_ITR)
    # Get only the last 3 QUARTERLY reports
    urls_itr = urls_itr[-3:]
    urls = urls_dfp + urls_itr
    return urls


def update_raw_file(url: str, s: requests.Session) -> Path:
    """Update raw file from CVM portal. Return a Path if file is updated."""
    filename = url[-23:]  # filename = end of url
    filepath = RAW_DIR / filename
    headers = s.head(url).headers
    filesize = filepath.stat().st_size if filepath.exists() else 0
    if filesize == int(headers["Content-Length"]):
        print(f"    - {filename} is the same -> skip.")
        return None
    r = s.get(url)
    if r.status_code != 200:
        return None

    # Save file with Pathlib
    filepath.write_bytes(r.content)
    print(f"    {CHECKMARK} {filename} updated.")

    return filepath


def update_raw_files(urls: str) -> List[Path]:
    """Update CVM raw files."""
    s = requests.Session()
    updated_filepaths = [update_raw_file(url, s) for url in urls]
    s.close()
    return updated_filepaths


def read_raw_file(filepath: Path) -> pd.DataFrame:
    """Read annual file, process it, save the result and return the file path."""
    cvm_zipfile = zf.ZipFile(filepath)
    child_filenames = cvm_zipfile.namelist()

    # Filename example for the first file in zip: "dfp_cia_aberta_2022.csv"
    # Do not read the first file, since it is a metadata file.
    child_filenames = child_filenames[1:]

    df_list = []
    for child_filename in child_filenames:
        child_zf = cvm_zipfile.open(child_filename)
        child_df = pd.read_csv(child_zf, sep=";", encoding="iso-8859-1")
        df_list.append(child_df)
    df = pd.concat(df_list, ignore_index=True)
    return df


def remove_empty_spaces(s: pd.Series) -> pd.Series:
    """Remove empty spaces in a pandas Series of strings."""
    s_unique_original = pd.Series(s.unique())
    s_unique_adjusted = s_unique_original.replace("\s+", " ", regex=True).str.strip()
    mapping_dict = dict(zip(s_unique_original, s_unique_adjusted))
    return s.map(mapping_dict)


def process_df(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """Format a cvm dataframe."""
    columns_translation = {
        "DENOM_CIA": "name_id",
        "CD_CVM": "cvm_id",
        "CNPJ_CIA": "tax_id",
        "VERSAO": "report_version",
        "DT_REFER": "period_reference",
        "DT_INI_EXERC": "period_begin",
        "DT_FIM_EXERC": "period_end",
        "ORDEM_EXERC": "period_order",
        "CD_CONTA": "acc_code",
        "DS_CONTA": "acc_name",
        "ST_CONTA_FIXA": "acc_fixed",
        "VL_CONTA": "acc_value",
        "COLUNA_DF": "equity_statement",
        # Columns below will be dropped after processing.
        "GRUPO_DFP": "report_group",
        "MOEDA": "Currency",
        "ESCALA_MOEDA": "currency_unit",
    }
    df = df.rename(columns=columns_translation)[columns_translation.values()]

    # Currency column has only one value (BRL) so it is not necessary.
    df = df.drop(columns=["Currency"])

    # Remove rows with acc_value == 0 and acc_fixed == False
    df.query("acc_value != 0 or acc_fixed == True", inplace=True)

    # There are two types of CVM files: DFP (ANNUAL) and ITR (QUARTERLY).
    # In database, "report_type" is positioned after "tax_id" -> position = 3
    if filename.startswith("dfp"):
        df.insert(loc=3, column="report_type", value="ANNUAL")
    else:
        df.insert(loc=3, column="report_type", value="QUARTERLY")

    # Remove any extra spaces (line breaks, tabs, etc.) from columns below.
    columns = ["name_id", "acc_name", "equity_statement"]
    df[columns] = df[columns].apply(remove_empty_spaces)

    # Convert string columns to categorical before mapping.
    columns = df.select_dtypes(include="object").columns
    df[columns] = df[columns].astype("category")

    # "acc_fixed" values are: 'S', 'N'
    map_dic = {"S": True, "N": False}
    df["acc_fixed"] = df["acc_fixed"].map(map_dic).astype(bool)
    # currency_unit values are ['MIL', 'UNIDADE']
    map_dic = {"UNIDADE": 1, "MIL": 1000}
    df["currency_unit"] = df["currency_unit"].map(map_dic).astype(int)

    # Do not ajust acc_value for 3.99 codes.
    df["acc_value"] = df["acc_value"].where(
        df["acc_code"].str.startswith("3.99"),
        df["acc_value"] * df["currency_unit"],
    )
    df.drop(columns=["currency_unit"], inplace=True)

    # "period_order" values are: 'ÚLTIMO', 'PENÚLTIMO'
    map_dic = {"ÚLTIMO": "LAST", "PENÚLTIMO": "PREVIOUS"}
    df["period_order"] = df["period_order"].map(map_dic)
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
    map_dic = {"Con": "CONSOLIDATED", "Ind": "SEPARATE"}
    df.insert(9, "acc_method", df["report_group"].str[3:6].map(map_dic))
    # 'GRUPO_DFP' data can be inferred from 'acc_code'
    df.drop(columns=["report_group"], inplace=True)

    # Correct/harmonize some account texts.
    # df["acc_name"].replace(to_replace=["\xa0ON\xa0", "On"], value="ON", inplace=True)

    # In "itr_cia_aberta_2022.zip", as an example, 2742 rows are duplicated.
    # Few of them have different values in "acc_value". Only one them will be kept.
    # REMOVE ALL VALUES OR MARK THESE ROWS AS ERRORS?
    cols = df.columns.tolist()
    cols.remove("acc_value")
    df.drop_duplicates(subset=cols, keep="last", inplace=True, ignore_index=True)

    return df


def save_processed_df(df: pd.DataFrame, filepath: Path) -> None:
    """Save a processed dataframe as a csv file."""
    # create a DuckDB connection
    con = duckdb.connect()

    # register the DataFrame
    con.register("df", df)

    # write the DataFrame to a Parquet file
    con.execute(f"COPY df TO '{filepath}' (FORMAT 'PARQUET', COMPRESSION 'zstd')")


def process_file(raw_filepath: Path) -> pd.DataFrame:
    """Read, process and save a CVM file."""
    df = read_raw_file(raw_filepath)
    df = process_df(df, raw_filepath.name)
    processed_filepath = PROCESSED_DIR / raw_filepath.name
    save_processed_df(df, processed_filepath)


def process_files(raw_filespaths: List[Path]) -> None:
    """Process a list of CVM raw files."""
    [process_file(raw_filepath) for raw_filepath in raw_filespaths]
