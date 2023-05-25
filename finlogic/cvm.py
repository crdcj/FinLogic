"""Module to download and process CVM data."""
import re
from typing import List
import zipfile as zf
from pathlib import Path
from tqdm import tqdm
import pandas as pd
import numpy as np
import requests
from . import config as cfg

URL_DFP = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/"
URL_ITR = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/"

CVM_RAW_DIR = cfg.DATA_PATH / "cvm" / "raw"
CVM_PROCESSED_DIR = cfg.DATA_PATH / "cvm" / "processed"
# Create CVM folders if they do not exist
Path.mkdir(CVM_RAW_DIR, parents=True, exist_ok=True)
Path.mkdir(CVM_PROCESSED_DIR, parents=True, exist_ok=True)


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
    filepath = CVM_RAW_DIR / filename
    headers = s.head(url).headers
    filesize = filepath.stat().st_size if filepath.exists() else 0
    if filesize == int(headers["Content-Length"]):
        # file is already updated
        return None
    r = s.get(url)
    if r.status_code != 200:
        return None

    # Save file with Pathlib
    filepath.write_bytes(r.content)
    return filepath


def update_raw_files(urls: str) -> List[Path]:
    """Update CVM raw files."""
    s = requests.Session()
    updated_filepaths = []
    for url in tqdm(urls, desc="Updating..."):
        filepath = update_raw_file(url, s)
        # print(f"    {CHECKMARK} {filename} updated.")
        if filepath:
            updated_filepaths.append(filepath)
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
    """Remove empty spaces in a pandas Series of repeated strings."""
    s_unique_original = pd.Series(s.unique())
    s_unique_adjusted = s_unique_original.replace("\s+", " ", regex=True).str.strip()
    mapping_dict = dict(zip(s_unique_original, s_unique_adjusted))
    return s.map(mapping_dict)


def process_df(df: pd.DataFrame, filepath: Path) -> pd.DataFrame:
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
        "COLUNA_DF": "es_name",  # equity_statement_name
        "ST_CONTA_FIXA": "is_acc_fixed",
        "VL_CONTA": "acc_value",
        "GRUPO_DFP": "report_group",
        "MOEDA": "Currency",
        "ESCALA_MOEDA": "currency_unit",
    }
    df = df.rename(columns=columns_translation)[columns_translation.values()]

    # Currency column has only one value (BRL) so it is not necessary.
    df = df.drop(columns=["Currency"])

    # The CVM file stores only the last report_version -> drop it.
    df = df.drop(columns=["report_version"])
    # report_version max. value is aprox. 9, so it can be uint8 (0 to 255)
    # df["report_version"] = df["report_version"].astype("uint8")

    # is_acc_fixed values are ['S', 'N']
    map_dic = {"S": True, "N": False}
    df["is_acc_fixed"] = df["is_acc_fixed"].map(map_dic).astype(bool)

    # Remove rows with acc_value == 0 and is_acc_fixed == False
    # df.query("acc_value != 0 or is_acc_fixed == True", inplace=True)
    df.query("acc_value != 0", inplace=True)

    # is_acc_fixed is being used used only non fixed accounts with acc_value == 0
    # So it can be dropped after the query above.
    df = df.drop(columns=["is_acc_fixed"])

    # cvm_id max. value is 600_000, so it can be uint32 (0 to 4_294_967_295)
    df["cvm_id"] = df["cvm_id"].astype("uint32")

    # There are two types of CVM files: DFP (ANNUAL) and ITR (QUARTERLY).
    # In database, "is_annual" is positioned after "tax_id" -> position = 3
    if filepath.name.startswith("dfp"):
        df.insert(loc=3, column="is_annual", value=True)
    else:
        df.insert(loc=3, column="is_annual", value=False)

    # For the moment, es_name will not be used since it adds to much complexity to
    # the database. It will be dropped.
    df = df.drop(columns=["es_name"])

    # Remove any extra spaces (line breaks, tabs, etc.) from columns below.
    columns = ["name_id", "acc_name"]
    df[columns] = df[columns].apply(remove_empty_spaces)

    # Replace "BCO " with "BANCO " in "name_id" column.
    df["name_id"] = df["name_id"].str.replace("BCO ", "BANCO ")

    # Convert string columns to categorical before mapping.
    columns = df.select_dtypes(include="object").columns
    df[columns] = df[columns].astype("category")

    # Convert datetime columns
    columns = ["period_reference", "period_begin", "period_end"]
    df[columns] = df[columns].apply(pd.to_datetime)

    # currency_unit values are ['MIL', 'UNIDADE']
    map_dic = {"UNIDADE": 1, "MIL": 1000}
    df["currency_unit"] = df["currency_unit"].map(map_dic).astype(int)

    # Earnings per share (EPS) values don't need to be adjusted by currency_unit.
    df["acc_value"] = np.where(
        df["acc_code"].str.startswith("3.99"),
        df["acc_value"],
        df["acc_value"] * df["currency_unit"],
    )

    # Change earnings per share codes from 3.99. to 8. to avoid confusion with
    # 3. (income_statement)
    df["acc_code"] = np.where(
        df["acc_code"].str.startswith("3.99"),
        df["acc_code"].str.replace("3.99", "8"),
        df["acc_code"],
    )
    # After the adjustment, currency_unit column is not necessary.
    df.drop(columns=["currency_unit"], inplace=True)

    """The "period_order" column is a redundant information, since it is possible to
    infer it from the "period_reference", "period_begin" and "period_end" columns.
    For example:
        if "period_reference" is 2020-12-31
           "period_begin" is 2020-01-01
           "period_end" is 2020-12-31
        then "period_order" is "LAST".

        If "period_reference" is 2020-12-31
           "period_begin" is 2019-01-01
           "period_end" is 2019-12-31
        then "period_order" is "PREVIOUS".
    Old code:
        "period_order" values are: 'ÚLTIMO', 'PENÚLTIMO'
        map_dic = {"ÚLTIMO": "LAST", "PENÚLTIMO": "PREVIOUS"}
        df["period_order"] = df["period_order"].map(map_dic)
    """
    df = df.drop(columns=["period_order"])

    """
    df['report_group'].unique() result:
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
    map_dic = {"Con": True, "Ind": False}
    df.insert(4, "is_consolidated", df["report_group"].str[3:6].map(map_dic))
    # 'report_group' data can be inferred from 'acc_code'
    df.drop(columns=["report_group"], inplace=True)

    # In "itr_cia_aberta_2022.zip", as an example, 2742 rows are duplicated.
    # Few of them have different values in "acc_value". Only one them will be kept.
    # REMOVE ALL VALUES OR MARK THESE ROWS AS ERRORS?
    cols = df.columns.tolist()
    cols.remove("acc_value")
    df.drop_duplicates(subset=cols, keep="last", inplace=True, ignore_index=True)

    return df


def process_file(raw_filepath: Path) -> Path:
    """Read, process and save a CVM file."""
    df = read_raw_file(raw_filepath)
    df = process_df(df, raw_filepath)
    processed_filepath = CVM_PROCESSED_DIR / (raw_filepath.stem + ".pickle")
    # save_processed_df(df, processed_filepath)
    df.to_pickle(processed_filepath, compression="zstd")
    return processed_filepath


def process_files_with_progress(filepaths_to_process):
    """Process CVM files with a progress bar."""
    for filepath in tqdm(filepaths_to_process, desc="Processing..."):
        # print(f"    {CHECKMARK} {raw_filepath.name} processed.")
        process_file(filepath)


def get_raw_file_mtimes() -> pd.DataFrame:
    """Return a Pandas DataFrame with file_source and file_mtime columns."""
    filepaths = sorted(CVM_RAW_DIR.glob("*.zip"))
    d_mtimes = {filepath.name: filepath.stat().st_mtime for filepath in filepaths}
    return pd.DataFrame(d_mtimes.items(), columns=["file_source", "file_mtime"])


def read_all_processed_files() -> pd.DataFrame:
    """Read all processed CVM files."""
    # list filepaths in processed folder
    filepaths = sorted(CVM_PROCESSED_DIR.glob("*.pickle"))
    df = pd.concat([pd.read_pickle(f, compression="zstd") for f in filepaths])
    columns = df.columns
    cat_cols = [c for c in columns if df[c].dtype in ["object"]]
    df[cat_cols] = df[cat_cols].astype("category")
    return df


def drop_not_last_entries(df: pd.DataFrame) -> pd.DataFrame:
    """Drop duplicated accounting entries before building database

    Because the report holds accounting entries for the year before, we can keep only
    the most recent one in the database. By doing this, we guarantee
    that there is only one valid accounting value in the database -> the last one
    """
    sort_cols = [
        "cvm_id",
        "is_consolidated",
        "acc_code",
        "period_reference",
        "is_annual",
        "period_begin",
        "period_end",
    ]
    df.sort_values(by=sort_cols, ascending=True, inplace=True, ignore_index=True)

    subset_cols = [
        "cvm_id",
        "is_consolidated",
        "acc_code",
        "period_begin",
        "period_end",
    ]
    return df.drop_duplicates(subset=subset_cols, keep="last", ignore_index=True)


def drop_unecessary_quarterly_entries(df: pd.DataFrame) -> pd.DataFrame:
    """Keep the last QUARTERLY report for each company only when necessary."""
    # Create a temporary column with the max. period_reference for each company
    df["max_period"] = df.groupby("cvm_id")["period_reference"].transform("max")

    mask1 = ~df["is_annual"]
    mask2 = df["period_reference"] < df["max_period"]
    mask = ~(mask1 & mask2)
    return df[mask].reset_index(drop=True).drop(columns=["max_period"])


def build_main_df():
    """Build FinLogic Database from processed CVM files."""
    df = read_all_processed_files()
    df = drop_not_last_entries(df)
    df = drop_unecessary_quarterly_entries(df)
    # After the drop_unecessary entries, period_reference is not necessary anymore
    df.drop(columns=["period_reference"], inplace=True)
    df.to_pickle(cfg.DF_PATH, compression="zstd")
