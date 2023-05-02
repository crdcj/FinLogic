from concurrent.futures import ThreadPoolExecutor
from typing import List
import zipfile as zf
from pathlib import Path
import pandas as pd
import requests
from . import config as cf

URL_DFP = "http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/"
URL_ITR = "http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/"

CVM_DF_PATH = cf.DATA_PATH / "cvm_df.pkl"


def get_cvm_df() -> pd.DataFrame:
    """Get CVM files metadata."""
    if CVM_DF_PATH.is_file():
        cvm_df = pd.read_pickle(CVM_DF_PATH)
    else:
        columns = ["filename", "file_size", "etag", "last_modified"]
        cvm_df = pd.DataFrame(columns=columns)
    return cvm_df


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


def update_cvm_file(url: str) -> Path:
    """Update raw file from CVM portal. Return a Path if file is updated."""
    cvm_filepath = Path(cf.CVM_DIR, url[-23:])  # filename = url final
    with requests.Session() as s:
        r = s.get(url, stream=True)
        if r.status_code != requests.codes.ok:
            return None

    if Path.exists(cvm_filepath):
        local_file_size = cvm_filepath.stat().st_size
    else:
        local_file_size = 0
    url_file_size = int(r.headers["Content-Length"])
    if local_file_size == url_file_size:
        # File is already updated
        return None
    with cvm_filepath.open(mode="wb") as f:
        f.write(r.content)

    # headers["Last-Modified"] -> 'Wed, 23 Jun 2021 12:19:24 GMT'
    ts_server = pd.to_datetime(
        r.headers["Last-Modified"], format="%a, %d %b %Y %H:%M:%S %Z"
    )
    # Store URL files metadata in a DataFrame
    cvm_df = get_cvm_df()
    cvm_df.loc[pd.Timestamp.now()] = [
        cvm_filepath.name,
        r.headers["Content-Length"],
        r.headers["ETag"],
        ts_server,
    ]
    cvm_df.to_pickle(CVM_DF_PATH)
    return cvm_filepath.name


def update_cvm_files(urls: str) -> List[Path]:
    """Update local CVM raw files asynchronously."""
    with ThreadPoolExecutor() as executor:
        results = executor.map(update_cvm_file, urls)
    updated_filenames = [r for r in results if r is not None]
    return updated_filenames


def read_cvm_file(cvm_filename: str) -> pd.DataFrame:
    """Read annual file, process it, save the result and return the file path."""
    df = pd.DataFrame()
    cvm_filepath = Path(cf.CVM_DIR, cvm_filename)
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
