"""Module containing functions for the DATASET generation, save and update."""
import os
import zipfile as zf
from concurrent.futures import ProcessPoolExecutor
import requests
import pandas as pd
import numpy as np

URL_DFP = 'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/'
URL_ITR = 'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/'
script_dir = os.path.dirname(__file__)
RAW_DIR = script_dir + '/data/raw/'
PROCESSED_DIR = script_dir + '/data/processed/'
DATASET_PATH = PROCESSED_DIR + 'dataset.pkl.zst'


def update_raw_file(url: str) -> bool:
    """Update file from CVM portal. Return True if file is updated."""
    file_name = url[-23:]  # nome do arquivo = final da url
    cam_arq = RAW_DIR + file_name
    with requests.Session() as s:
        r = s.get(url, stream=True)
        if r.status_code != requests.codes.ok:
            # print(f'{file_name} not found -> continue', flush=True)
            return False
        tam_arq_arm = 0
        if os.path.isfile(cam_arq):
            tam_arq_arm = os.path.getsize(cam_arq)
        tam_arq_url = int(r.headers['Content-Length'])
        if(tam_arq_arm == tam_arq_url):
            # print(f'{file_name} already updated -> continue', flush=True)
            return False
        print(f'{file_name} new/outdated -> download file', flush=True)
        with open(cam_arq, 'wb') as f:
            f.write(r.content)
        return True


def list_urls() -> list:
    """Atualizar a base de arquivos do Portal da CVM.

    Urls com os links para as tabelas de dados:
    http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/
    http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/
    Exemplos de links:
    http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/dfp_cia_aberta_2020.zip
    http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/itr_cia_aberta_2020.zip
    Ao longo de 2021, já existem DFs do ano 2022 por conta do calendário social
    da empresa poder não coincidir, necessariamente, com o calendário oficial.
    Por conta disso, soma-se 2 ano ano atual (o segundo limite da função range
    é exlusivo)
    """
    first_year = 2010  # First year avaible at CVM Portal.
    # Next year files will appear during current year.
    last_year = pd.Timestamp.now().year + 1
    years = list(range(first_year, last_year + 1))
    first_year_itr = last_year - 3
    urls = []
    for year in years:
        filename = f'dfp_cia_aberta_{year}.zip'
        url = f'{URL_DFP}{filename}'
        urls.append(url)
        if year >= first_year_itr:
            filename = f'itr_cia_aberta_{year}.zip'
            url = f'{URL_ITR}{filename}'
            urls.append(url)

    return urls


def update_raw_dataset():
    """Update raw files according by comparing to CVM regulator site."""
    urls = list_urls()
    with ProcessPoolExecutor() as executor:
        results = executor.map(update_raw_file, urls)

    was_updated = []
    [was_updated.append(r) for r in results]
    updated_urls = []
    for t in zip(was_updated, urls):
        if t[0]:
            updated_urls.append(t[1])
    return updated_urls


def clean_raw_df(df: pd.DataFrame) -> pd.DataFrame:
    """Convert raw dataframe into processed dataframe."""

    columns_translation = {
        'CD_CVM': 'corp_cvm_id',
        'CNPJ_CIA': 'corp_fiscal_id',
        'DENOM_CIA': 'corp_name',
        'VERSAO': 'report_version',
        'DT_INI_EXERC': 'period_begin',
        'DT_FIM_EXERC': 'period_end',
        'DT_REFER': 'period_reference',
        'ORDEM_EXERC': 'period_order',
        'CD_CONTA': 'account_code',
        'DS_CONTA': 'account_name',
        'ST_CONTA_FIXA': 'account_fixed',
        'VL_CONTA': 'account_value',
        'COLUNA_DF': 'equity_statement_column',
        'MOEDA': 'currency',
        'ESCALA_MOEDA': 'currency_unit',
    }
    df.rename(columns=columns_translation, inplace=True)
    # df['report_version'].unique()
    # ['3', '2', '4', '1', '7', '5', '6', '9', '8']
    df['report_version'] = df['report_version'].astype(np.int8)
    df['corp_cvm_id'] = df['corp_cvm_id'].astype(np.int32)  # max < 600_000
    df['account_value'] = df['account_value'].astype(float)

    # df.query("account_value == 0") -> 10.891.139 rows from 17.674.199
    # Zero values will not be used.
    df.query("account_value != 0", inplace=True)

    # df['currency'].value_counts() -> REAL    43391302
    df.drop(columns=['currency'], inplace=True)

    # df['currency_unit'].value_counts()
    #   MIL        40483230
    #   UNIDADE     2908072
    df['currency_unit'] = df['currency_unit'].map({'MIL': 1000, 'UNIDADE': 1})

    # Unit base currency.
    df['account_codes_level'] = df['account_code'].str[0:4]
    # Do not adjust earnings per share rows (account codes 3.99...)
    df['account_value'] = np.where(
        df.account_codes_level == '3.99',
        df['account_value'],
        df['account_value'] * df['currency_unit'])
    df.drop(columns=['currency_unit', 'account_codes_level'], inplace=True)

    # df['account_fixed'].unique() -> ['S', 'N']
    df['account_fixed'] = df['account_fixed'].map({'S': True, 'N': False})

    # df['period_order'].unique() -> ['PENÚLTIMO', 'ÚLTIMO']
    df['period_order'] = df['period_order'].map({'ÚLTIMO': 0, 'PENÚLTIMO': -1})
    df['period_order'] = df['period_order'].astype(np.int8)

    df['period_reference'] = pd.to_datetime(df['period_reference'])
    df['period_end'] = pd.to_datetime(df['period_end'])
    # BPA, BPP and DFC files have no period_begin column.
    if 'period_begin' in df.columns:
        df['period_begin'] = pd.to_datetime(df['period_begin'])
    else:
        # column_order.remove('period_begin')
        df['period_begin'] = pd.NaT
    if 'equity_statement_column' not in df.columns:
        df['equity_statement_column'] = np.nan

    """
    accounting_method -> Financial Statemen Type
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
    df['accounting_method'] = df['GRUPO_DFP'].str[3:6].map({
        'Con': 'consolidated',
        'Ind': 'separate'})
    df['accounting_method'] = df['accounting_method'].astype('category')
    # 'GRUPO_DFP' data can be inferred from 'accounting_method' and report_type
    df.drop(columns=['GRUPO_DFP'], inplace=True)

    columns_order = [
        'corp_name',
        'corp_cvm_id',
        'corp_fiscal_id',        
        'report_type',
        'report_version',
        'period_reference',
        'period_begin',
        'period_end',
        'period_order',
        'account_code',
        'account_name',
        'accounting_method',
        'account_fixed',
        'account_value',
        'equity_statement_column',
    ]
    df = df[columns_order]

    return df


def process_raw_file(parent_filename):
    """Read yearly raw files, process it and consolidate into one dataframe."""
    df = pd.DataFrame()
    parent_path = RAW_DIR + parent_filename
    # print(parent_path, flush=True)
    parent_file = zf.ZipFile(parent_path)
    child_filenames = parent_file.namelist()
    for child_filename in child_filenames[1:]:
        # print(child_parent_file_name)
        child_file = parent_file.open(child_filename)
        df_child = pd.read_csv(
            child_file, sep=';', encoding='iso-8859-1', dtype=str)
        # There are two types of CVM files: DFP(annual) and ITR(quarterly).
        if parent_filename[0:3] == 'dfp':
            df_child['report_type'] = 'annual'
        else:
            df_child['report_type'] = 'quarterly'

        df_child = clean_raw_df(df_child)
        df = pd.concat([df, df_child], ignore_index=True)
    print(parent_filename, 'processed')
    return df


def update_processed_dataset():
    """Update the processed dataset."""
    # print(os.path.dirname(os.path.abspath(__file__)))
    filenames = sorted(os.listdir(RAW_DIR))
    with ProcessPoolExecutor() as executor:
        results = executor.map(process_raw_file, filenames)

    lista_dfs = []
    [lista_dfs.append(df) for df in results]
    print('')
    print('Concatenating dataframes ...')
    df = pd.concat(lista_dfs, ignore_index=True)

    # Correct/harmonize some account texts.
    df.replace(to_replace=['\xa0ON\xa0', 'On'], value='ON', inplace=True)

    sort_by = [
        'corp_cvm_id',
        'period_reference',
        'report_version',
        'period_order',
        'accounting_method',
        'account_code',
    ]
    df.sort_values(by=sort_by, ignore_index=True, inplace=True)

    df = df.astype('category')
    print('Columns data type changed to category')

    df.to_pickle(DATASET_PATH)


def update_dataset():
    # create dataset folders in case they do not exist
    if not os.path.isdir(RAW_DIR):
        os.makedirs(RAW_DIR)
    if not os.path.isdir(PROCESSED_DIR):
        os.makedirs(PROCESSED_DIR)

    urls = list_urls()
    print('')
    print('Downloading CVM raw files...')
    urls = update_raw_dataset()
    for url in urls:
        print(url)
    print('')
    print('Processing CVM raw dataset...')
    update_processed_dataset()
    print('')
    print('Processed dataset saved')


def search_in_dataset(expression: str) -> pd.DataFrame:
    """Search dataset for corp. names that contains the 'expression'"""
    expression = expression.upper()
    df = pd.read_pickle(DATASET_PATH)
    mask = df['corp_name'].str.contains(expression)
    df = df[mask].copy()
    df.sort_values(by='corp_name', inplace=True)
    df.drop_duplicates(subset='corp_cvm_id', inplace=True, ignore_index=True)
    columns = ['corp_name', 'corp_cvm_id', 'corp_fiscal_id']
    return df[columns]

def dataset_info() -> pd.DataFrame:
    """Return dataframe with dataset info"""
    df = pd.read_pickle(DATASET_PATH)
    columns_duplicates = [
        'corp_cvm_id', 'report_version', 'report_type', 'period_reference']
    fs_periods = df['period_end'].astype('datetime64')
    dataset_info = {
        'Number of account values (total rows)': len(df.index),
        'Number of unique account codes': df[
            'account_code'].nunique(),
        'Number of corporations': df['corp_cvm_id'].nunique(),
        'Number of Financial Statements':  len(
            df.drop_duplicates(subset=columns_duplicates).index),
        'First Financial Statement': fs_periods.min().strftime('%Y-%m-%d'),
        'Last Financial Statement': fs_periods.max().strftime('%Y-%m-%d')
    }
    df_info = pd.DataFrame.from_dict(
        dataset_info, orient='index', columns=['Dataset Info'])
    return df_info

