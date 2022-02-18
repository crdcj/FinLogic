import os
import zipfile as zf
from concurrent.futures import ProcessPoolExecutor
import requests
import pandas as pd
import numpy as np

URL_DFP = 'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/'
URL_ITR = 'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/'
PATH_RAW = './data/raw/'
PATH_PROCESSED = './data/processed/'
READ_OPTIONS = {
    'sep': ';',
    'encoding': 'iso-8859-1',
    'dtype': str,
}


def update_raw_file(url: str) -> bool:
    """Update file from CVM portal. Return True if file is updated"""
    file_name = url[-23:]  # nome do arquivo = final da url
    cam_arq = PATH_RAW + file_name
    with requests.Session() as s:
        r = s.get(url, stream=True)
        if r.status_code != requests.codes.ok:
            print(f'{file_name} not found in CVM server -> continue')
            return False
        tam_arq_arm = 0
        if os.path.isfile(cam_arq):
            tam_arq_arm = os.path.getsize(cam_arq)
        tam_arq_url = int(r.headers['Content-Length'])
        if(tam_arq_arm == tam_arq_url):
            print(f'{file_name} already updated -> continue')
            return False
        print(f'{file_name} outdated -> download file')
        with open(cam_arq, 'wb') as f:
            f.write(r.content)
        return True


def list_urls() -> list:
    """
    Atualizar a base de arquivos do Portal da CVM
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
    # first year avaible at CVM Portal
    first_year = 2010
    # Next year files will appear during current year
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


def clean_raw_df(df) -> pd.DataFrame:
    "converts raw dataframe into processed dataframe"
    # df.VERSAO.unique() -> ['3', '2', '4', '1', '7', '5', '6', '9', '8']
    df.VERSAO = df.VERSAO.astype(np.int8)
    df.CD_CVM = df.CD_CVM.astype(np.int32)  # max < 600_000
    df.VL_CONTA = df.VL_CONTA.astype(float)

    # df.query("VL_CONTA == 0") -> 10.891.139 rows from 17.674.199
    # Zero values will not be used
    df.query("VL_CONTA != 0", inplace=True)

    # df.MOEDA.value_counts()
    # REAL    43391302
    df.drop(columns=['MOEDA'], inplace=True)

    # df.ESCALA_MOEDA.value_counts()
    # MIL        40483230
    # UNIDADE     2908072
    df.ESCALA_MOEDA = df.ESCALA_MOEDA.map({'MIL': 1000, 'UNIDADE': 1})

    # unit base currency
    df.VL_CONTA = df.VL_CONTA * df.ESCALA_MOEDA
    df.drop(columns=['ESCALA_MOEDA'], inplace=True)

    # df.ST_CONTA_FIXA.unique() -> ['S', 'N']
    df.ST_CONTA_FIXA = df.ST_CONTA_FIXA.map({'S': True, 'N': False})

    # df.ORDEM_EXERC.unique() -> ['PENÚLTIMO', 'ÚLTIMO']
    df.ORDEM_EXERC = df.ORDEM_EXERC.map({'ÚLTIMO': 0, 'PENÚLTIMO': -1})
    df.ORDEM_EXERC = df.ORDEM_EXERC.astype(np.int8)

    df.DT_REFER = pd.to_datetime(df.DT_REFER)
    df.DT_FIM_EXERC = pd.to_datetime(df.DT_FIM_EXERC)
    # BPA, BPP and DFC files have no DT_INI_EXERC column
    if 'DT_INI_EXERC' in df.columns:
        df.DT_INI_EXERC = pd.to_datetime(df.DT_INI_EXERC)
    else:
        # column_order.remove('DT_INI_EXERC')
        df['DT_INI_EXERC'] = pd.NaT
    if 'COLUNA_DF' not in df.columns:
        df['COLUNA_DF'] = np.nan

    """
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
    Hence, with string position 3 we can make:
    if == 'C' -> consolidated statement is True
    if == 'I' -> consolidated statement is False (stand alone statement)
    """
    df['is_consolidated'] = df['GRUPO_DFP'].str[3].map({'C': True, 'I': False})
    df['is_consolidated'] = df['is_consolidated'].astype(bool)
    # information in 'GRUPO_DFP' is already in 'is_consolidated' or in fs_type
    df.drop(columns=['GRUPO_DFP'], inplace=True)

    columns_order = [
        'CD_CVM', 'CNPJ_CIA', 'DENOM_CIA', 'is_annual', 'is_consolidated',
        'DT_REFER', 'VERSAO', 'DT_INI_EXERC', 'DT_FIM_EXERC', 'ORDEM_EXERC',
        'CD_CONTA', 'DS_CONTA', 'ST_CONTA_FIXA', 'COLUNA_DF', 'VL_CONTA'
    ]
    df = df[columns_order]

    return df


def process_raw_file(parent_filename):
    df = pd.DataFrame()
    parent_path = PATH_RAW + parent_filename
    # print(parent_path, flush=True)
    parent_file = zf.ZipFile(parent_path)
    child_filenames = parent_file.namelist()
    for child_filename in child_filenames[1:]:
        # print(child_parent_file_name)
        child_file = parent_file.open(child_filename)
        df_child = pd.read_csv(child_file, **READ_OPTIONS)
        # there are two types of CVM files: DFP(annual) and ITR(quarterly)
        if parent_filename[0:3] == 'dfp':
            df_child['is_annual'] = True
        else:
            df_child['is_annual'] = False

        df_child = clean_raw_df(df_child)
        df = pd.concat([df, df_child], ignore_index=True)
    print(parent_path)
    return df


def update_processed_dataset():
    filenames = sorted(os.listdir('./data/raw/'))
    with ProcessPoolExecutor() as executor:
        results = executor.map(process_raw_file, filenames)

    lista_dfs = []
    [lista_dfs.append(df) for df in results]

    # print('concatenar dataframes...')
    df = pd.concat(lista_dfs, ignore_index=True)

    sort_by = [
        'CD_CVM', 'DT_REFER', 'VERSAO', 'ORDEM_EXERC', 'is_consolidated',
        'CD_CONTA']
    df.sort_values(by=sort_by, ignore_index=True, inplace=True)
    print('Dataset sorted')

    # Convert all columns, except bool and float64, to category
    # for column, c_type in zip(df.columns, df.dtypes):
    #     if (c_type != 'float64') and (c_type != 'bool'): #(c_type != 'int8'):
    #         # print(column, c_type)
    #         df[f'{column}'] = df[f'{column}'].astype('category')
    df = df.astype('category')
    print('Columns of type int, str and datetime changed to category')

    file_path = PATH_PROCESSED + 'dataset.pkl.zst'
    df.to_pickle(file_path)
    print('Dataset saved')
