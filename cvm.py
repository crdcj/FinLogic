import os
import zipfile as zf
import requests
import pandas as pd

URL_DFP = 'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/'
URL_ITR = 'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/'
PATH_RAW = 'data/raw/'


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


def update_raw_dataset() -> list:
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
    files_updated = []
    first_year = 2010  # first year avaible at CVM Portal
    last_year = pd.Timestamp.now().year + 1
    years = [year for year in range(first_year, last_year + 1)]
    # DFPs update
    for year in years:
        file_name = f'dfp_cia_aberta_{year}.zip'
        url_dfp = f'{URL_DFP}{file_name}'
        is_updated = update_raw_file(url_dfp)
        files_updated.append(file_name) if is_updated else None
    # ITRs update -> only the last 3 years will be used for ltm calculations
    for year in years[-3:]:
        file_name = f'itr_cia_aberta_{year}.zip'
        is_updated = update_raw_file(f'{URL_ITR}{file_name}')
        files_updated.append(file_name) if is_updated else None
    return files_updated


def load_metadata() -> pd.DataFrame:
    """retorna um dataframe com os metadados da base PORTAL"""
    df = pd.DataFrame()
    files_names = sorted(os.listdir(PATH_RAW))
    kwargs = {'sep': ';', 'encoding': 'iso-8859-1', 'dtype': str}
    for n, file_name in enumerate(files_names):
        cam_arquivo = 'data/raw/' + file_name
        arquivo = zf.ZipFile(cam_arquivo)
        # print(f'{n}: {nom_arquivo}, ')
        file_name_md = file_name[0:-3] + 'csv'
        df_arquivo = pd.read_csv(arquivo.open(file_name_md), **kwargs)
        df = pd.concat([df, df_arquivo])

    cols_int = ['VERSAO', 'CD_CVM', 'ID_DOC']
    df[cols_int] = df[cols_int].astype(int)
    df.drop(columns=['CNPJ_CIA', 'DENOM_CIA', 'LINK_DOC'], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df
