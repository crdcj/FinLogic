import os
import zipfile as zf
import requests
import pandas as pd
URL_CVM = 'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/'


def download_files(url: str) -> bool:
    """Baixar arquivo do portal. Retorna True se o arquivo foi baixado"""
    nome_arq = url[-23:]  # nome do arquivo = final da url
    cam_arq = 'data/' + nome_arq
    with requests.Session() as s:
        r = s.get(url, stream=True)
        if r.status_code != requests.codes.ok:
            print(f'{nome_arq} não encontrado no servidor')
            return False
        tam_arq_arm = 0
        if os.path.isfile(cam_arq):
            tam_arq_arm = os.path.getsize(cam_arq)
        tam_arq_url = int(r.headers['Content-Length'])
        if(tam_arq_arm == tam_arq_url):
            print(f'{nome_arq} está atualizado -> não baixar')
            return False
        print(f'{nome_arq} está desatualizado -> baixar')
        with open(cam_arq, 'wb') as f:
            f.write(r.content)
        return True


def update_files() -> int:
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
    n_arqs_baixados = 0
    ano_i = 2010  # Primeiro ano da base da CVM
    ano_f = pd.Timestamp.now().year + 2
    lista_anos = [ano for ano in range(ano_i, ano_f)]
    for ano in lista_anos:
        url_dfp = f'{URL_CVM}DFP/DADOS/dfp_cia_aberta_{ano}.zip'
        url_itr = f'{URL_CVM}ITR/DADOS/itr_cia_aberta_{ano}.zip'
        if download_files(url_dfp):
            n_arqs_baixados += 1
        if download_files(url_itr):
            n_arqs_baixados += 1

    return n_arqs_baixados


def load_metadata() -> pd.DataFrame:
    """retorna um dataframe com os metadados da base PORTAL"""
    df = pd.DataFrame()
    nomes_arqs = sorted(os.listdir('data/'))
    kwargs = {'sep': ';', 'encoding': 'iso-8859-1', 'dtype': str}
    for n, nome_arq in enumerate(nomes_arqs):
        cam_arquivo = 'data/' + nome_arq
        arquivo = zf.ZipFile(cam_arquivo)
        # print(f'{n}: {nom_arquivo}, ')
        nome_arq_md = nome_arq[0:-3] + 'csv'
        df_arquivo = pd.read_csv(arquivo.open(nome_arq_md), **kwargs)
        df = pd.concat([df, df_arquivo])

    cols_int = ['VERSAO', 'CD_CVM', 'ID_DOC']
    df[cols_int] = df[cols_int].astype(int)
    df.drop(columns=['CNPJ_CIA', 'DENOM_CIA', 'LINK_DOC'], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def listar_docs() -> set:
    df = load_metadata()
    return set(df['ID_DOC'].sort_values())