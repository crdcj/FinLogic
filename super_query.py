# Gerar a base inteira, menos o ano corrente
import os
import zipfile as zf
from concurrent.futures import ProcessPoolExecutor
import pandas as pd
import cvm

PATH_RAW = './data/processed/raw/'
READ_OPTIONS = {
    'sep': ';',
    'encoding': 'iso-8859-1',
    'dtype': str,
}



def query_pickle(f_and_c):
    filename, codcvm = f_and_c
    df = pd.read_pickle(f'{PATH_DATASET}{filename}', compression='zstd')
    df.query("CD_CVM == @codcvm", inplace=True)
    # print(len(df.index))
    return df


def query_mp(codcvm) -> pd.DataFrame:
    filenames = sorted(os.listdir('./data/processed/pickle/'))
    # f_and_c -> list with filename + company code
    f_and_c = [[filename, codcvm] for filename in filenames]
    # code_list = [codcvm] * len(filenames)
    with ProcessPoolExecutor() as executor:
        results = executor.map(query_pickle, f_and_c)

    lista_dfs = []
    [lista_dfs.append(r) for r in results]
    # print('concatenar dataframes...')
    df = pd.DataFrame()
    df = pd.concat(lista_dfs, ignore_index=True)
    return df
