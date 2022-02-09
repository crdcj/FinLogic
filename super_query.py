# Gerar a base inteira, menos o ano corrente
import concurrent.futures
import os
import pandas as pd
PATH_DATASET = './data/processed/pickle/'


def query_pickle(file_name:str):
    df = pd.read_pickle(f'{PATH_DATASET}{file_name}', compression='zstd')
    df.query("CD_CVM == 9512", inplace=True)
    # print(len(df.index))
    return df


def query_mp(files_names) -> pd.DataFrame:
    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = executor.map(query_pickle, files_names)

    lista_dfs = []
    [lista_dfs.append(r) for r in results]

    # print('concatenar dataframes...')
    df = pd.DataFrame()
    df = pd.concat(lista_dfs, ignore_index=True)
    # print(len(df.index))
    #df.drop_duplicates(inplace=True)
    #cols_filtro = ['datneg', 'codneg', 'prazot']
    # df.sort_values(cols_filtro, inplace=True, ignore_index=True)

    return df


# files_names = sorted(os.listdir(PATH_DATASET))
# df = main(files_names)
