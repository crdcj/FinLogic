import pandas as pd
import numpy as np

file_path = './data/processed/dataset.pkl.zst'
DATASET = pd.read_pickle(file_path)


class Company():

    def __init__(self, cvm_number: int, consolidated=True):
        self.cvm_number = int(cvm_number)
        self.consolidated = consolidated
        self._df = DATASET.query(
            "(CD_CVM == @self.cvm_number) and \
             (is_consolidated == @self.consolidated)"
        ).copy()
        self.remove_category()

    def remove_category(self):
        self._df = self._df.astype({
            'CD_CVM': 'object',
            'is_annual': bool,
            'fs_type': np.int8,
            'is_consolidated': bool,
            'DT_REFER': 'datetime64',
            'DT_INI_EXERC': 'datetime64',
            'DT_FIM_EXERC': 'datetime64',
            'ORDEM_EXERC': np.int8,
            'CD_CONTA': 'object',
            'DS_CONTA': 'object',
            'ST_CONTA_FIXA': bool,
            'COLUNA_DF': 'object',
            'VL_CONTA': float
        })
        return


    def assets(self) -> pd.DataFrame:
        df = self._df.query("fs_type == 1").copy()
        # df.query("CD_CONTA == '1'", inplace=True)

        # quarterly financial statement = qfc
        # keep only last qfc
        last_qfs_period = df.query("is_annual == False").DT_FIM_EXERC.max()
        df.query(
            "(is_annual == True) or (DT_FIM_EXERC == @last_qfs_period)",
            inplace=True)
        # sort for drop operation
        df.sort_values(['DT_FIM_EXERC', 'DT_REFER', 'CD_CONTA'], inplace=True)

        # only the last published statements will be used
        df['financial_year'] = df.DT_FIM_EXERC.dt.year
        df.drop_duplicates(
            subset=['financial_year', 'CD_CONTA'],
            keep='last',
            inplace=True,
            ignore_index=True)

        columns_drop = [
            'CNPJ_CIA', 'DENOM_CIA', 'VERSAO', 'ORDEM_EXERC', 'financial_year',
            'CD_CVM']
        df.drop(columns=columns_drop, inplace=True)

        df_initial_columns = [
            'DS_CONTA', 'CD_CONTA', 'ST_CONTA_FIXA', 'COLUNA_DF'
        ]
        df_assets = df.loc[:, df_initial_columns]
        df_assets.drop_duplicates(ignore_index=True, inplace=True)

        merge_columns = [
            'CD_CONTA', 'DS_CONTA', 'ST_CONTA_FIXA', 'COLUNA_DF', 'VL_CONTA']
        for period in df.DT_FIM_EXERC.unique():
            # print(date)
            df_year = df.query("DT_FIM_EXERC == @period").copy()
            df_year = df_year[merge_columns]
            df_year.rename(
                columns={'VL_CONTA': np.datetime_as_string(period, unit='D')},
                inplace=True)    
            df_assets = pd.merge(df_assets, df_year, how='left')

        # df.reset_index(drop=True, inplace=True)
        return df_assets
