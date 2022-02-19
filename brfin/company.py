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
        self._get_ac_levels()
        self._get_assets()
        self._get_liabilities_and_equity()
        self._get_equity()

    def remove_category(self):
        self._df = self._df.astype({
            'CD_CVM': 'object',
            'is_annual': bool,
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

    def _get_assets(self):
        df = self._df.query("ac_l1 == 1").copy()
        # df.query("CD_CONTA == '1'", inplace=True)
        self.assets = self._make_bs(df)

    def _get_liabilities_and_equity(self):
        df = self._df.query("ac_l1 == 2").copy()
        # df.query("CD_CONTA == '1'", inplace=True)
        self.liabilities_and_equity = self._make_bs(df)

    def _get_equity(self):
        df = self._df.query("ac_l1 == 2 and ac_l2 == 3").copy()
        # df.query("CD_CONTA == '1'", inplace=True)
        self.equity = self._make_bs(df)

    def _make_bs(self, df: pd.DataFrame) -> pd.DataFrame:

        # quarterly financial statement = qfc
        # keep only last qfc
        last_qfs_period = df.query("is_annual == False").DT_FIM_EXERC.max()  # noqa
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

        base_columns = ['DS_CONTA', 'CD_CONTA', 'ST_CONTA_FIXA']
        df_bs = df.loc[:, base_columns]
        df_bs.drop_duplicates(ignore_index=True, inplace=True)

        merge_columns = base_columns + ['VL_CONTA']
        for period in df.DT_FIM_EXERC.unique():
            # print(date)
            df_year = df.query("DT_FIM_EXERC == @period").copy()
            df_year = df_year[merge_columns]
            df_year.rename(
                columns={'VL_CONTA': np.datetime_as_string(period, unit='D')},
                inplace=True)
            df_bs = pd.merge(df_bs, df_year, how='left')

        df_bs.sort_values('CD_CONTA', ignore_index=True, inplace=True)
        return df_bs

    def _get_ac_levels(self):
        """
        Get accounting code (ac) levels in CD_CONTA column
        The first part of CD_CONTA is the financial statement type
        df['CD_CONTA'].str[0].unique() -> [1, 2, 3, 4, 5, 6, 7]
        Table of correspondences:
            1 -> Balanço Patrimonial Ativo
            2 -> Balanço Patrimonial Passivo
            3 -> Demonstração do Resultado
            4 -> Demonstração de Resultado Abrangente
            5 -> Demonstração das Mutações do Patrimônio Líquido
            6 -> Demonstração do Fluxo de Caixa (Método Indireto)
            7 -> Demonstração de Valor Adicionado
        """
        self._df['ac_l1'] = pd.to_numeric(
            self._df['CD_CONTA'].str[0], downcast='integer')
        self._df['ac_l2'] = pd.to_numeric(
            self._df['CD_CONTA'].str[2:4], downcast='integer')
