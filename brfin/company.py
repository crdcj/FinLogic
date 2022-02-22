"""Module containing BrFin Company Class Definition."""
import numpy as np
import pandas as pd

DATASET = pd.read_pickle(
    '/home/crcj/GitHub/BrFin/data/processed/dataset.pkl.zst'
)


class Company():
    """Financial Statement Class for Brazilian Companies."""

    def __init__(
        self,
        cvm_number: int,
        start_period: str = '2009-12-31',
        fs_type: str = 'consolidated',
    ):
        """Initialize main variables.

        Args:
            cvm_number (int): CVM unique number of the company.
            fs_type (str, optional): Financial Statement agregation type
                (consolidated or separate).
        """
        self.cvm_number = cvm_number
        self.fs_type = fs_type
        self.start_period = start_period
        self._set_df_main()
        self._set_ac_levels()

    @property
    def cvm_number(self):
        """Return cvm_number if number exists in DATASET."""
        return self._cvm_number

    @cvm_number.setter
    def cvm_number(self, value):
        companies_list = list(DATASET['CD_CVM'].unique())
        if value in companies_list:
            self._cvm_number = value
        else:
            print('cvm_number not found')
            self._cvm_number = None

    @property
    def fs_type(self):
        """Return selected financial statement type (fs_type).

        Options are: 'consolidated' or 'separate'
        """
        return self._fs_type

    @fs_type.setter
    def fs_type(self, value):
        if value in ('consolidated', 'separate'):
            self._fs_type = value
        else:
            print("""Financial Statement Type (fs_type) not valid -> 'consolidated' selected.
Valid options are: 'consolidated' or 'separate'""")  # noqa
            self._fs_type = 'consolidated'

    @property
    def start_period(self):
        """Return selected start period for Dataset screening."""
        return self._start_period

    @start_period.setter
    def start_period(self, value):
        date = pd.to_datetime(value, errors='coerce')
        if date == pd.NaT:
            print('Start Period not valid. Must be in YYYY-MM-DD format')
            print('Start Period 2009-12-31 selected')
            self._start_period = pd.to_datetime('2009-12-31')
        else:
            print(f"Selected Start Period = {date}")
            self._start_period = date

    def _set_df_main(self) -> pd.DataFrame:
        self._df_main = DATASET.query("CD_CVM == @self._cvm_number").copy()
        self._df_main = self._df_main.astype({
            'CD_CVM': 'object',
            'is_annual': bool,
            'fs_type': str,
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
        self._df_main.query("fs_type == @self.fs_type", inplace=True)
        self._df_main.query("DT_FIM_EXERC >= @self.start_period", inplace=True)

    @property
    def assets(self) -> pd.DataFrame:
        """Return company assets."""
        df = self._df_main.query("ac_l1 == 1").copy()
        return self._make_bs(df)

    @property
    def liabilities_and_equity(self) -> pd.DataFrame:
        """Return company liabilities_and_equity."""
        df = self._df_main.query("ac_l1 == 2").copy()
        return self._make_bs(df)

    @property
    def equity(self) -> pd.DataFrame:
        """Return company equity."""
        df = self._df_main.query("ac_l1 == 2 and ac_l2 == 3").copy()
        # df.query("CD_CONTA == '1'", inplace=True)
        return self._make_bs(df)

    def _make_bs(self, df: pd.DataFrame) -> pd.DataFrame:

        # quarterly financial statement = qfc
        # keep only last qfc
        last_qfs_period = df.query("is_annual == False").DT_FIM_EXERC.max()  # noqa
        df.query(
            "(is_annual == True) or (DT_FIM_EXERC == @last_qfs_period)",
            inplace=True
        )
        # sort for drop operation
        df.sort_values(['DT_FIM_EXERC', 'DT_REFER', 'CD_CONTA'], inplace=True)

        # only the last published statements will be used
        df['financial_year'] = df.DT_FIM_EXERC.dt.year
        df.drop_duplicates(
            subset=['financial_year', 'CD_CONTA'],
            keep='last',
            inplace=True,
            ignore_index=True
        )

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

    def _set_ac_levels(self):
        """
        Get accounting code (ac) levels 1 and 2 in CD_CONTA column.

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
        self._df_main['ac_l1'] = pd.to_numeric(
            self._df_main['CD_CONTA'].str[0], downcast='integer')
        self._df_main['ac_l2'] = pd.to_numeric(
            self._df_main['CD_CONTA'].str[2:4], downcast='integer')
