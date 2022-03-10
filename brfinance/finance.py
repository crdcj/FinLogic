"""Module containing Financial Class definition for company financials.
Abbreviation used for Financial Statement = FS
"""
import os
import numpy as np
import pandas as pd


class Finance():
    """Company Financials Class for Brazilian Companies."""

    script_dir = os.path.dirname(__file__)
    DATASET = pd.read_pickle(script_dir + '/data/processed/dataset.pkl.zst')
    companies_list = list(DATASET['CD_CVM'].unique())

    def __init__(
        self,
        cvm_number: int,
        report_type: str = 'consolidated',
        first_period: str = '2009-12-31',
        last_period: str = '2200-12-31',
        show_accounts: int = 0,
        unit: float = 1
    ):
        """Initialize main variables.

        Args:
            cvm_number (int): CVM unique number of the company.
            report_type (str, optional): 'consolidated' or 'separate'.
            first_period: first accounting period in YYYY-MM-DD format
            last_period: last accounting period in YYYY-MM-DD format
            unit (float, optional): number to divide account values
            show_accounts: account levels to show (default = show all accounts)
        """
        self.cvm_number = cvm_number
        self.report_type = report_type
        self.first_period = first_period
        self.last_period = last_period
        self.unit = unit
        self.show_accounts = show_accounts
        self._set_main_df()

    @property
    def cvm_number(self):
        """Return cvm_number if number exists in DATASET."""
        return self._cvm_number

    @cvm_number.setter
    def cvm_number(self, value):
        if value in Finance.companies_list:
            self._cvm_number = value
        else:
            print('cvm_number not found')
            self._cvm_number = None

    @property
    def report_type(self):
        """Return selected FS type (report_type).

        Options are: 'consolidated' or 'separate'
        """
        return self._report_type

    @report_type.setter
    def report_type(self, value):
        if value in ('consolidated', 'separate'):
            self._report_type = value
        else:
            raise ValueError("Select 'consolidated' or 'separate' report type")

    @property
    def first_period(self):
        """Return selected start date for filtering FS end period."""
        return self._min_end_period

    @first_period.setter
    def first_period(self, value):
        value = pd.to_datetime(value, errors='coerce')
        if value == pd.NaT:
            print('Inserted first_period period not in YYYY-MM-DD format')
            print('2009-12-31 selected instead')
            self._min_end_period = pd.to_datetime('2009-12-31')
        else:
            print(f"Selected first_period = {value.date()}")
            self._min_end_period = value

    @property
    def last_period(self):
        """Return selected end date for filtering FS end period."""
        return self._max_end_period

    @last_period.setter
    def last_period(self, value):
        value = pd.to_datetime(value, errors='coerce')
        if value == pd.NaT:
            print('Inserted last_period not in YYYY-MM-DD format')
            print('2200-12-31 selected instead')
            self._max_end_period = pd.to_datetime('2200-12-31')
        else:
            print(f"Selected last_period = {value.date()}")
            self._max_end_period = value

    @property
    def show_accounts(self):
        """Return account levels to show: default = 0 (show all accounts).
        X.YY.ZZ.WW...   level 0
        X.YY            level 1
        X.YY.ZZ         level 2
        X.YY.ZZ.YY      level 3
        """

        return self._account_mode

    @show_accounts.setter
    def show_accounts(self, value):
        if value in [0, 1, 2, 3]:
            self._account_mode = value
        else:
            raise ValueError(
                "Account levels are: 0 (show all accounts), 1, 2, 3.")

    @property
    def unit(self):
        """Return the number by which account values are being divided."""
        return self._unit

    @unit.setter
    def unit(self, value):
        if value > 0:
            self._unit = value
        else:
            raise ValueError("Unit value must be greater than 0")

    def _set_main_df(self) -> pd.DataFrame:
        self._MAIN_DF = Finance.DATASET.query(
            'CD_CVM == @self.cvm_number').copy()
        self._MAIN_DF = self._MAIN_DF.astype({
            'CD_CVM': 'object',
            'report_period': str,
            'report_type': str,
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
        """
        Get accounting code (ac) levels 1 and 2 from CD_CONTA column.
        The first part of CD_CONTA is the FS type
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
        self._MAIN_DF['account_code_p1'] = self._MAIN_DF['CD_CONTA'].str[0]
        self._MAIN_DF['account_code_p12'] = (
            self._MAIN_DF['CD_CONTA'].str[0:4]
        )
        self._MAIN_DF.sort_values(
            by='CD_CONTA', ignore_index=True, inplace=True)

    def _get_company_df(self) -> pd.DataFrame:
        query_expression = '''
            report_type == @self.report_type and \
            DT_FIM_EXERC >= @self.first_period and \
            DT_FIM_EXERC <= @self.last_period
        '''
        df = self._MAIN_DF.query(query_expression).copy()
        # change unit only for accounts different from 3.99
        df['VL_CONTA'] = np.where(
            df['account_code_p12'] == '3.99',
            df['VL_CONTA'],
            df['VL_CONTA'] / self._unit
        )
        df['account_code_len'] = df['CD_CONTA'].str.len()
        # show only selected accounting levels
        if self.show_accounts > 0:
            account_code_limit = self.show_accounts * 3 + 1  # noqa
            df.query('account_code_len <= @account_code_limit', inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

    @property
    def info(self) -> dict:
        """Return company info."""
        annual_reports = self._MAIN_DF.query('report_period == "annual"')
        annual_reports = annual_reports.DT_REFER.dt.strftime('%Y-%m-%d')
        annual_reports = list(annual_reports.unique())
        annual_reports.sort()
        last_quarterly_report = self._MAIN_DF.query(
            'report_period == "quarterly"').DT_REFER.max()
        last_quarterly_report = last_quarterly_report.strftime('%Y-%m-%d')

        company_info = {
            'CVM number': self._MAIN_DF.loc[0, 'CD_CVM'],
            'Fiscal Code': self._MAIN_DF.loc[0, 'CNPJ_CIA'],
            'Name': self._MAIN_DF.loc[0, 'DENOM_CIA'],
            'Annual Reports': annual_reports,
            'Last Quarterly Report': last_quarterly_report,
        }
        return company_info

    @property
    def assets(self) -> pd.DataFrame:
        """Return company assets."""
        df = self._get_company_df()
        df.query('account_code_p1 == "1"', inplace=True)
        return self._make_report(df)

    @property
    def liabilities_and_equity(self) -> pd.DataFrame:
        """Return company liabilities and equity."""
        df = self._get_company_df()
        df.query('account_code_p1 == "2"', inplace=True)
        return self._make_report(df)

    @property
    def liabilities(self) -> pd.DataFrame:
        """Return company liabilities."""
        df = self._get_company_df()
        df.query(
            'account_code_p12 == "2.01" or account_code_p12 == "2.02"',
            inplace=True
        )
        return self._make_report(df)

    @property
    def equity(self) -> pd.DataFrame:
        """Return company equity."""
        df = self._get_company_df()
        df.query('account_code_p12 == "2.03"', inplace=True)
        return self._make_report(df)

    @property
    def earnings_per_share(self) -> pd.DataFrame:
        """Return company equity.
        3.99                -> Earnings per Share (BRL / Share)
            3.99.01         -> Earnings per Share
                3.99.01.01  -> ON (ordinary)
            3.99.02         -> Diluted Earnings per Share
                3.99.02.01  -> ON (ordinary)
        """
        df = self._get_company_df()
        df.query(
            'CD_CONTA == "3.99.01.01" or CD_CONTA == "3.99.02.01"',
            inplace=True
        )

        return self._make_report(df)

    @staticmethod
    def calculate_ltm(df_flow: pd.DataFrame) -> pd.DataFrame:
        last_annual = df_flow.query(
            'report_period == "annual"')['DT_FIM_EXERC'].max()
        last_quarterly = df_flow.query(
            'report_period == "quarterly"')['DT_FIM_EXERC'].max()
        if last_annual > last_quarterly:
            df_flow.query('report_period == "annual"', inplace=True)
            return df_flow

        df1 = df_flow.query('DT_FIM_EXERC == @last_quarterly').copy()
        df1.query('DT_INI_EXERC == DT_INI_EXERC.min()', inplace=True)

        df2 = df_flow.query('DT_REFER == @last_quarterly').copy()
        df2.query('DT_INI_EXERC == DT_INI_EXERC.min()', inplace=True)
        df2['VL_CONTA'] = -df2['VL_CONTA']

        df3 = df_flow.query('DT_FIM_EXERC == @last_annual').copy()

        df_ltm = pd.concat([df1, df2, df3], ignore_index=True)
        df_ltm = df_ltm[['CD_CONTA', 'VL_CONTA']]
        df_ltm = df_ltm.groupby(by='CD_CONTA').sum().reset_index()
        df1.drop(columns='VL_CONTA', inplace=True)
        df_ltm = pd.merge(df1, df_ltm)
        df_ltm['report_period'] = 'ltm'
        df_ltm['DT_INI_EXERC'] = last_quarterly - pd.DateOffset(years=1)

        df_flow.query('report_period == "annual"', inplace=True)
        df_flow_ltm = pd.concat([df_flow, df_ltm], ignore_index=True)
        return df_flow_ltm

    @property
    def income(self) -> pd.DataFrame:
        """Return company income statement."""
        df = self._get_company_df()
        df.query('account_code_p1 == "3"', inplace=True)
        df = Finance.calculate_ltm(df)
        return self._make_report(df)

    @property
    def cash_flow(self) -> pd.DataFrame:
        """Return company income statement."""
        df = self._get_company_df()
        df.query('account_code_p1 == "6"', inplace=True)
        df = Finance.calculate_ltm(df)
        return self._make_report(df)

    @staticmethod
    def account_value(account_code: str, df: pd.DataFrame) -> float:
        """Return value for an account in dataframe."""
        df.query('CD_CONTA == @account_code', inplace=True)
        return df.iloc[0]['VL_CONTA']

    @property
    def operating_performance(self):
        """Return company main operating indicators."""
        df = self._get_company_df()
        df_as = self.assets
        df_as.query('CD_CONTA == "1"', inplace=True)
        df_le = self.liabilities_and_equity
        df_le.query('CD_CONTA == "2.03"', inplace=True)
        df_in = self.income
        # df_in.query('CD_CONTA == "3.11"', inplace=True)
        df = pd.concat([df_as, df_le, df_in], ignore_index=True)
        df.set_index(keys='CD_CONTA', drop=True, inplace=True)
        df.drop(columns=['ST_CONTA_FIXA', 'DS_CONTA'], inplace=True)
        df.loc['return_on_assets'] = df.loc['3.11'] / df.loc['1']
        df.loc['return_on_equity'] = df.loc['3.11'] / df.loc['2.03']
        df.loc['gross_margin'] = df.loc['3.03'] / df.loc['3.01']
        df.loc['ebit_margin'] = df.loc['3.05'] / df.loc['3.01']
        df.loc['operating_margin'] = (
            (df.loc['3.03'] + df.loc['3.04.01'] + df.loc['3.04.02'])
            / df.loc['3.01']
        )
        df.loc['net_margin'] = df.loc['3.11'] / df.loc['3.01']

        # 3.03 3.04.01 3.04.02

        # df.reset_index(drop=True, inplace=True)
        return df

    @property
    def valuation(self) -> pd.DataFrame:
        accounts = [  # noqa
            '3.01', '3.02', '3.03', '3.04', '3.04.01', '3.04.02', '3.04.03',
            '3.04.04', '3.04.05', '3.04.06', '3.05', '3.06', '3.07', '3.08',
            '3.09', '3.10', '3.10.01', '3.11', '1.01', '1.01.01', '1.01.02',
            '1.01.03', '1.01.04', '1.01.05', '1.01.06', '1.01.07', '1.01.08',
            '1.02.01', '1.02.02', '1.02.03', '1.02.04', '2.01', '2.01.01',
            '2.01.02', '2.01.03', '2.01.04', '2.01.05', '2.01.06', '2.02',
            '2.02.01', '2.02.02', '2.02.03', '2.02.04', '2.03', '2.03.09',
            '6.01', '6.01.01', '6.01.02'
        ]
        df_as = self.assets
        df_le = self.liabilities_and_equity
        df_is = self.income
        df = pd.concat([df_as, df_le, df_is], ignore_index=True)
        df.query('CD_CONTA == @accounts', inplace=True)
        return df

    def _make_report(self, df: pd.DataFrame) -> pd.DataFrame:
        # keep only last quarterly fs
        last_end_period = df.DT_FIM_EXERC.max()  # noqa
        query_expression = '''
            report_period == 'annual' or \
            DT_FIM_EXERC == @last_end_period
        '''
        df.query(query_expression, inplace=True)
        # sort for drop operation
        df.sort_values(['DT_FIM_EXERC', 'DT_REFER', 'CD_CONTA'], inplace=True)
        # only last published statements will be used
        df['financial_year'] = df.DT_FIM_EXERC.dt.year
        df.drop_duplicates(
            subset=['financial_year', 'CD_CONTA'],
            keep='last',
            inplace=True,
            ignore_index=True
        )
        base_columns = ['DS_CONTA', 'CD_CONTA', 'ST_CONTA_FIXA']
        df_report = df.loc[:, base_columns]
        df_report.drop_duplicates(ignore_index=True, inplace=True)

        merge_columns = base_columns + ['VL_CONTA']
        for period in df.DT_FIM_EXERC.unique():
            # print(date)
            df_year = df.query('DT_FIM_EXERC == @period').copy()
            df_year = df_year[merge_columns]
            df_year.rename(
                columns={'VL_CONTA': np.datetime_as_string(period, unit='D')},
                inplace=True)
            df_report = pd.merge(df_report, df_year, how='left')

        df_report.sort_values('CD_CONTA', ignore_index=True, inplace=True)
        return df_report
