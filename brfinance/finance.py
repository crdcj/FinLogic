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
    TAX_RATE = 0.34
    companies_list = list(DATASET['cvm_id'].unique())

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

    @classmethod
    def search_company(cls, expression: str) -> pd.DataFrame:
        """Return dataframe with companies that matches the 'expression'"""
        expression = expression.upper()
        mask = cls.DATASET.company_name.str.contains(expression)
        df = cls.DATASET[mask].copy()
        df.sort_values(by='company_name', inplace=True)
        df.drop_duplicates(subset='cvm_id', inplace=True, ignore_index=True)
        columns = ['company_name', 'cvm_id', 'fiscal_id']
        return df[columns]

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
            'cvm_id == @self.cvm_number').copy()
        self._MAIN_DF = self._MAIN_DF.astype({
            'cvm_id': np.uint32,
            'fiscal_id': str,
            'company_name': str,
            'report_period': str,
            'report_version': str,
            'report_type': str,
            'reference_date': 'datetime64',
            'start_date': 'datetime64',
            'end_date': 'datetime64',
            'year_order': np.int8,
            'account_code': str,
            'account_name': str,
            'fixed_account': bool,
            'report_column': str,
            'account_value': float,
        })
        """
        Get accounting code (ac) levels 1 and 2 from 'account_code' column.
        The first part of 'account_code' is the FS type
        df['account_code'].str[0].unique() -> [1, 2, 3, 4, 5, 6, 7]
        Table of correspondences:
            1 -> Balanço Patrimonial Ativo
            2 -> Balanço Patrimonial Passivo
            3 -> Demonstração do Resultado
            4 -> Demonstração de Resultado Abrangente
            5 -> Demonstração das Mutações do Patrimônio Líquido
            6 -> Demonstração do Fluxo de Caixa (Método Indireto)
            7 -> Demonstração de Valor Adicionado
        """
        self._MAIN_DF['account_code_p1'] = self._MAIN_DF['account_code'].str[0]
        self._MAIN_DF['account_code_p12'] = (
            self._MAIN_DF['account_code'].str[0:4]
        )
        self._MAIN_DF.sort_values(
            by='account_code', ignore_index=True, inplace=True)

    def _get_company_df(self) -> pd.DataFrame:
        query_expression = '''
            report_type == @self.report_type and \
            end_date >= @self.first_period and \
            end_date <= @self.last_period
        '''
        df = self._MAIN_DF.query(query_expression).copy()
        # change unit only for accounts different from 3.99
        df['account_value'] = np.where(
            df['account_code_p12'] == '3.99',
            df['account_value'],
            df['account_value'] / self._unit
        )
        df['account_code_len'] = df['account_code'].str.len()
        # show only selected accounting levels
        if self.show_accounts > 0:
            account_code_limit = self.show_accounts * 3 + 1  # noqa
            df.query('account_code_len <= @account_code_limit', inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

    @property
    def info(self) -> dict:
        """Return company info."""
        dfa = self._MAIN_DF.query('report_period == "annual"')
        dfq = self._MAIN_DF.query('report_period == "quarterly"')
        first_annual_report = dfa['reference_date'].min().strftime('%Y-%m-%d')
        last_annual_report = dfa['reference_date'].max().strftime('%Y-%m-%d')
        last_quarterly_report = (
            dfq['reference_date'].max().strftime('%Y-%m-%d')
        )

        company_info = {
            'CVM Number': self._MAIN_DF.loc[0, 'cvm_id'],
            'Fiscal Number': self._MAIN_DF.loc[0, 'fiscal_id'],
            'Company Name': self._MAIN_DF.loc[0, 'company_name'],
            'First Annual Report': first_annual_report,
            'Last Annual Report': last_annual_report,
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
            'account_code == "3.99.01.01" or account_code == "3.99.02.01"',
            inplace=True
        )

        return self._make_report(df)

    @staticmethod
    def calculate_ltm(df_flow: pd.DataFrame) -> pd.DataFrame:
        last_annual = df_flow.query(
            'report_period == "annual"')['end_date'].max()
        last_quarterly = df_flow.query(
            'report_period == "quarterly"')['end_date'].max()
        if last_annual > last_quarterly:
            df_flow.query('report_period == "annual"', inplace=True)
            return df_flow

        df1 = df_flow.query('end_date == @last_quarterly').copy()
        df1.query('start_date == start_date.min()', inplace=True)

        df2 = df_flow.query('reference_date == @last_quarterly').copy()
        df2.query('start_date == start_date.min()', inplace=True)
        df2['account_value'] = -df2['account_value']

        df3 = df_flow.query('end_date == @last_annual').copy()

        df_ltm = pd.concat([df1, df2, df3], ignore_index=True)
        df_ltm = df_ltm[['account_code', 'account_value']]
        df_ltm = df_ltm.groupby(by='account_code').sum().reset_index()
        df1.drop(columns='account_value', inplace=True)
        df_ltm = pd.merge(df1, df_ltm)
        df_ltm['report_period'] = 'ltm'
        df_ltm['start_date'] = last_quarterly - pd.DateOffset(years=1)

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
        df.query('account_code == @account_code', inplace=True)
        return df.iloc[0]['account_value']

    @staticmethod
    def shift_right(s: pd.Series, is_on: bool) -> pd.Series:
        """Shift row to the right in order to obtain series previous values"""
        if is_on:
            arr = s.iloc[:-1].values
            return np.append(np.nan, arr)
        else:
            return s

    def operating_performance(self, is_on: bool = True):
        """Return company main operating indicators."""
        df = self._get_company_df()
        df_as = self.assets
        # df_as.query('account_code == "1"', inplace=True)
        df_le = self.liabilities_and_equity
        # df_le.query('account_code == "2.03"', inplace=True)
        df_in = self.income
        # df_in.query('account_code == "3.11"', inplace=True)
        df = pd.concat([df_as, df_le, df_in], ignore_index=True)
        df.set_index(keys='account_code', drop=True, inplace=True)
        df.drop(columns=['fixed_account', 'account_name'], inplace=True)

        # series definition
        revenues = df.loc['3.01']
        gross_profit = df.loc['3.03']
        ebit = df.loc['3.05']
        net_income = df.loc['3.11']
        total_assets = self.shift_right(df.loc['1'], is_on)
        equity = self.shift_right(df.loc['2.03'], is_on)
        invested_capital = (
            df.loc['2.03']
            + df.loc['2.01.04']
            + df.loc['2.02.01']
            - df.loc['1.01.01']
            - df.loc['1.01.02']
        )
        invested_capital = self.shift_right(invested_capital, is_on)

        # indicators calculation
        df.loc['return_on_assets'] = (
            ebit * (1 - Finance.TAX_RATE) / total_assets
        )
        df.loc['return_on_capital'] = (
            ebit * (1 - Finance.TAX_RATE) / invested_capital
        )
        df.loc['return_on_equity'] = net_income / equity
        df.loc['gross_margin'] = gross_profit / revenues
        df.loc['operating_margin'] = ebit * (1 - Finance.TAX_RATE) / revenues
        df.loc['net_margin'] = net_income / revenues

        # discard rows used for calculation
        df = df.iloc[-6:]
        # discard index name 'account_code'
        df.index.name = None
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
        df.query('account_code == @accounts', inplace=True)
        return df

    def _make_report(self, df: pd.DataFrame) -> pd.DataFrame:
        # keep only last quarterly fs
        last_end_period = df.end_date.max()  # noqa
        query_expression = '''
            report_period == 'annual' or \
            end_date == @last_end_period
        '''
        df.query(query_expression, inplace=True)
        # sort for drop operation
        df.sort_values(
            ['end_date', 'reference_date', 'account_code'],
            inplace=True
        )
        # only last published statements will be used
        df['financial_year'] = df.end_date.dt.year
        df.drop_duplicates(
            subset=['financial_year', 'account_code'],
            keep='last',
            inplace=True,
            ignore_index=True
        )
        base_columns = ['account_name', 'account_code', 'fixed_account']
        df_report = df.loc[:, base_columns]
        df_report.drop_duplicates(ignore_index=True, inplace=True)

        merge_columns = base_columns + ['account_value']
        for period in df.end_date.unique():
            # print(date)
            df_year = df.query('end_date == @period').copy()
            df_year = df_year[merge_columns]
            period_str = np.datetime_as_string(period, unit='D')
            df_year.rename(columns={'account_value': period_str}, inplace=True)
            df_report = pd.merge(df_report, df_year, how='left')

        df_report.sort_values('account_code', ignore_index=True, inplace=True)
        return df_report
