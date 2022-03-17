"""Module containing Finance Class definition for Brazilian Corporations.
Abbreviation used for Financial Statement = FS
df['account_code'].str[0].unique() -> [1, 2, 3, 4, 5, 6, 7]
The first part of 'account_code' is the FS type
Table of statements correspondence:
    1 -> Balance Sheet - Assets
    2 -> Balance Sheet - Liabilities and Shareholders’ Equity
    3 -> Income
    4 -> Comprehensive Income
    5 -> Changes in Equity
    6 -> Cash Flow (Indirect Method)
    7 -> Added Value
"""
import os
import numpy as np
import pandas as pd


class Finance():
    """corporation Financials Class for Brazilian Companies."""

    script_dir = os.path.dirname(__file__)
    DATASET = pd.read_pickle(script_dir + '/data/processed/dataset.pkl.zst')
    TAX_RATE = 0.34
    CORP_IDS = list(DATASET['corp_id'].unique())
    FISCAL_IDS = list(DATASET['corp_fiscal_id'].unique())

    def __init__(self, identifier):
        """Initialize main variables.

        Args:
            identifier: can be used both CVM (regulator) ID or Fiscal ID.
                CVM ID must be an integer
                Fiscal ID must be a string in the format: 'XX.XXX.XXX/XXXX-XX'
            
            unit (float, optional): number to divide account values
        """
        # Control atributes validation during object initalization
        self.identifier = identifier

    @classmethod
    def search_corp_name(cls, expression: str) -> pd.DataFrame:
        """Return dataframe with corp. names that contains the 'expression'"""
        expression = expression.upper()
        mask = cls.DATASET['corp_name'].str.contains(expression)
        df = cls.DATASET[mask].copy()
        df.sort_values(by='corp_name', inplace=True)
        df.drop_duplicates(subset='corp_id', inplace=True, ignore_index=True)
        columns = ['corp_name', 'corp_id', 'corp_fiscal_id']
        return df[columns]

    @property
    def identifier(self):
        """Change corporation identifier."""
        return self._identifier

    @identifier.setter
    def identifier(self, value):
        self._identifier = value
        # Checks for value existance in DATASET
        if value in Finance.CORP_IDS:
            self._corp_id = value
            df = Finance.DATASET.query('corp_id == @self._corp_id').copy()
            df.reset_index(drop=True, inplace=True)
            self._corp_fiscal_id = df.loc[0, 'corp_fiscal_id']
        elif value in Finance.FISCAL_IDS:
            self._corp_fiscal_id = value
            expression = 'corp_fiscal_id == @self._corp_fiscal_id'
            df = Finance.DATASET.query(expression).copy()
            df.reset_index(drop=True, inplace=True)
            self._corp_id = df.loc[0, 'corp_id']
        else:
            raise ValueError(
                "Selected CVM ID or Fiscal ID for the corporation not found")
        # Only modify _DF after first object atributes validation in __init__
        self._set_main_data()

    def _set_main_data(self) -> pd.DataFrame:
        self._CORP_DF = Finance.DATASET.query(
            'corp_id == @self._corp_id').copy()
        self._CORP_DF = self._CORP_DF.astype({
            'corp_name': str,
            'corp_id': np.uint32,
            'corp_fiscal_id': str,            
            'report_type': str,
            'report_version': str,
            'period_reference': 'datetime64',
            'period_begin': 'datetime64',
            'period_end': 'datetime64',
            'period_order': np.int8,
            'account_code': str,
            'account_name': str,
            'accounting_method': str,
            'account_fixed': bool,
            'account_value': float,
            'equity_statement_column': str,
        })
        self._CORP_DF.sort_values(
            by='account_code', ignore_index=True, inplace=True)

        self._CORP_NAME = self._CORP_DF['corp_name'].unique()[0]
        self._FIRST_ANNUAL = self._CORP_DF.query(
            'report_type == "annual"')['period_end'].min()
        self._LAST_ANNUAL = self._CORP_DF.query(
            'report_type == "annual"')['period_end'].max()
        self._LAST_QUARTERLY = self._CORP_DF.query(
            'report_type == "quarterly"')['period_end'].max()

    def info(self) -> dict:
        """Return dictionary with corporation info."""
        f = '%Y-%m-%d'
        corporation_info = {
            'Corp. Name': self._CORP_NAME,
            'Corp. ID (CVM number)': self._corp_id,
            'Corp. Fiscal ID': self._corp_fiscal_id,
            'First Annual Report': self._FIRST_ANNUAL.strftime(f),
            'Last Annual Report': self._LAST_ANNUAL.strftime(f),
            'Last Quarterly Report': self._LAST_QUARTERLY.strftime(f),
        }
        return corporation_info

    def report(
        self,
        rtype: str,
        accounting_method: str='consolidated',
        unit: float=1,
        account_level: int=0,
        first_period: str='2009-01-01'
    ) -> pd.DataFrame:
        """
        Return Corporation Financial Statements.

        Parameters
        ----------

        type: assets, liabilities_and_equity, liabilities, equity, income and
            cash_flow
        accounting_method (str, optional): 'consolidated' or 'separate'.
        account_level (int, optional): detail level to show for account codes
            account_level = 0 -> X...       (default: show all accounts)
            account_level = 2 -> X.YY       (show 2 levels)
            account_level = 3 -> X.YY.ZZ    (show 3 levels)
            account_level = 4 -> X.YY.ZZ.WW (show 4 levels)
        first_period: first accounting period, in YYYY-MM-DD format, to show
        """
        first_period = pd.to_datetime(first_period, errors='coerce')
        if first_period == pd.NaT:
            raise ValueError(
                'first_period expects a string in YYYY-MM-DD format')

        if accounting_method not in ['consolidated', 'separate']:
            raise ValueError(
                "accounting_method expects 'consolidated' or 'separate'")

        if unit <= 0:
            raise ValueError("Unit expects a value greater than 0")

        if account_level not in [0, 1, 2, 3, 4]:
            raise ValueError(
                "account_level expects integers 0, 1, 2, 3 or 4")

        expression = '''
            accounting_method == @accounting_method and \
            period_end >= @first_period
        '''
        df = self._CORP_DF.query(expression).copy()

        # Change unit only for accounts different from 3.99
        df['account_value'] = np.where(
            df['account_code'].str.startswith('3.99'),
            df['account_value'],
            df['account_value'] / unit
        )

        # Filter dataframe for selected account_level
        if account_level:
            account_code_limit = account_level * 3 - 2 # noqa
            expression = 'account_code.str.len() <= @account_code_limit'
            df.query(expression, inplace=True)

        # Filter dataframe for selected rtype (report type)
        report_types = {
            "assets": ["1"],
            "liabilities_and_equity": ["2"],
            "liabilities": ["2.01", "2.02"],
            "equity": ["2.03"],
            "income": ["3"],
            "cash_flow": ["6"],
            "earnings_per_share": ["3.99.01.01", "3.99.02.01"]
        }
        account_codes = report_types[rtype]
        expression = ''
        for count, account_code in enumerate(account_codes):
            if count > 0:
                expression += " or "
            expression += f'account_code.str.startswith("{account_code}")'
        df.query(expression, inplace=True)

        if rtype in ['income', 'cash_flow']:
            df = self._calculate_ltm(df)

        df.reset_index(drop=True, inplace=True)
        return self._make_report(df)

    def _calculate_ltm(self, df_flow: pd.DataFrame) -> pd.DataFrame:
        if self._LAST_ANNUAL > self._LAST_QUARTERLY:
            df_flow.query('report_type == "annual"', inplace=True)
            return df_flow

        df1 = df_flow.query('period_end == @self._LAST_QUARTERLY').copy()
        df1.query('period_begin == period_begin.min()', inplace=True)

        df2 = df_flow.query('period_reference == @self._LAST_QUARTERLY').copy()
        df2.query('period_begin == period_begin.min()', inplace=True)
        df2['account_value'] = -df2['account_value']

        df3 = df_flow.query('period_end == @self._LAST_ANNUAL').copy()

        df_ltm = pd.concat([df1, df2, df3], ignore_index=True)
        df_ltm = df_ltm[['account_code', 'account_value']]
        df_ltm = df_ltm.groupby(by='account_code').sum().reset_index()
        df1.drop(columns='account_value', inplace=True)
        df_ltm = pd.merge(df1, df_ltm)
        df_ltm['report_type'] = 'quarterly'
        df_ltm['period_begin'] = self._LAST_QUARTERLY - pd.DateOffset(years=1)

        df_flow.query('report_type == "annual"', inplace=True)
        df_flow_ltm = pd.concat([df_flow, df_ltm], ignore_index=True)
        return df_flow_ltm

    @staticmethod
    def shift_right(s: pd.Series, is_shifted: bool) -> pd.Series:
        """Shift row to the right in order to obtain series previous values"""
        if is_shifted:
            arr = s.iloc[:-1].values
            return np.append(np.nan, arr)
        else:
            return s

    def get_accounts(
        self,
        accounts: list,
        unit: float=1,
        first_period: str='2009-01-01'
    ) -> pd.DataFrame:
        """Return a report for a list of account codes"""
        kwargs = {'unit': unit, 'first_period': first_period}
        df_as = self.report('assets', **kwargs)
        df_le = self.report('liabilities_and_equity', **kwargs)
        df_is = self.report('income', **kwargs)
        df_cf = self.report('cash_flow', **kwargs)
        df = pd.concat([df_as, df_le, df_is, df_cf], ignore_index=True)
        df.query('account_code == @accounts', inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

    def operating_performance(self, is_shifted: bool = True):
        """Return corporation main operating indicators."""
        df_as = self.report('assets')
        df_le = self.report('liabilities_and_equity')
        df_in = self.report('income')
        df = pd.concat([df_as, df_le, df_in], ignore_index=True)
        df.set_index(keys='account_code', drop=True, inplace=True)
        df.drop(columns=['account_fixed', 'account_name'], inplace=True)
        df.fillna(0, inplace=True)

        # series with indicators
        revenues = df.loc['3.01']
        gross_profit = df.loc['3.03']
        ebit = df.loc['3.05']
        net_income = df.loc['3.11']
        total_assets = self.shift_right(df.loc['1'], is_shifted)
        equity = self.shift_right(df.loc['2.03'], is_shifted)
        invested_capital = (
            df.loc['2.03']
            + df.loc['2.01.04']
            + df.loc['2.02.01']
            - df.loc['1.01.01']
            - df.loc['1.01.02']
        )
        invested_capital = self.shift_right(invested_capital, is_shifted)

        # dfi: dataframe with indicators
        dfi = pd.DataFrame(columns=df.columns)
        dfi.loc['return_on_assets'] = (
            ebit * (1 - Finance.TAX_RATE) / total_assets
        )
        # dfi.loc['invested_capital'] = invested_capital
        dfi.loc['return_on_capital'] = (
            ebit * (1 - Finance.TAX_RATE) / invested_capital
        )
        dfi.loc['return_on_equity'] = net_income / equity
        dfi.loc['gross_margin'] = gross_profit / revenues
        dfi.loc['operating_margin'] = ebit * (1 - Finance.TAX_RATE) / revenues
        dfi.loc['net_margin'] = net_income / revenues

        return dfi

    def _make_report(self, df: pd.DataFrame) -> pd.DataFrame:
        # keep only last quarterly fs
        if self._LAST_ANNUAL > self._LAST_QUARTERLY:
            df.query('report_type == "annual"', inplace=True)
            expression = '''
                period_order == -1 or \
                period_end == @self._LAST_ANNUAL
            '''
            df.query(expression, inplace=True)
        else:
            expression = '''
                report_type == 'annual' or \
                period_end == @self._LAST_QUARTERLY
            '''
            df.query(expression, inplace=True)
            expression = '''
                period_order == -1 or \
                period_end == @self._LAST_QUARTERLY or \
                period_end == @self._LAST_ANNUAL
            '''
            df.query(expression, inplace=True)

        # Create Index
        df.sort_values(by='period_end', inplace=True, ascending=True)
        base_columns = ['account_name', 'account_code', 'account_fixed']
        df_report = df.loc[:, base_columns]
        df_report.drop_duplicates(
            subset='account_code',
            ignore_index=True,
            inplace=True,
            keep='last'
        )

        periods = list(df.period_end.unique())
        periods.sort()
        for period in periods:
            # print(date)
            df_year = df.query('period_end == @period').copy()
            df_year = df_year[['account_value', 'account_code']]
            period_str = np.datetime_as_string(period, unit='D')
            if period == self._LAST_QUARTERLY:
                period_str += ' (ltm)'

            df_year.rename(columns={'account_value': period_str}, inplace=True)
            df_report = pd.merge(
                df_report,
                df_year,
                how='left',
                on=['account_code']
            )

        df_report.sort_values('account_code', ignore_index=True, inplace=True)
        return df_report
