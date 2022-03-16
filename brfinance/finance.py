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

    def __init__(
            self,
            identifier,
            accounting_method: str = 'consolidated',
            unit: float = 1):
        """Initialize main variables.

        Args:
            identifier: can be used both CVM (regulator) ID or Fiscal ID.
                CVM ID must be an integer
                Fiscal ID must be a string in the format: 'XX.XXX.XXX/XXXX-XX'
            accounting_method (str, optional): 'consolidated' or 'separate'.
            unit (float, optional): number to divide account values
        """
        # Control atributes validation during object initalization
        self._is_object_initialized = False  
        self.identifier = identifier
        self.accounting_method = accounting_method
        self.unit = unit
        self._set_main_data()
        self._is_object_initialized = True

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
                "Selected CVM ID or Fiscal ID for the corporation  not found")
        # Only modify _DF after first object atributes validation in __init__
        if self._is_object_initialized:
            self._set_main_data()

    @property
    def accounting_method(self):
        """Change accounting method for subsidiaries registration.

        Options are: 'consolidated' or 'separate'
        """
        return self._accounting_method

    @accounting_method.setter
    def accounting_method(self, value):
        if value in ('consolidated', 'separate'):
            self._accounting_method = value
        else:
            raise ValueError("Select 'consolidated' or 'separate' report type")
        # Only modify _DF after first object atributes validation in __init__
        if self._is_object_initialized:
            self._set_main_data()

    @property
    def unit(self):
        """Divide account values by 'unit' number."""
        return self._unit

    @unit.setter
    def unit(self, value):
        if value > 0:
            self._unit = value
        else:
            raise ValueError("Unit value must be greater than 0")
        # Only modify _DF after first object atributes validation in __init__
        if self._is_object_initialized:
            self._set_main_data()

    def _set_main_data(self) -> pd.DataFrame:
        expression = '''
            corp_id == @self._corp_id and \
            accounting_method == @self._accounting_method
        '''
        self._MAIN_DF = Finance.DATASET.query(expression).copy()
        self._MAIN_DF = self._MAIN_DF.astype({
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
        # Change unit only for accounts different from 3.99
        self._MAIN_DF['account_value'] = np.where(
            self._MAIN_DF['account_code'].str.startswith('3.99'),
            self._MAIN_DF['account_value'],
            self._MAIN_DF['account_value'] / self._unit
        )
        self._MAIN_DF.sort_values(
            by='account_code', ignore_index=True, inplace=True)
        self._CORP_NAME = self._MAIN_DF['corp_name'].unique()[0]
        self._FIRST_ANNUAL = self._MAIN_DF.query(
            'report_type == "annual"')['period_end'].min()
        self._LAST_ANNUAL = self._MAIN_DF.query(
            'report_type == "annual"')['period_end'].max()
        self._LAST_QUARTERLY = self._MAIN_DF.query(
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

    def _filter_main_df(self, account_level, first_period) -> pd.DataFrame:
        first_period = pd.to_datetime(first_period, errors='coerce')
        if first_period == pd.NaT:
            raise ValueError(
                'Inserted first_period period not in YYYY-MM-DD format')
        df = self._MAIN_DF.query("period_end >= @first_period").copy()

        # Show only selected accounting account_level
        if account_level not in [0, 2, 3, 4]:
            raise ValueError(
                "account_level not equal to 0 (all accounts) 2, 3 or 4 ")

        # Filter dataframe only with selected account_level
        if account_level:
            account_code_limit = account_level * 3 - 2 # noqa
            df.query(
                'account_code.str.len() <= @account_code_limit',
                inplace=True)

        df.reset_index(drop=True, inplace=True)
        return df

    def assets(
            self,
            account_level=0,
            first_period: str = '2009-01-01'
        ) -> pd.DataFrame:
        """
        Return corporation assets.

        account_level (int, optional): detail level to show for account codes
            account_level = 0 -> X...       (default: show all accounts)
            account_level = 2 -> X.YY       (show 2 levels)
            account_level = 3 -> X.YY.ZZ    (show 3 levels)
            account_level = 4 -> X.YY.ZZ.WW (show 4 levels)
        first_period: first accounting period in YYYY-MM-DD format to show
        """
        df = self._filter_main_df(
            account_level=account_level,
            first_period=first_period
        )
        df.query('account_code.str.startswith("1")', inplace=True)
        return self._make_report(df)

    def liabilities_and_equity(
            self,
            account_level: int = 0,
            first_period: str = '2009-01-01'
        ) -> pd.DataFrame:
        """
        Return corporation liabilities and equity.

        account_level (int, optional): detail level to show for account codes
            account_level = 0 -> X...       (default: show all accounts)
            account_level = 2 -> X.YY       (show 2 levels)
            account_level = 3 -> X.YY.ZZ    (show 3 levels)
            account_level = 4 -> X.YY.ZZ.WW (show 4 levels)
        first_period: first accounting period in YYYY-MM-DD format to show
        """
        df = self._filter_main_df(
            account_level=account_level,
            first_period=first_period
        )
        df.query('account_code.str.startswith("2")', inplace=True)
        return self._make_report(df)

    def liabilities(
            self,
            account_level: int = 0,
            first_period: str = '2009-01-01'
        ) -> pd.DataFrame:
        """
        Return corporation liabilities.

        account_level (int, optional): detail level to show for account codes
            account_level = 0 -> X...       (default: show all levels)
            account_level = 2 -> X.YY       (show 2 levels)
            account_level = 3 -> X.YY.ZZ    (show 3 levels)
            account_level = 4 -> X.YY.ZZ.WW (show 4 levels)
        first_period: first accounting period in YYYY-MM-DD format to show
        """
        df = self._filter_main_df(
            account_level=account_level,
            first_period=first_period
        )
        expression = '''
            account_code.str.startswith("2.01") or \
            account_code.str.startswith("2.02")
        '''
        df.query(expression, inplace=True)
        return self._make_report(df)

    def equity(
            self,
            account_level: int = 0,
            first_period: str = '2009-01-01'
        ) -> pd.DataFrame:
        """
        Return corporation equity.

        account_level (int, optional): detail level to show for account codes
            account_level = 0 -> X...       (default: show all levels)
            account_level = 2 -> X.YY       (show 2 levels)
            account_level = 3 -> X.YY.ZZ    (show 3 levels)
            account_level = 4 -> X.YY.ZZ.WW (show 4 levels)
        first_period: first accounting period in YYYY-MM-DD format to show
        """
        df = self._filter_main_df(
            account_level=account_level,
            first_period=first_period
        )
        df.query('account_code.str.startswith("2.03")', inplace=True)
        return self._make_report(df)

    def earnings_per_share(
            self,
            account_level: int = 0,
            first_period: str = '2009-01-01'
        ) -> pd.DataFrame:
        """
        Return corporation Earnings per Share (EPS).
        3.99                -> EPS (BRL / Share)
            3.99.01         -> EPS
                3.99.01.01  -> Stock Type
            3.99.02         -> Diluted EPS
                3.99.02.01  -> Stock Type

        account_level (int, optional): detail level to show for account codes
            account_level = 0 -> X...       (default: show all levels)
            account_level = 2 -> X.YY       (show 2 levels)
            account_level = 3 -> X.YY.ZZ    (show 3 levels)
            account_level = 4 -> X.YY.ZZ.WW (show 4 levels)
        first_period: first accounting period in YYYY-MM-DD format to show
        """
        df = self._filter_main_df(
            account_level=account_level,
            first_period=first_period
        )
        expression = '''
            account_code == "3.99.01.01" or \
            account_code == "3.99.02.01"
        '''
        df.query(expression, inplace=True)

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

    def income(
            self,
            account_level: int = 0,
            first_period: str = '2009-01-01'
        ) -> pd.DataFrame:
        """
        Return corporation income statement.

        account_level (int, optional): detail level to show for account codes
            account_level = 0 -> X...       (default: show all levels)
            account_level = 2 -> X.YY       (show 2 levels)
            account_level = 3 -> X.YY.ZZ    (show 3 levels)
            account_level = 4 -> X.YY.ZZ.WW (show 4 levels)
        first_period: first accounting period in YYYY-MM-DD format to show
        """
        df = self._filter_main_df(
            account_level=account_level,
            first_period=first_period
        )
        df.query('account_code.str.startswith("3")', inplace=True)
        df = self._calculate_ltm(df)
        return self._make_report(df)

    def cash_flow(
            self,
            account_level: int = 0,
            first_period: str = '2009-01-01'
        ) -> pd.DataFrame:
        """
        Return corporation cash flow statement.

        account_level (int, optional): detail level to show for account codes
            account_level = 0 -> X...       (default: show all levels)
            account_level = 2 -> X.YY       (show 2 levels)
            account_level = 3 -> X.YY.ZZ    (show 3 levels)
            account_level = 4 -> X.YY.ZZ.WW (show 4 levels)
        first_period: first accounting period in YYYY-MM-DD format to show
        """
        df = self._filter_main_df(
            account_level=account_level,
            first_period=first_period
        )
        df.query('account_code.str.startswith("6")', inplace=True)
        df = self._calculate_ltm(df)
        return self._make_report(df)

    @staticmethod
    def shift_right(s: pd.Series, is_on: bool) -> pd.Series:
        """Shift row to the right in order to obtain series previous values"""
        if is_on:
            arr = s.iloc[:-1].values
            return np.append(np.nan, arr)
        else:
            return s

    def operating_performance(self, is_on: bool = True):
        """Return corporation main operating indicators."""
        df = self._filter_main_df()
        df_as = self.assets
        # df_as.query('account_code == "1"', inplace=True)
        df_le = self.liabilities_and_equity
        # df_le.query('account_code == "2.03"', inplace=True)
        df_in = self.income
        # df_in.query('account_code == "3.11"', inplace=True)
        df = pd.concat([df_as, df_le, df_in], ignore_index=True)
        df.set_index(keys='account_code', drop=True, inplace=True)
        df.drop(columns=['account_fixed', 'account_name'], inplace=True)

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

    def get_accounts(self, accounts: list) -> pd.DataFrame:
        df_as = self.assets()
        df_le = self.liabilities_and_equity()
        df_is = self.income()
        df_cf = self.cash_flow()
        df = pd.concat([df_as, df_le, df_is, df_cf], ignore_index=True)
        df.query('account_code == @accounts', inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

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
            df_year.rename(columns={'account_value': period_str}, inplace=True)
            df_report = pd.merge(
                df_report,
                df_year,
                how='left',
                on=['account_code']
            )

        df_report.sort_values('account_code', ignore_index=True, inplace=True)
        return df_report
