"""Module containing the Company Class."""
import os
from typing import Union
import numpy as np
import pandas as pd


class Company():
    """
    Finance Data Class for Brazilian Companies.

    Attributes
    ----------
    identity: int or str
        A unique value to select the company in dataset. Both
        CVM ID or a Fiscal ID can be used. CVM ID (regulator ID) must be an
        integer. Fiscal ID must be a string in 'XX.XXX.XXX/XXXX-XX' format.
    """
    script_dir = os.path.dirname(__file__)
    DATASET = pd.read_pickle(script_dir + '/data/processed/dataset.pkl.zst')
    TAX_RATE = 0.34
    CVM_IDS = list(DATASET['cvm_id'].unique())
    FISCAL_IDS = list(DATASET['fiscal_id'].unique())

    def __init__(self, identity: Union[int, str]):
        """Initialize main variables.

        Parameters
        ----------
        identity: int or str
            A unique value to select the company in dataset. Both
            CVM ID or a Fiscal ID can be used. CVM ID (regulator ID) must be an
            integer. Fiscal ID must be a string in 'XX.XXX.XXX/XXXX-XX' format.
        """
        self.identity = identity

    @property
    def identity(self):
        """
        Get or set company unique identity for dataset selection.

        Parameters
        ----------
        value: int or str
            A unique value to select the company in dataset. Both
            CVM ID or a Fiscal ID can be used. CVM ID (regulator ID) must be an
            integer. Fiscal ID must be a string in 'XX.XXX.XXX/XXXX-XX' format.

        Returns
        -------
        int or str

        Raises
        ------
        KeyError
            * If passed ``identity`` not found in dataset.
        """
        return self._identity

    @identity.setter
    def identity(self, value):
        self._identity = value
        if value in Company.CVM_IDS:
            self._cvm_id = value
            df = Company.DATASET.query('cvm_id == @self._cvm_id').copy()
            df.reset_index(drop=True, inplace=True)
            self._fiscal_id = df.loc[0, 'fiscal_id']
        elif value in Company.FISCAL_IDS:
            self._fiscal_id = value
            expression = 'fiscal_id == @self._fiscal_id'
            df = Company.DATASET.query(expression).copy()
            df.reset_index(drop=True, inplace=True)
            self._cvm_id = df.loc[0, 'cvm_id']
        else:
            raise KeyError("Identity for the company not found in dataset")
        # Only set company data after object identity validation
        self._set_main_data()

    def _set_main_data(self) -> pd.DataFrame:
        self._CO_DF = Company.DATASET.query(
            'cvm_id == @self._cvm_id').copy()
        self._CO_DF = self._CO_DF.astype({
            'co_name': str,
            'cvm_id': np.uint32,
            'fiscal_id': str,
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
        self._CO_DF.sort_values(
            by='account_code', ignore_index=True, inplace=True)

        self._NAME = self._CO_DF['co_name'].unique()[0]
        self._FIRST_ANNUAL = self._CO_DF.query(
            'report_type == "annual"')['period_end'].min()
        self._LAST_ANNUAL = self._CO_DF.query(
            'report_type == "annual"')['period_end'].max()
        self._LAST_QUARTERLY = self._CO_DF.query(
            'report_type == "quarterly"')['period_end'].max()

    def info(self) -> pd.DataFrame:
        """Return dictionary with company info."""
        f = '%Y-%m-%d'
        company_info = {
            'Company Name': self._NAME,
            'Company CVM ID': self._cvm_id,
            'Company Fiscal ID (CNPJ)': self._fiscal_id,
            'First Annual Report': self._FIRST_ANNUAL.strftime(f),
            'Last Annual Report': self._LAST_ANNUAL.strftime(f),
            'Last Quarterly Report': self._LAST_QUARTERLY.strftime(f),
        }
        df = pd.DataFrame.from_dict(
            company_info, orient='index', columns=['Company Info'])
        return df

    def report(
        self,
        report_type: str,
        accounting_method: str = 'consolidated',
        unit: float = 1.0,
        account_level: Union[int, None] = None,
        first_period: str = '2009-01-01'
    ) -> pd.DataFrame:
        """
        Return a DataFrame with company selected report type.

        This function generates a report representing one of the financial
        statements for the company adjusted by the attributes passed and
        returns a pandas.DataFrame with this report.

        Parameters
        ----------
        report_type : {'assets', 'liabilities_and_equity', 'liabilities',
            'equity', 'income', 'cash_flow'}
            Report type to be generated.
        accounting_method : {'consolidated', 'separate'}, default
            'consolidated'
            Accounting method used for registering investments.
        account_level : {None, 2, 3, 4}, default None
            Detail level to show for account codes.
            account_level = None -> X...       (default: show all accounts)
            account_level = 2    -> X.YY       (show 2 levels)
            account_level = 3    -> X.YY.ZZ    (show 3 levels)
            account_level = 4    -> X.YY.ZZ.WW (show 4 levels)
        first_period: str, default '2009-01-01'
            First accounting period to show. Format must be YYYY-MM-DD.
        unit : float, default 1.0
            Account values will be divided by 'unit' value.

        Returns
        ------
        pandas.DataFrame

        Raises
        ------
        ValueError
            * If ``report_type`` attribute is invalid
            * If ``accounting_method`` attribute is invalid
            * If ``account_level`` attribute is invalid
            * If ``first_period`` attribute is not in YYYY-MM-DD string format
            * If ``unit`` <= 0

        """
        # Check input arguments.
        if account_level not in {None, 2, 3, 4}:
            raise ValueError(
                "account_level expects None, 2, 3 or 4")

        first_period = pd.to_datetime(first_period, errors='coerce')
        if first_period == pd.NaT:
            raise ValueError(
                'first_period expects a string in YYYY-MM-DD format')

        if accounting_method not in {'consolidated', 'separate'}:
            raise ValueError(
                "accounting_method expects 'consolidated' or 'separate'")

        if unit <= 0:
            raise ValueError("Unit expects a value greater than 0")

        expression = '''
            accounting_method == @accounting_method and \
            period_end >= @first_period
        '''
        df = self._CO_DF.query(expression).copy()

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

        """
        Filter dataframe for selected report_type (report type)

        df['account_code'].str[0].unique() -> [1, 2, 3, 4, 5, 6, 7]
        The first part of 'account_code' is the report type
        Table of reports correspondence:
            1 -> Balance Sheet - Assets
            2 -> Balance Sheet - Liabilities and Shareholders’ Equity
            3 -> Income
            4 -> Comprehensive Income
            5 -> Changes in Equity
            6 -> Cash Flow (Indirect Method)
            7 -> Added Value
        """
        report_types = {
            "assets": ["1"],
            "cash": ["1.01.01", "1.01.02"],
            "current_assets": ["1.01"],
            "non_current_assets": ["1.02"],
            "liabilities": ["2.01", "2.02"],
            "debt": ["2.01.04", "2.02.01"],
            "current_liabilities": ["2.01"],
            "non_current_liabilities": ["2.02"],
            "liabilities_and_equity": ["2"],
            "equity": ["2.03"],
            "income": ["3"],
            "cash_flow": ["6"],
            "earnings_per_share": ["3.99.01.01", "3.99.02.01"]
        }
        account_codes = report_types[report_type]
        expression = ""
        for count, account_code in enumerate(account_codes):
            if count > 0:
                expression += " or "
            expression += f'account_code.str.startswith("{account_code}")'
        df.query(expression, inplace=True)

        if report_type in {'income', 'cash_flow'}:
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
    def _shift_right(s: pd.Series, t_minus1: bool) -> pd.Series:
        """Shift row to the right in order to obtain series previous values"""
        if t_minus1:
            arr = s.iloc[:-1].values
            return np.append(np.nan, arr)
        else:
            return s

    def custom_report(
        self,
        accounts: list[str],
        accounting_method: str = 'consolidated',
        first_period: str = '2009-01-01',
        unit: float = 1.0
    ) -> pd.DataFrame:
        """
        Return a financial report from custom list of accounting codes

        Creates DataFrame object with a custom list of accounting codes
        adjusted by function attributes

        Parameters
        ----------
        accounts : list[str]
            A list of strings containg accounting codes to be used in report
        accounting_method : {'consolidated', 'separate'}, default
            'consolidated'
            Accounting method used for registering investments.
        first_period: str, default '2009-01-01'
            First accounting period to show. Format must be YYYY-MM-DD.
        unit : float, default 1.0
            Account values will be divided by 'unit' value.

        Returns
        -------
        pandas.DataFrame
        """
        kwargs = {
            'accounting_method': accounting_method,
            'unit': unit,
            'first_period': first_period}
        df_as = self.report('assets', **kwargs)
        df_le = self.report('liabilities_and_equity', **kwargs)
        df_is = self.report('income', **kwargs)
        df_cf = self.report('cash_flow', **kwargs)
        df = pd.concat([df_as, df_le, df_is, df_cf], ignore_index=True)
        df.query('account_code == @accounts', inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

    def operating_performance(
        self,
        accounting_method: str = 'consolidated',
        t_minus1: bool = True
    ) -> pd.DataFrame:
        """
        Return company main operating indicators.

        Creates DataFrame object with company operating indicators as
        described in reference [1]

        Parameters
        ----------
        t_minus1 : bool, default True
            Wheather to divide return measurements by book values from the end
            of the prior year (see Damodaran paper above).
        Returns
        -------
        pandas.Dataframe

        References
        ----------
        .. [1]  Aswath Damodaran, "Return on Capital (ROC), Return on Invested
                Capital (ROIC) and Return on Equity (ROE): Measurement and
                Implications.", 2007,
                https://people.stern.nyu.edu/adamodar/pdfiles/papers/returnmeasures.pdf
        """
        kwargs = {'accounting_method': accounting_method}
        df_as = self.report('assets', **kwargs)
        df_le = self.report('liabilities_and_equity', **kwargs)
        df_in = self.report('income', **kwargs)
        df = pd.concat([df_as, df_le, df_in], ignore_index=True)
        df.set_index(keys='account_code', drop=True, inplace=True)
        df.drop(columns=['account_fixed', 'account_name'], inplace=True)
        df.fillna(0, inplace=True)

        # series with indicators
        revenues = df.loc['3.01']
        gross_profit = df.loc['3.03']
        ebit = df.loc['3.05']
        net_income = df.loc['3.11']
        total_assets = self._shift_right(df.loc['1'], t_minus1)
        equity = self._shift_right(df.loc['2.03'], t_minus1)
        invested_capital = (
            df.loc['2.03']
            + df.loc['2.01.04']
            + df.loc['2.02.01']
            - df.loc['1.01.01']
            - df.loc['1.01.02']
        )
        invested_capital = self._shift_right(invested_capital, t_minus1)

        # dfi: dataframe with indicators
        dfi = pd.DataFrame(columns=df.columns)
        dfi.loc['return_on_assets'] = (
            ebit * (1 - Company.TAX_RATE) / total_assets
        )
        # dfi.loc['invested_capital'] = invested_capital
        dfi.loc['return_on_capital'] = (
            ebit * (1 - Company.TAX_RATE) / invested_capital
        )
        dfi.loc['return_on_equity'] = net_income / equity
        dfi.loc['gross_margin'] = gross_profit / revenues
        dfi.loc['operating_margin'] = ebit * (1 - Company.TAX_RATE) / revenues
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
