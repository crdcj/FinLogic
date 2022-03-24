"""Module containing the Company Class."""
import os
from typing import Union
import numpy as np
import pandas as pd


class Company():
    """
    Finance Data Class for listed Brazilian Companies.

    Attributes
    ----------
    identity: int or str
        A unique value to select the company in dataset. Both CVM ID or
        Fiscal ID can be used. CVM ID (regulator ID) must be an integer.
        Fiscal ID must be a string in 'XX.XXX.XXX/XXXX-XX' format.
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
            A unique value to select the company in dataset. Both CVM ID or
            Fiscal ID can be used. CVM ID (regulator ID) must be an integer.
            Fiscal ID must be a string in 'XX.XXX.XXX/XXXX-XX' format.
        """
        self.identity = identity

    @property
    def identity(self):
        """
        Get or set company unique identity for dataset selection.

        Parameters
        ----------
        value: int or str
            A unique value to select the company in dataset. Both CVM ID or
            Fiscal ID can be used. CVM ID (regulator ID) must be an integer.
            Fiscal ID must be a string in 'XX.XXX.XXX/XXXX-XX' format.

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
            'acc_code': str,
            'acc_name': str,
            'acc_method': str,
            'acc_fixed': bool,
            'acc_value': float,
            'equity_statement_column': str,
        })
        self._CO_DF.sort_values(
            by='acc_code', ignore_index=True, inplace=True)

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
        acc_method: str = 'consolidated',
        acc_unit: float = 1.0,
        acc_level: Union[int, None] = None,
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
        acc_method : {'consolidated', 'separate'}, default
            'consolidated'
            Accounting method used for registering investments.
        acc_level : {None, 2, 3, 4}, default None
            Detail level to show for account codes.
            acc_level = None -> X...       (default: show all accounts)
            acc_level = 2    -> X.YY       (show 2 levels)
            acc_level = 3    -> X.YY.ZZ    (show 3 levels)
            acc_level = 4    -> X.YY.ZZ.WW (show 4 levels)
        first_period: str, default '2009-01-01'
            First accounting period to show. Format must be YYYY-MM-DD.
        acc_unit : float, default 1.0
            Account values will be divided by 'acc_unit' value.

        Returns
        ------
        pandas.DataFrame

        Raises
        ------
        ValueError
            * If ``report_type`` attribute is invalid
            * If ``acc_method`` attribute is invalid
            * If ``acc_level`` attribute is invalid
            * If ``first_period`` attribute is not in YYYY-MM-DD string format
            * If ``acc_unit`` <= 0

        """
        # Check input arguments.
        if acc_level not in {None, 2, 3, 4}:
            raise ValueError(
                "acc_level expects None, 2, 3 or 4")

        first_period = pd.to_datetime(first_period, errors='coerce')
        if first_period == pd.NaT:
            raise ValueError(
                'first_period expects a string in YYYY-MM-DD format')

        if acc_method not in {'consolidated', 'separate'}:
            raise ValueError(
                "acc_method expects 'consolidated' or 'separate'")

        if acc_unit <= 0:
            raise ValueError("acc_Unit expects a value greater than 0")

        expression = '''
            acc_method == @acc_method and \
            period_end >= @first_period
        '''
        df = self._CO_DF.query(expression).copy()

        # Change acc_unit only for accounts different from 3.99
        df['acc_value'] = np.where(
            df['acc_code'].str.startswith('3.99'),
            df['acc_value'],
            df['acc_value'] / acc_unit
        )

        # Filter dataframe for selected acc_level
        if acc_level:
            acc_code_limit = acc_level * 3 - 2 # noqa
            expression = 'acc_code.str.len() <= @acc_code_limit'
            df.query(expression, inplace=True)

        """
        Filter dataframe for selected report_type (report type)

        df['acc_code'].str[0].unique() -> [1, 2, 3, 4, 5, 6, 7]
        The first part of 'acc_code' is the report type
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
        acc_codes = report_types[report_type]
        expression = ""
        for count, acc_code in enumerate(acc_codes):
            if count > 0:
                expression += " or "
            expression += f'acc_code.str.startswith("{acc_code}")'
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
        df2['acc_value'] = -df2['acc_value']

        df3 = df_flow.query('period_end == @self._LAST_ANNUAL').copy()

        df_ltm = pd.concat([df1, df2, df3], ignore_index=True)
        df_ltm = df_ltm[['acc_code', 'acc_value']]
        df_ltm = df_ltm.groupby(by='acc_code').sum().reset_index()
        df1.drop(columns='acc_value', inplace=True)
        df_ltm = pd.merge(df1, df_ltm)
        df_ltm['report_type'] = 'quarterly'
        df_ltm['period_begin'] = self._LAST_QUARTERLY - pd.DateOffset(years=1)

        df_flow.query('report_type == "annual"', inplace=True)
        df_flow_ltm = pd.concat([df_flow, df_ltm], ignore_index=True)
        return df_flow_ltm

    def custom_report(
        self,
        acc_list: list[str],
        acc_method: str = 'consolidated',
        first_period: str = '2009-01-01',
        acc_unit: float = 1.0
    ) -> pd.DataFrame:
        """
        Return a financial report from custom list of accounting codes

        Creates DataFrame object with a custom list of accounting codes
        adjusted by function attributes

        Parameters
        ----------
        acc_list : list[str]
            A list of strings containg accounting codes to be used in report
        acc_method : {'consolidated', 'separate'}, default
            'consolidated'
            Accounting method used for registering investments.
        first_period: str, default '2009-01-01'
            First accounting period to show. Format must be YYYY-MM-DD.
        acc_unit : float, default 1.0
            Account values will be divided by 'acc_unit' value.

        Returns
        -------
        pandas.DataFrame
        """
        kwargs = {
            'acc_method': acc_method,
            'acc_unit': acc_unit,
            'first_period': first_period}
        df_as = self.report('assets', **kwargs)
        df_le = self.report('liabilities_and_equity', **kwargs)
        df_is = self.report('income', **kwargs)
        df_cf = self.report('cash_flow', **kwargs)
        df = pd.concat([df_as, df_le, df_is, df_cf], ignore_index=True)
        df.query('acc_code == @acc_list', inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

    @staticmethod
    def _prior_values(s: pd.Series, is_prior: bool) -> pd.Series:
        """Shift row to the right in order to obtain series previous values"""
        if is_prior:
            arr = s.iloc[:-1].values
            return np.append(np.nan, arr)
        else:
            return s

    @staticmethod
    def _acc_values(df: pd.DataFrame, acc: str) -> pd.Series:
        """
        Return account values or null if account not found

        Some companies have less accounts if separate financial statements is
        selected, as for accounts '3.01' and '3.03'
        """
        if acc in df.index:
            s = df.loc[acc]
        else:
            s = pd.Series(data=np.NAN, index=df.columns)
        return s

    def indicators(
        self,
        acc_method: str = 'consolidated',
        acc_unit: float = 1.0,
        is_prior: bool = True
    ) -> pd.DataFrame:
        """
        Return company main operating indicators.

        Creates DataFrame object with company operating indicators as
        described in reference [1]

        Parameters
        ----------
        is_prior : bool, default True
            Divide return measurements by book values from the end of the prior
            year (see Damodaran reference).
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
        kwargs = {'acc_method': acc_method, 'acc_unit': acc_unit}
        df_as = self.report('assets', **kwargs)
        df_le = self.report('liabilities_and_equity', **kwargs)
        df_in = self.report('income', **kwargs)
        df = pd.concat([df_as, df_le, df_in], ignore_index=True)
        df.set_index(keys='acc_code', drop=True, inplace=True)
        df.drop(columns=['acc_fixed', 'acc_name'], inplace=True)
        df.fillna(0, inplace=True)

        # series with indicators
        revenues = self._acc_values(df, '3.01')
        gross_profit = self._acc_values(df, '3.03')

        ebit = df.loc['3.05']
        net_income = df.loc['3.11']
        total_assets = df.loc['1']
        total_assets_tm1 = self._prior_values(total_assets, is_prior)
        equity = df.loc['2.03']
        equity_tm1 = self._prior_values(equity, is_prior)
        invested_capital = (
            df.loc['2.03']
            + df.loc['2.01.04']
            + df.loc['2.02.01']
            - df.loc['1.01.01']
            - df.loc['1.01.02']
        )
        invested_capital_tm1 = self._prior_values(invested_capital, is_prior)

        # dfi: dataframe with indicators
        dfi = pd.DataFrame(columns=df.columns)
        dfi.loc['invested_capital'] = invested_capital
        dfi.loc['return_on_assets'] = (
            ebit * (1 - Company.TAX_RATE) / total_assets_tm1
        )
        # dfi.loc['invested_capital'] = invested_capital
        dfi.loc['return_on_capital'] = (
            ebit * (1 - Company.TAX_RATE) / invested_capital_tm1
        )
        dfi.loc['return_on_equity'] = net_income / equity_tm1
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
        base_columns = ['acc_name', 'acc_code', 'acc_fixed']
        df_report = df.loc[:, base_columns]
        df_report.drop_duplicates(
            subset='acc_code', ignore_index=True, inplace=True, keep='last'
        )

        periods = list(df.period_end.unique())
        periods.sort()
        for period in periods:
            # print(date)
            df_year = df.query('period_end == @period').copy()
            df_year = df_year[['acc_value', 'acc_code']]
            period_str = np.datetime_as_string(period, unit='D')
            if period == self._LAST_QUARTERLY:
                period_str += ' (ltm)'

            df_year.rename(columns={'acc_value': period_str}, inplace=True)
            df_report = pd.merge(
                df_report, df_year, how='left', on=['acc_code']
            )

        df_report.sort_values('acc_code', ignore_index=True, inplace=True)
        return df_report
