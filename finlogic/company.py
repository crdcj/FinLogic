"""Module containing the Company Class."""
import os
from typing import Union, Literal
import numpy as np
import pandas as pd

BASE_DIR = os.path.dirname(__file__)
MAIN_DF_PATH = BASE_DIR + '/data/main_df.pkl.zst'


class Company():
    """
    Finance Data Class for listed Brazilian Companies.

    Attributes
    ----------
    identifier: int or str
        A unique identifier to filter a company in as fi. Both CVM
        ID or Fiscal ID can be used. CVM ID (regulator ID) must be an integer.
        Fiscal ID must be a string in 'XX.XXX.XXX/XXXX-XX' format.
    """
    TAX_RATE = 0.34

    def __init__(
        self,
        identifier: Union[int, str],
        acc_method: Literal["consolidated", "separate"] = "consolidated",
        acc_unit: Union[float, str] = 1.0,
    ):
        """Initialize main variables.

        Parameters
        ----------
        identifier: int or str
            A unique identifier to filter a company in as fi.
            Both CVM ID or Fiscal ID can be used.
            CVM ID (regulator ID) must be an integer.
            Fiscal ID must be a string in 'XX.XXX.XXX/XXXX-XX' format.
        acc_method : {'consolidated', 'separate'}, default 'consolidated'
            Accounting method used for registering investments in subsidiaries.
        acc_unit : float or str, default 1.0
            acc_unit is a constant that will divide company account values.
            The constant can be a number greater than zero or the strings
            {'thousand', 'million', 'billion'}.
        """
        self.set_id(identifier)
        self.acc_method = acc_method
        self.acc_unit = acc_unit

    def set_id(self, identifier: Union[int, str]):
        """
        Set a unique identifier to filter the company in as fi.

        Parameters
        ----------
        value: int or str
            A unique identifier to filter a company in as fi.
            Both CVM ID or Fiscal ID can be used.
            CVM ID (regulator ID) must be an integer.
            Fiscal ID must be a string in 'XX.XXX.XXX/XXXX-XX' format.

        Returns
        -------
        int or str

        Raises
        ------
        KeyError
            * If passed ``identifier`` not found in as fi.
        """
        # Create custom data frame for ID selection
        df = pd.read_pickle(MAIN_DF_PATH)
        df = df[['cvm_id', 'fiscal_id']].drop_duplicates()
        df = df.astype({'cvm_id': int, 'fiscal_id': str})
        if identifier in df['cvm_id'].values:
            self._cvm_id = identifier
            self._fiscal_id = (
                df.loc[df['cvm_id'] == identifier, 'fiscal_id'].item())
        elif identifier in df['fiscal_id'].values:
            self._fiscal_id = identifier
            self._cvm_id = (
                df.loc[df['fiscal_id'] == identifier, 'cvm_id'].item())
        else:
            raise KeyError(
                "'identifier' for the company not found in database")
        # Only set company data after object identifier validation
        self._set_main_data()

    @property
    def acc_method(self):
        """
        Get or set accounting method used for registering investments in
        subsidiaries.

        Parameters
        ----------
        acc_method : {'consolidated', 'separate'}, default 'consolidated'
            Accounting method used for registering investments in subsidiaries.

        Returns
        -------
        str

        Raises
        ------
        ValueError
            * If passed ``acc_method`` is invalid.
        """
        return self._acc_unit

    @acc_method.setter
    def acc_method(self, value: Literal["consolidated", "separate"]):
        if value in {'consolidated', 'separate'}:
            self._acc_method = value
        else:
            raise ValueError("acc_method expects 'consolidated' or 'separate'")

    @property
    def acc_unit(self):
        """
        Get or set a constant to divide company account values.

        Parameters
        ----------
        acc_unit : float or str, default 1.0
            acc_unit is a constant that will divide company account values.
            The constant can be a number greater than zero or the strings
            {'thousand', 'million', 'billion'}.

        Returns
        -------
        float

        Raises
        ------
        ValueError
            * If passed ``acc_unit`` is invalid.
        """
        return self._acc_unit

    @acc_unit.setter
    def acc_unit(self, value: Union[float, str]):
        if value == 'thousand':
            self._acc_unit = 1_000
        elif value == 'million':
            self._acc_unit = 1_000_000
        elif value == 'billion':
            self._acc_unit = 1_000_000_000
        elif value >= 0:
            self._acc_unit = value
        else:
            raise ValueError("Accounting Unit is invalid")

    def _set_main_data(self) -> pd.DataFrame:
        self._COMP_DF = pd.read_pickle(MAIN_DF_PATH)
        self._COMP_DF.query('cvm_id == @self._cvm_id', inplace=True)
        self._COMP_DF = self._COMP_DF.astype({
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
        self._COMP_DF.sort_values(
            by='acc_code', ignore_index=True, inplace=True)
        self._NAME = self._COMP_DF['co_name'].unique()[0]
        self._FIRST_ANNUAL = self._COMP_DF.query(
            'report_type == "annual"')['period_end'].min()
        self._LAST_ANNUAL = self._COMP_DF.query(
            'report_type == "annual"')['period_end'].max()
        self._LAST_QUARTERLY = self._COMP_DF.query(
            'report_type == "quarterly"')['period_end'].max()

    def info(self) -> pd.DataFrame:
        """Return dictionary with company info."""
        company_info = {
            'Company Name': self._NAME,
            'Company CVM ID': self._cvm_id,
            'Company Fiscal ID (CNPJ)': self._fiscal_id,
            'Company total accounting rows': len(self._COMP_DF.index),
            'Selected Accounting Method': self._acc_method,
            'Selected Accounting Unit': self._acc_unit,
            'First Annual Report': self._FIRST_ANNUAL.strftime('%Y-%m-%d'),
            'Last Annual Report': self._LAST_ANNUAL.strftime('%Y-%m-%d'),
            'Last Quarterly Report': self._LAST_QUARTERLY.strftime('%Y-%m-%d'),
        }
        df = pd.DataFrame.from_dict(
            company_info, orient='index', columns=['Company Info'])
        return df

    def report(
        self,
        report_type: str,
        acc_level: Union[int, None] = None,
        num_years: int = 0,
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
        acc_level : {None, 2, 3, 4}, default None
            Detail level to show for account codes.
            acc_level = None -> X...       (default: show all accounts)
            acc_level = 2    -> X.YY       (show 2 levels)
            acc_level = 3    -> X.YY.ZZ    (show 3 levels)
            acc_level = 4    -> X.YY.ZZ.WW (show 4 levels)
        num_years : int, default 0
            Select how many last years to show where 0 -> show all years

        Returns
        ------
        pandas.DataFrame

        Raises
        ------
        ValueError
            * If ``report_type`` attribute is invalid
            * If ``acc_level`` attribute is invalid
        """
        # Check input arguments.
        if acc_level not in {None, 2, 3, 4}:
            raise ValueError(
                "acc_level expects None, 2, 3 or 4")

        expression = 'acc_method == @self._acc_method'
        df = self._COMP_DF.query(expression).copy()
        # Change acc_unit only for accounts different from 3.99
        df['acc_value'] = np.where(
            df['acc_code'].str.startswith('3.99'),
            df['acc_value'],
            df['acc_value'] / self._acc_unit
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
            "earnings_per_share": ["3.99.01.01", "3.99.02.01"],
            "comprehensive_income": ["4"],
            "changes_in_equity": ["5"],
            "cash_flow": ["6"],
            "added_value": ["7"],
        }
        acc_codes = report_types[report_type]
        expression = ""
        for count, acc_code in enumerate(acc_codes):
            if count > 0:
                expression += " or "
            expression += f'acc_code.str.startswith("{acc_code}")'
        df.query(expression, inplace=True)

        if report_type in {'income', 'cash_flow'}:
            df = self._calculate_ttm(df)

        df.reset_index(drop=True, inplace=True)

        report_df = self._make_report(df)
        # Show only the selected number of years
        if num_years > 0:
            cols = report_df.columns.to_list()
            cols = cols[0:3] + cols[-num_years:]
            report_df = report_df[cols]
        return report_df

    def _calculate_ttm(self, df_flow: pd.DataFrame) -> pd.DataFrame:
        if self._LAST_ANNUAL > self._LAST_QUARTERLY:
            df_flow.query('report_type == "annual"', inplace=True)
            return df_flow

        df1 = df_flow.query('period_end == @self._LAST_QUARTERLY').copy()
        df1.query('period_begin == period_begin.min()', inplace=True)

        df2 = df_flow.query('period_reference == @self._LAST_QUARTERLY').copy()
        df2.query('period_begin == period_begin.min()', inplace=True)
        df2['acc_value'] = -df2['acc_value']

        df3 = df_flow.query('period_end == @self._LAST_ANNUAL').copy()

        df_ttm = pd.concat([df1, df2, df3], ignore_index=True)
        df_ttm = df_ttm[['acc_code', 'acc_value']]
        df_ttm = df_ttm.groupby(by='acc_code').sum().reset_index()
        df1.drop(columns='acc_value', inplace=True)
        df_ttm = pd.merge(df1, df_ttm)
        df_ttm['report_type'] = 'quarterly'
        df_ttm['period_begin'] = self._LAST_QUARTERLY - pd.DateOffset(years=1)

        df_flow.query('report_type == "annual"', inplace=True)
        df_flow_ttm = pd.concat([df_flow, df_ttm], ignore_index=True)
        return df_flow_ttm

    def custom_report(
        self,
        acc_list: list[str],
        num_years: int = 0,
    ) -> pd.DataFrame:
        """
        Return a financial report from custom list of accounting codes

        Creates DataFrame object with a custom list of accounting codes
        adjusted by function attributes

        Parameters
        ----------
        acc_list : list[str]
            A list of strings containg accounting codes to be used in report
        num_years : int, default 0
            Select how many last years to show where 0 -> show all years

        Returns
        -------
        pandas.DataFrame
        """
        df_as = self.report('assets')
        df_le = self.report('liabilities_and_equity')
        df_is = self.report('income')
        df_cf = self.report('cash_flow')
        df = pd.concat([df_as, df_le, df_is, df_cf], ignore_index=True)
        df.query('acc_code == @acc_list', inplace=True)
        df.reset_index(drop=True, inplace=True)
        # Show only the selected number of years
        if num_years > 0:
            cols = df.columns.to_list()
            cols = cols[0:3] + cols[-num_years:]
            df = df[cols]
        return df

    @staticmethod
    def _prior_values(s: pd.Series, is_prior: bool) -> pd.Series:
        """Shift row to the right in order to obtain series previous values"""
        if is_prior:
            arr = s.iloc[:-1].values
            return np.append(np.nan, arr)
        else:
            return s

    def indicators(
        self,
        num_years: int = 0,
        is_prior: bool = True
    ) -> pd.DataFrame:
        """
        Return company main operating indicators.

        Creates DataFrame object with company operating indicators as
        described in reference [1]

        Parameters
        ----------
        num_years : int, default 0
            Select how many last years to show where 0 -> show all years
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
        df_as = self.report('assets')
        df_le = self.report('liabilities_and_equity')
        df_in = self.report('income')
        df_cf = self.report('cash_flow')
        df = pd.concat([df_as, df_le, df_in, df_cf], ignore_index=True)
        df.set_index(keys='acc_code', drop=True, inplace=True)
        df.drop(columns=['acc_fixed', 'acc_name'], inplace=True)
        # Calculate indicators series
        revenues = df.loc['3.01']
        gross_profit = df.loc['3.03']
        ebit = df.loc['3.05']
        depreciation_amortization = df.loc['6.01.01.04']
        ebitda = ebit + depreciation_amortization
        operating_cash_flow = df.loc['6.01']
        net_income = df.loc['3.11']
        total_assets = df.loc['1']
        total_assets_p = self._prior_values(total_assets, is_prior)
        equity = df.loc['2.03']
        equity_p = self._prior_values(equity, is_prior)
        total_cash = df.loc['1.01.01'] + df.loc['1.01.02']
        current_assets = df.loc['1.01']
        current_liabilities = df.loc['2.01']
        working_capital = current_assets - current_liabilities
        total_debt = df.loc['2.01.04'] + df.loc['2.02.01']
        net_debt = total_debt - total_cash
        invested_capital = total_debt + equity - total_cash
        invested_capital_p = self._prior_values(invested_capital, is_prior)
        # dfi: dataframe with indicators
        dfi = pd.DataFrame(columns=df.columns)
        dfi.loc['revenues'] = revenues
        dfi.loc['operating_cash_flow'] = operating_cash_flow
        dfi.loc['ebitda'] = ebitda
        dfi.loc['ebit'] = ebit
        dfi.loc['net_income'] = net_income
        dfi.loc['total_cash'] = total_cash
        dfi.loc['total_debt'] = total_debt
        dfi.loc['net_debt'] = net_debt
        dfi.loc['working_capital'] = working_capital
        dfi.loc['invested_capital'] = invested_capital
        dfi.loc['return_on_assets'] = (
            ebit * (1 - Company.TAX_RATE) / total_assets_p)
        dfi.loc['return_on_capital'] = (
            ebit * (1 - Company.TAX_RATE) / invested_capital_p)
        dfi.loc['return_on_equity'] = net_income / equity_p
        dfi.loc['gross_margin'] = gross_profit / revenues
        dfi.loc['ebitda_margin'] = ebitda / revenues
        dfi.loc['operating_margin'] = ebit * (1 - Company.TAX_RATE) / revenues
        dfi.loc['net_margin'] = net_income / revenues
        # Show only the selected number of years
        if num_years > 0:
            dfi = dfi[dfi.columns[-num_years:]]
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
            df_year = df.query('period_end == @period').copy()
            df_year = df_year[['acc_value', 'acc_code']]
            period_str = np.datetime_as_string(period, unit='D')
            if period == self._LAST_QUARTERLY:
                period_str += ' (ttm)'
            df_year.rename(columns={'acc_value': period_str}, inplace=True)
            df_report = pd.merge(
                df_report, df_year, how='left', on=['acc_code']
            )
        df_report.sort_values('acc_code', ignore_index=True, inplace=True)
        return df_report
