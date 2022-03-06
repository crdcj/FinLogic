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
        min_end_period: str = '2009-12-31',
        max_end_period: str = '2200-12-31',
        unit: float = 1
    ):
        """Initialize main variables.

        Args:
            cvm_number (int): CVM unique number of the company.
            report_type (str, optional): 'consolidated' or 'separate'.
        """
        self.cvm_number = cvm_number
        self.report_type = report_type
        self.min_end_period = min_end_period
        self.max_end_period = max_end_period
        self.unit = unit
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
    def min_end_period(self):
        """Return selected start date for filtering FS end period."""
        return self._min_end_period

    @min_end_period.setter
    def min_end_period(self, value):
        value = pd.to_datetime(value, errors='coerce')
        if value == pd.NaT:
            print('Inserted min_end_period period not in YYYY-MM-DD format')
            print('2009-12-31 selected instead')
            self._min_end_period = pd.to_datetime('2009-12-31')
        else:
            print(f"Selected min_end_period = {value.date()}")
            self._min_end_period = value

    @property
    def max_end_period(self):
        """Return selected end date for filtering FS end period."""
        return self._max_end_period

    @max_end_period.setter
    def max_end_period(self, value):
        value = pd.to_datetime(value, errors='coerce')
        if value == pd.NaT:
            print('Inserted max_end_period not in YYYY-MM-DD format')
            print('2200-12-31 selected instead')
            self._max_end_period = pd.to_datetime('2200-12-31')
        else:
            print(f"Selected max_end_period = {value.date()}")
            self._max_end_period = value

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
        self._MAIN_DF['account_level1'] = self._MAIN_DF['CD_CONTA'].str[0]
        self._MAIN_DF['account_level_12'] = self._MAIN_DF['CD_CONTA'].str[0:4]
        self._MAIN_DF.reset_index(drop=True, inplace=True)

    def _get_company_df(self) -> pd.DataFrame:
        query_expression = '''
            report_type == @self._report_type and \
            DT_FIM_EXERC >= @self._min_end_period and \
            DT_FIM_EXERC <= @self._max_end_period
        '''
        df = self._MAIN_DF.query(query_expression).copy()
        # change unit only for accounts different from 3.99
        df['VL_CONTA'] = np.where(
            df['account_level_12'] == '3.99',
            df['VL_CONTA'],
            df['VL_CONTA'] / self._unit
        )
        df.reset_index(drop=True, inplace=True)
        return df

    @property
    def info(self) -> dict:
        """Return company info."""
        annual_reports = self._MAIN_DF.query("report_period == 'annual'")
        annual_reports = annual_reports.DT_REFER.dt.strftime('%Y-%m-%d')
        annual_reports = list(annual_reports.unique())
        last_quarterly_report = self._MAIN_DF.query(
            "report_period == 'quarterly'").DT_REFER.max()
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
        df.query("account_level1 == '1'", inplace=True)
        return self._make_report(df)

    @property
    def liabilities_and_equity(self) -> pd.DataFrame:
        """Return company liabilities and equity."""
        df = self._get_company_df()
        df.query("account_level1 == '2'", inplace=True)
        return self._make_report(df)

    @property
    def liabilities(self) -> pd.DataFrame:
        """Return company liabilities."""
        df = self._get_company_df()
        df.query(
            "account_level_12 == '2.01' or account_level_12 == '2.02'",
            inplace=True
        )
        return self._make_report(df)

    @property
    def equity(self) -> pd.DataFrame:
        """Return company equity."""
        df = self._get_company_df()
        df.query("account_level_12 == '2.03'", inplace=True)
        # df.query("CD_CONTA == '1'", inplace=True)
        return self._make_report(df)

    @property
    def earnings_per_share(self) -> pd.DataFrame:
        """Return company equity."""
        df = self._get_company_df()
        df.query("account_level_12 == '3.99'", inplace=True)
        # df.query("CD_CONTA == '1'", inplace=True)
        return self._make_report(df)

    @staticmethod
    def calculate_ltm(df_flow: pd.DataFrame) -> pd.DataFrame:
        last_annual_statement = df_flow.query(
            'report_period == "annual"')['DT_FIM_EXERC'].max()
        last_quarterly_statement = df_flow.query(
            'report_period == "quarterly"')['DT_FIM_EXERC'].max()
        if last_annual_statement > last_quarterly_statement:
            df_flow.query('report_period == "annual"', inplace=True)
            return df_flow

        df1 = df_flow.query(
            'DT_FIM_EXERC == @last_quarterly_statement'
            ).copy()
        df1.query('DT_INI_EXERC == DT_INI_EXERC.min()', inplace=True)

        df2 = df_flow.query('DT_REFER == @last_quarterly_statement').copy()
        df2.query('DT_INI_EXERC == DT_INI_EXERC.min()', inplace=True)
        df2['VL_CONTA'] = -df2['VL_CONTA']

        df3 = df_flow.query('DT_FIM_EXERC == @last_annual_statement').copy()

        df_ltm = pd.concat([df1, df2, df3], ignore_index=True)
        df_ltm = df_ltm[['CD_CONTA', 'VL_CONTA']]
        df_ltm = df_ltm.groupby(by='CD_CONTA').sum().reset_index()
        df1.drop(columns='VL_CONTA', inplace=True)
        df_ltm = pd.merge(df1, df_ltm)
        df_ltm['report_period'] = 'ltm'
        df_ltm['DT_INI_EXERC'] = (
            last_quarterly_statement - pd.DateOffset(years=1)
        )

        df_flow.query('report_period == "annual"', inplace=True)
        df_flow_ltm = pd.concat([df_flow, df_ltm], ignore_index=True)
        return df_flow_ltm

    @property
    def income(self) -> pd.DataFrame:
        """Return company income statement."""
        df = self._get_company_df()
        df.query("account_level1 == '3'", inplace=True)
        df = Finance.calculate_ltm(df)
        return self._make_report(df)

    @property
    def cash_flow(self) -> pd.DataFrame:
        """Return company income statement."""
        df = self._get_company_df()
        df.query("account_level1 == '6'", inplace=True)
        df = Finance.calculate_ltm(df)
        return self._make_report(df)

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
        df_is = self.income_statement
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
            df_year = df.query("DT_FIM_EXERC == @period").copy()
            df_year = df_year[merge_columns]
            df_year.rename(
                columns={'VL_CONTA': np.datetime_as_string(period, unit='D')},
                inplace=True)
            df_report = pd.merge(df_report, df_year, how='left')

        df_report.sort_values('CD_CONTA', ignore_index=True, inplace=True)
        return df_report
