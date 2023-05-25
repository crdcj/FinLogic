"""This module contains the Company class, which provides methods for obtaining
financial reports and calculating financial indicators for a company. Users can
set the accounting method, unit, tax rate, and language for the company object.

Classes:
    Company: Represents a company with its financial information and allows
    users to generate financial reports and indicators.

Main variables:
    cfg.DF  memory dataframe with all accounting data
    _df     company dataframe (protected)
    dfi     function input dataframe
    dfo     function output dataframe

RuntimeWarning:
    Pandas query + numexpr module -> Engine has switched to 'python' because
    numexpr does not support extension array dtypes (category not supported
    yet). Please set your engine to python manually. That means that the query
    method has to use engine='python' to work with category dtype. N.B. Only
    FinLogic Dataframe uses category dtype for efficiency.

"""
from typing import Literal
import numpy as np
import pandas as pd
from .language import language_df
from .frich import print_dict
from . import data_manager as dm


class Company:
    """A class to represent a company financial data.

     This class provides methods to create financial reports and to calculate
     financial indicators based on a company's accounting data. The class also
     has an AI generated dictionary to translate from Portuguese to English.

    Attributes:
         identifier: A unique identifier for the company. Both CVM ID (int) and
            Fiscal ID (str) can be used.
         is_consolidated: The accounting methods can be either 'con' for consolidated or
            'sep' for separate. Defaults to 'con' (str).
         acc_unit: The accounting unit for the financial statements where "t"
            represents thousands, "m" represents millions and "b" represents
            billions (int, float or str). Defaults to 1.
         tax_rate: The tax rate for the company. Defaults to 0.34, which is
            the standard corporate tax rate in Brazil (float).
         language: The language for the financial reports. Options are "english"
            or "portuguese". Defaults to "english" (str).

     Methods:
         report: Creates a financial report for the company.
         custom_report: Creates a custom financial report for the company.
         indicators: Calculates the financial indicators of the company.

     Raises:
         ValueError: If the input arguments are invalid.
    """

    def __init__(
        self,
        identifier: int | str,
        is_consolidated: bool = True,
        acc_unit: int | float | Literal["t", "m", "b"] = 1,
        tax_rate: float = 0.34,
        language: Literal["english", "portuguese"] = "english",
    ):
        """Initializes a new instance of the Company class."""
        self._initialized = False
        self.identifier = identifier
        self.is_consolidated = is_consolidated
        self.acc_unit = acc_unit
        self.tax_rate = tax_rate
        self.language = language
        self._initialized = True
        # Only set _df after identifier, is_consolidated and acc_unit are setted
        self._set_df()

    @property
    def identifier(self) -> int | str:
        """Set a unique identifier to select the company in FinLogic Database.

        This method sets the company's CVM and fiscal ID based on a given
        identifier. The identifier can be either the CVM ID or the Fiscal ID
        (CNPJ). If the identifier is not found in the database, a KeyError is
        raised.

        Args:
            identifier: A unique identifier to select a company in FinLogic
                Database. Both CVM ID or Fiscal ID can be used. CVM ID
                (regulator number) must be an integer. Fiscal ID must be a
                string in 'XX.XXX.XXX/XXXX-XX' format.

        Returns:
            None

        Raises:
            KeyError: If the given identifier isn't found in Finlogic Database.
        """
        return self._identifier

    @identifier.setter
    def identifier(self, identifier: int | str):
        # Create custom data frame for ID selection
        df = (
            dm.get_main_df()[["cvm_id", "tax_id", "name_id"]]
            .query("cvm_id == @identifier or tax_id == @identifier")
            .drop_duplicates(ignore_index=True)
        )
        if not df.empty:
            self._cvm_id = df.loc[0, "cvm_id"]
            self.tax_id = df.loc[0, "tax_id"]
            self.name_id = df.loc[0, "name_id"]
            self._identifier = identifier
        else:
            raise KeyError(f"Company 'identifier' {identifier} not found.")
        # If object was already initialized, reset company dataframe
        if self._initialized:
            self._set_df()

    @property
    def is_consolidated(self) -> bool:
        """Gets or sets the accounting method for registering investments in
        subsidiaries.

        The "is_consolidated" must be True for consolidated or False for separate.
        Consolidated accounting combines the financial statements of a parent
        company and its subsidiaries, while separate accounting keeps them
        separate. Defaults to 'consolidated'.

        Raises:
            ValueError: If the accounting method is invalid.
        """
        return self._acc_unit

    @is_consolidated.setter
    def is_consolidated(self, value: bool):
        if value:
            self._is_consolidated = True
        else:
            self._is_consolidated = False
        # If object was already initialized, reset company dataframe
        if self._initialized:
            self._set_df()

    @property
    def acc_unit(self) -> float:
        """Gets or sets the accounting unit for the financial statements.

        The "acc_unit" is a constant that will divide all company
        accounting values. The constant must be a number greater than
        zero or one of the following strings:
            - "t" to represent thousands       (1,000)
            - "m" to represent millions     (1,000,000)
            - "b" to represent billions (1,000,000,000)

        Returns:
            The current accounting unit.

        Raises:
            ValueError: If the accounting unit is invalid.

        Examples:
            To set the accounting unit to millions:
                company.acc_unit = "m"

            To set the accounting unit to a custom factor, e.g., 10,000:
                company.acc_unit = 10_000
        """
        return self._acc_unit

    @acc_unit.setter
    def acc_unit(self, value: int | float | Literal["t", "m", "b"]):
        match value:
            case "t":
                self._acc_unit = 1_000
            case "m":
                self._acc_unit = 1_000_000
            case "b":
                self._acc_unit = 1_000_000_000
            case str():  # Add this case to catch invalid strings
                raise ValueError("Invalid string for Accounting Unit")
            case v if v > 0:
                self._acc_unit = float(v)
            case _:
                raise ValueError("Accounting Unit is invalid")

        # If object was already initialized, reset company dataframe
        if self._initialized:
            self._set_df()

    @property
    def tax_rate(self) -> float:
        """Gets or sets company 'tax_rate' property.

        The "tax_rate" is used to calculate some of the company
        indicators, such as EBIT and net income. The default value
        is 0.34, which is the standard corporate tax rate in Brazil.

        Returns:
            The tax rate value.

        Raises:
            ValueError: If the tax rate is not a float between 0 and 1.

        Examples:
            To set the tax rate to 21%:
                company.tax_rate = 0.21

            To set the tax rate to 0% (tax exempt):
                company.tax_rate = 0.0
        """
        return self._tax_rate

    @tax_rate.setter
    def tax_rate(self, value: float):
        if 0 <= value <= 1:
            self._tax_rate = value
        else:
            raise ValueError("Company 'tax_rate' value is invalid")

    @property
    def language(self) -> str:
        """Gets or sets the language of the account names.

        It is the language used in the account names of the financial
        statements. This property accepts a string representing the
        desired language. Supported languages are "english" and
        "portuguese". The default is 'english'.

        Returns:
            The language used in the account names.

        Raises:
            KeyError: If the provided language is not supported.
        """

        return self._language

    @language.setter
    def language(self, language: Literal["english", "portuguese"]):
        # Supported languages
        list_languages = ["english", "portuguese"]
        if language.lower() in list_languages:
            self._language = language.capitalize()
        else:
            sup_lang = f"Supported languages: {', '.join(list_languages)}"
            raise KeyError(f"'{language}' not supported. {sup_lang}")

    def _set_df(self) -> pd.DataFrame:
        """Sets the company data frame.

        This method creates a dataframe with the company's financial
        statements.
        """
        df = (
            dm.get_main_df()
            .query("cvm_id == @self._cvm_id and is_consolidated")
            .reset_index(drop=True)
        )

        # Convert category columns back to string
        columns = df.columns
        cat_cols = [c for c in columns if df[c].dtype == "category"]
        df[cat_cols] = df[cat_cols].astype("string")

        # Change acc_value only when it is not earnings_per_share (acc_code 8)
        df["acc_value"] = np.where(
            df["acc_code"].str.startswith("8"),
            df["acc_value"],
            df["acc_value"] / self._acc_unit,
        )

        self._first_period = df["period_end"].min()
        self._last_period = df["period_end"].max()

        # Not necessarily there will be a quarterly report for the last period
        self._last_annual = df.query("is_annual")["period_end"].max()

        if self._last_period == self._last_annual:
            self._last_period_type = "ANNUAL"
            self._last_quarterly = None
        else:
            self._last_period_type = "QUARTERLY"
            self._last_quarterly = df.query("not is_annual")["period_end"].max()

        # Drop columns that are already company attributes or will not be used
        df.drop(
            columns=["name_id", "cvm_id", "tax_id", "is_consolidated"],
            inplace=True,
        )

        # Set company data frame
        self._df = df

    def info(self) -> dict:
        """Print a concise summary of a company."""
        company_info = {
            "Name": self.name_id,
            "CVM ID": self._cvm_id,
            "Fiscal ID (CNPJ)": self.tax_id,
            "Total Accounting Rows": len(self._df.index),
            "Selected Accounting Method": self._is_consolidated,
            "Selected Accounting Unit": self._acc_unit,
            "Selected Tax Rate": self._tax_rate,
            "First Report": self._first_period.strftime("%Y-%m-%d"),
            "Last Report": self._last_period.strftime("%Y-%m-%d"),
            "Last Report Type": self._last_period_type,
        }
        print_dict(info_dict=company_info, table_name="Company Info")

    def _build_report_index(self, dfi: pd.DataFrame) -> pd.DataFrame:
        """Build the index for the report. This function is used by the
        _build_report function. The index is built from the annual reports
        "acc_code" works as a primary key. Other columns set the preference order
        """
        df = (
            dfi[["acc_code", "acc_name", "period_end"]]
            .sort_values(by=["acc_code", "period_end"])
            .drop_duplicates(subset=["acc_code"], keep="last", ignore_index=True)[
                ["acc_code", "acc_name"]
            ]
        )
        return df

    def _build_report(self, dfi: pd.DataFrame) -> pd.DataFrame:
        # Start "dfo" with the index
        dfo = self._build_report_index(dfi)
        year_cols = ["acc_code", "acc_value"]
        periods = sorted(dfi["period_end"].drop_duplicates())
        for period in periods:
            df_year = dfi.query("period_end == @period")[year_cols].copy()
            period_str = period.strftime("%Y-%m-%d")
            if period == self._last_period and self._last_period_type == "QUARTERLY":
                period_str += " (ttm)"
            df_year.rename(columns={"acc_value": period_str}, inplace=True)
            dfo = pd.merge(dfo, df_year, how="left", on=["acc_code"])
        dfo.fillna(0, inplace=True)
        return dfo.sort_values("acc_code", ignore_index=True)

    def report(
        self,
        report_type: str,
        acc_level: int = 0,
        num_years: int = 0,
    ) -> pd.DataFrame:
        """Generate an accounting report for the company.

        This function generates a report representing one of the financial
        statements for the company adjusted by the attributes passed.

        Args:
            report_type: Type of financial report to be generated. Options are:
                - balance_sheet
                    - assets
                        - cash
                        - current_assets,
                        - non_current_assets
                    - liabilities
                        - debt
                        - current_liabilities
                        - non_current_liabilities,
                    - liabilities_and_equity
                        - equity
                - income_statement
                - cash_flow
                - earnings_per_share
                - comprehensive_income,
                - added_value
            acc_level: Detail level to show for account codes. Options are 0, 1,
                2, 3 or 4. Defaults to 0. How the values works:
                    0    -> X...       (show all accounts)
                    1    -> X          (show 1 level)
                    2    -> X.YY       (show 2 levels)
                    3    -> X.YY.ZZ    (show 3 levels)
                    4    -> X.YY.ZZ.WW (show 4 levels)
            num_years: Number of years to include in the report. Defaults to 0
                (all years).

        Returns:
            pd.DataFrame: Generated financial report as a pandas DataFrame.

        Raises:
            ValueError: If some argument is invalid.
        """
        # Copy company dataframe to avoid changing it
        df = self._df.copy()
        # Check input arguments.
        if acc_level not in [0, 1, 2, 3, 4]:
            raise ValueError("acc_level expects 0, 1, 2, 3 or 4")

        # Filter dataframe for selected acc_level
        # Example of an acc_code: "7.08.04.04" -> 4 levels and 3 dots
        if acc_level:
            df.query(rf"acc_code.str.count('\.') <= {acc_level - 1}", inplace=True)

        # Set language
        class MyDict(dict):
            """Custom dictionary class to return key if key is not found."""

            def __missing__(self, key):
                return "(pt) " + key

        if self._language == "English":
            _pten_dict = dict(language_df.values)
            _pten_dict = MyDict(_pten_dict)
            df["acc_name"] = df["acc_name"].map(_pten_dict)

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
            8 -> Earnings per Share
        """
        report_types = {
            "balance_sheet": ("1", "2"),
            "income_statement": ("3"),
            "cash_flow": ("6"),
            "assets": ("1"),
            "cash": ("1.01.01", "1.01.02"),
            "current_assets": ("1.01"),
            "non_current_assets": ("1.02"),
            "liabilities": ("2.01", "2.02"),
            "debt": ("2.01.04", "2.02.01"),
            "current_liabilities": ("2.01"),
            "non_current_liabilities": ("2.02"),
            "liabilities_and_equity": ("2"),
            "equity": ("2.03"),
            "comprehensive_income": ("4"),
            "added_value": ("7"),
            "earnings_per_share": ("8"),
        }
        acc_codes = report_types[report_type]  # noqa
        df.query("acc_code.str.startswith(@acc_codes)", inplace=True)
        df.reset_index(drop=True, inplace=True)

        if (
            report_type in ["income_statement", "cash_flow"]
            and self._last_period_type == "QUARTERLY"
        ):
            df = self._calculate_ttm(df)

        # Show only selected years
        all_periods = sorted(df["period_end"].drop_duplicates())
        selected_periods = all_periods[-num_years:]  # noqa
        df.query("period_end in @selected_periods", inplace=True)

        return self._build_report(df)

    def _calculate_ttm(self, dfi: pd.DataFrame) -> pd.DataFrame:
        """Calculate trailing twelve months (TTM) for income statement and cash
        when quarterly data is the most recent available. If the function was
        called, the last period is quarterly"""

        # Quarterly dataframe
        dfq = dfi.query("not is_annual").copy()
        ttm_period_begin = dfq["period_end"].min()

        # Last quarter in quarterly dataframe
        df1 = dfq.query("period_end == period_end.max()").copy()

        # Previous quarter in quarterly dataframe
        df2 = dfq.query("period_end == period_end.min()").copy()
        df2["acc_value"] = -df2["acc_value"]

        # Last annual report
        dfa = dfi.query("is_annual and period_end == @self._last_annual").copy()

        # Construct TTM dataframe
        df_ttm = (
            pd.concat([df1, df2, dfa], ignore_index=True)[["acc_code", "acc_value"]]
            .groupby(by="acc_code")
            .sum()
            .reset_index()
        )
        df1.drop(columns="acc_value", inplace=True)
        df_ttm = pd.merge(df1, df_ttm)
        df_ttm["period_begin"] = ttm_period_begin

        df_annual = dfi.query("is_annual").copy()

        return pd.concat([df_annual, df_ttm], ignore_index=True)

    def custom_report(
        self,
        acc_list: list[str],
        num_years: int = 0,
    ) -> pd.DataFrame:
        """Generate a financial report from custom list of accounting codes

        Args:
            acc_list: A list of strings containg accounting codes to be used
                in the report
            num_years: Select how many years to show in the report.
                Defaults to 0 (show all years)

        Returns:
            pd.DataFrame: Generated financial report as a pandas DataFrame.

        Raises:
            ValueError: If some argument is invalid.
        """
        df_bs = self.report("balance_sheet", num_years=num_years)
        df_is = self.report("income_statement", num_years=num_years)
        df_cf = self.report("cash_flow", num_years=num_years)
        df = (
            pd.concat([df_bs, df_is, df_cf])
            .query(f"acc_code == {acc_list}")
            .reset_index(drop=True)
        )
        return df

    @staticmethod
    def _prior_values(s: pd.Series, is_prior: bool) -> pd.Series:
        """Shift row to the right in order to obtain series previous values"""
        if is_prior:
            arr = s.iloc[:-1].values
            return np.append(np.nan, arr)
        else:
            return s

    def indicators(self, num_years: int = 0, is_prior: bool = True) -> pd.DataFrame:
        """Calculate the company main operating indicators.

        Args:
            num_years: Number of years to consider for calculation. If 0, use
                all available years. Defaults to 0.
            is_prior: Divide return measurements by book values from the end of
                the prior year (see Damodaran reference). Defaults to True.

        Returns:
            pd.DataFrame: Dataframe containing calculated financial indicators.

        References:
            [1] Aswath Damodaran, "Return on Capital (ROC), Return on Invested
                Capital (ROIC) and Return on Equity (ROE): Measurement and
                Implications.", 2007,
                https://people.stern.nyu.edu/adamodar/pdfoles/papers/returnmeasures.pdf
                https://people.stern.nyu.edu/adamodar/New_Home_Page/datafile/variable.htm
        """
        df_bs = self.report("balance_sheet")
        df_is = self.report("income_statement")
        df_cf = self.report("cash_flow")
        df = (
            pd.concat([df_bs, df_is, df_cf])
            .drop(columns=["acc_name"])
            .set_index("acc_code", drop=True)
        )

        # Calculate indicators series
        revenues = df.loc["3.01"]
        gross_profit = df.loc["3.03"]
        ebit = df.loc["3.05"]
        ebt = df.loc["3.07"]
        effective_tax = df.loc["3.08"]
        depreciation_amortization = df.loc["6.01.01.04"]
        ebitda = ebit + depreciation_amortization
        operating_cash_flow = df.loc["6.01"]
        # capex = df.loc["6.02"]
        net_income = df.loc["3.11"]
        total_assets = df.loc["1"]
        total_assets_p = self._prior_values(total_assets, is_prior)
        equity = df.loc["2.03"]
        equity_p = self._prior_values(equity, is_prior)
        total_cash = df.loc["1.01.01"] + df.loc["1.01.02"]
        current_assets = df.loc["1.01"]
        current_liabilities = df.loc["2.01"]
        working_capital = current_assets - current_liabilities
        total_debt = df.loc["2.01.04"] + df.loc["2.02.01"]
        net_debt = total_debt - total_cash
        invested_capital = total_debt + equity - total_cash
        invested_capital_p = self._prior_values(invested_capital, is_prior)

        # Output Dataframe (dfo)
        dfo = pd.DataFrame(columns=df.columns)
        dfo.loc["revenues"] = revenues
        dfo.loc["operating_cash_flow"] = operating_cash_flow
        # dfo.loc["capex"] = capex
        dfo.loc["ebitda"] = ebitda
        dfo.loc["ebit"] = ebit
        dfo.loc["ebt"] = ebt
        dfo.loc["effective_tax_rate"] = -1 * effective_tax / ebt
        dfo.loc["net_income"] = net_income
        dfo.loc["total_cash"] = total_cash
        dfo.loc["total_debt"] = total_debt
        dfo.loc["net_debt"] = net_debt
        dfo.loc["working_capital"] = working_capital
        dfo.loc["invested_capital"] = invested_capital
        dfo.loc["return_on_assets"] = ebit * (1 - self._tax_rate) / total_assets_p
        dfo.loc["return_on_capital"] = ebit * (1 - self._tax_rate) / invested_capital_p
        dfo.loc["return_on_equity"] = net_income / equity_p
        dfo.loc["gross_margin"] = gross_profit / revenues
        dfo.loc["ebitda_margin"] = ebitda / revenues
        dfo.loc["pre_tax_operating_margin"] = ebit / revenues
        dfo.loc["after_tax_operating_margin"] = ebit * (1 - self._tax_rate) / revenues
        dfo.loc["net_margin"] = net_income / revenues

        # Show only the selected number of years
        if num_years > 0:
            dfo = dfo[dfo.columns[-num_years:]]
        return dfo
