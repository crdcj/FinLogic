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

import polars as pl

from . import data as dt
from . import indicators as ic


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
        acc_unit: float | Literal["t", "m", "b"] = 1.0,
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

    @staticmethod
    def convert_to_sl(expr: str) -> str:
        """Converts a string to a single line."""
        return expr.replace("\n", "")

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
        if isinstance(identifier, int):
            df = (
                dt.FINANCIALS_DF.select(["cvm_id", "tax_id", "name_id"])
                .filter(pl.col("cvm_id") == identifier)
                .unique()
            )
        else:
            df = (
                dt.FINANCIALS_DF.select(["cvm_id", "tax_id", "name_id"])
                .filter(pl.col("tax_id") == identifier)
                .unique()
            )
        if not df.is_empty():
            self._cvm_id = df["cvm_id"][0]
            self.tax_id = df["tax_id"][0]
            self.name_id = df["name_id"][0]
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
        return self._is_consolidated

    @is_consolidated.setter
    def is_consolidated(self, value: bool):
        if type(value) is bool:
            self._is_consolidated = value
        else:
            raise ValueError("Company 'is_consolidated' value is invalid")
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
    def acc_unit(self, value: float | Literal["t", "m", "b"]):
        match value:
            case "t":
                self._acc_unit = 1_000.0
            case "m":
                self._acc_unit = 1_000_000.0
            case "b":
                self._acc_unit = 1_000_000_000.0
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

    def _set_df(self) -> None:
        """Sets the company data frame.

        This method creates a dataframe with the company's financial
        statements.
        """
        df = dt.FINANCIALS_DF.filter(
            (pl.col("cvm_id") == self._cvm_id)
            & (pl.col("is_consolidated") == self._is_consolidated)
        )

        # Adjust for unit change only where it is not EPS (acc_code 3.99...)
        df = df.with_columns(
            pl.when(~pl.col("acc_code").str.starts_with("3.99"))
            .then(pl.col("acc_value") / self._acc_unit)
            .otherwise(pl.col("acc_value"))
            .alias("acc_value")
        )

        self._first_period = df["period_end"].min()
        self._last_period = df["period_end"].max()

        # Not necessarily there will be a quarterly report for the last period
        self._last_annual = df.filter(pl.col("is_annual"))["period_end"].max()

        if self._last_period == self._last_annual:
            self._last_period_type = "annual"
            self._last_quarterly = None
        else:
            self._last_period_type = "quarterly"
            self._last_quarterly = df.filter(~pl.col("is_annual"))["period_end"].max()

        # Drop columns that are already company attributes or will not be used
        self._df = df.drop(["name_id", "cvm_id", "tax_id", "is_consolidated"])

    def info(self) -> pl.DataFrame | None:
        """Print a concise summary of a company."""
        if self._df.is_empty():
            acc_method = "consolidated" if self._is_consolidated else "separate"
            print("There is no avaible accounting data for:")
            print(f"    cvm_id = {self._cvm_id} and accounting method = {acc_method}")
            return None
        company_info = {
            "Name": self.name_id,
            "CVM ID": str(self._cvm_id),
            "Fiscal ID (CNPJ)": self.tax_id,
            "Total Accounting Rows": str(len(self._df)),
            "Selected Accounting Method": "consolidated"
            if self._is_consolidated
            else "separate",
            "Selected Accounting Unit": str(self._acc_unit),
            "Selected Tax Rate": str(self._tax_rate),
            "First Report": str(self._first_period),
            "Last Report": str(self._last_period),
        }
        return pl.DataFrame(
            {
                "key": list(company_info.keys()),
                "Company Info": list(company_info.values()),
            }
        )

    @staticmethod
    def _build_report_index(dfi: pl.DataFrame) -> pl.DataFrame:
        """Build the index for the report. This function is used by the
        _build_report function. The index is built from the annual reports
        "acc_code" works as a primary key. Other columns set the preference order
        """
        return (
            dfi.select(["acc_code", "acc_name", "period_end"])
            .sort(["acc_code", "period_end"])
            .unique(subset=["acc_code"], keep="last", maintain_order=True)
            .select(["acc_code", "acc_name"])
        )

    def _build_report(self, dfi: pl.DataFrame) -> pl.DataFrame:
        # Start "dfo" with the index
        dfo = self._build_report_index(dfi)
        periods = sorted(dfi["period_end"].unique().to_list())
        for period in periods:
            df_year = dfi.filter(pl.col("period_end") == period).select(
                ["acc_code", "acc_value"]
            )
            period_str = period.strftime("%Y-%m-%d")
            if period == self._last_period and self._last_period_type == "quarterly":
                period_str += " ltm"
            df_year = df_year.rename({"acc_value": period_str})
            dfo = dfo.join(df_year, on="acc_code", how="left")
        return dfo.fill_null(0).sort("acc_code")

    @staticmethod
    def _remove_not_last_quarters(df: pl.DataFrame) -> pl.DataFrame:
        """Remove quarters that are not the last one.

        This function removes quarters that are not the last one.
        This is useful when generating reports.

        Args:
            df: Dataframe with the financial statements.

        Returns:
            Dataframe with the financial statements without the quarters that are not
            the last one.
        """
        max_period = df["period_end"].max()
        return df.filter(pl.col("is_annual") | (pl.col("period_end") == max_period))

    def report(
        self,
        report_type: Literal[
            "balance_sheet",
            "assets",
            "cash",
            "current_assets",
            "non_current_assets",
            "liabilities",
            "debt",
            "current_liabilities",
            "non_current_liabilities",
            "liabilities_and_equity",
            "equity",
            "income_statement",
            "cash_flow",
            "earnings_per_share",
        ],
        acc_level: Literal[0, 1, 2, 3, 4] = 0,
        num_years: int = 0,
    ) -> pl.DataFrame:
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
            pl.DataFrame: Generated financial report as a Polars DataFrame.

        Raises:
            ValueError: If some argument is invalid.
        """
        df = self._remove_not_last_quarters(self._df)
        if acc_level not in [0, 1, 2, 3, 4]:
            raise ValueError("acc_level expects 0, 1, 2, 3 or 4")

        # Filter dataframe for selected acc_level
        # Example of an acc_code: "7.08.04.04" -> 4 levels and 3 dots
        if acc_level:
            df = df.filter(pl.col("acc_code").str.count_matches("[.]") <= acc_level - 1)

        # Set language
        if self._language == "English":
            cols = dt.LANGUAGE_DF.columns
            _pten_dict = dict(
                zip(
                    dt.LANGUAGE_DF[cols[0]].to_list(),
                    dt.LANGUAGE_DF[cols[1]].to_list(),
                )
            )
            pt_col = pl.col("acc_name")
            en_col = pl.col("acc_name").replace_strict(_pten_dict, default=None)
            df = df.with_columns(
                pl.when(en_col.is_null())
                .then(pl.lit("(pt) ") + pt_col)
                .otherwise(en_col)
                .alias("acc_name")
            )

        """
        Filter dataframe for selected acc_code
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
            "balance_sheet": ["1", "2"],
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
            "income_statement": ["3"],
            "earnings_per_share": ["3.99"],
            "cash_flow": ["6"],
        }
        acc_codes = report_types[report_type]
        df = df.filter(
            pl.any_horizontal(
                [pl.col("acc_code").str.starts_with(c) for c in acc_codes]
            )
        )

        # Show only selected years
        all_periods = sorted(df["period_end"].unique().to_list())
        selected_periods = all_periods[-num_years:] if num_years else all_periods
        df = df.filter(pl.col("period_end").is_in(selected_periods))

        return self._build_report(df)

    def custom_report(
        self,
        acc_list: list[str],
        num_years: int = 0,
    ) -> pl.DataFrame:
        """Generate a financial report from custom list of accounting codes

        Args:
            acc_list: A list of strings containg accounting codes to be used
                in the report
            num_years: Select how many years to show in the report.
                Defaults to 0 (show all years)

        Returns:
            pl.DataFrame: Generated financial report as a Polars DataFrame.

        Raises:
            ValueError: If some argument is invalid.
        """
        df_bs = self.report("balance_sheet", num_years=num_years)
        df_is = self.report("income_statement", num_years=num_years)
        df_cf = self.report("cash_flow", num_years=num_years)
        return pl.concat([df_bs, df_is, df_cf]).filter(
            pl.col("acc_code").is_in(acc_list)
        )

    def indicators(self, num_years: int = 0) -> pl.DataFrame:
        """Calculate the company main operating indicators.

        Args:
            num_years: Number of years to consider for calculation. If 0, use
                all available years. Defaults to 0.

        Returns:
            pl.DataFrame: Dataframe containing calculated financial indicators.
        """
        df = dt.INDICATORS_DF.filter(
            (pl.col("cvm_id") == self._cvm_id)
            & (pl.col("is_consolidated") == self._is_consolidated)
        )
        df = ic.format_indicators(df, unit=self._acc_unit)
        # Columns cvm_id and is_consolidated are redundant for the Company class
        df = df.drop(["cvm_id", "is_consolidated"])
        # Show only the selected number of years
        if num_years > 0:
            period_cols = df.columns[1:]  # everything after "indicator"
            df = df.select(["indicator"] + list(period_cols[-num_years:]))
        return df
