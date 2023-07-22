import pandas as pd


TAX_RATE = 0.34
INDICATORS_CODES = {
    "1": "total_assets",
    "1.01": "current_assets",
    "1.01.01": "cash_equivalents",
    "1.01.02": "financial_investments",
    "2.01": "current_liabilities",
    "2.01.04": "short_term_debt",
    "2.02.01": "long_term_debt",
    "2.03": "equity",
    "3.01": "revenues",
    "3.03": "gross_profit",
    "3.05": "ebit",
    "3.07": "ebt",
    "3.08": "effective_tax",
    "3.11": "net_income",
    "6.01": "operating_cash_flow",
    "6.01.01.04": "depreciation_amortization",
    "3.99.01.01": "eps",
}


def filter_indicators_data(dfi: pd.DataFrame) -> pd.DataFrame:
    codes = list(INDICATORS_CODES.keys())  # noqa: used in query below
    """There are 137 repeated entries in 208784 rows. These are from companies
    with some exotic period_end dates, as for cvm_id 3450. These entries will be
    removed in the next step, when we drop duplicates and the last entry
    published will be kept.
    """
    drop_cols = ["tax_id", "acc_name", "period_begin"]
    sort_cols = ["cvm_id", "is_consolidated", "acc_code", "period_end"]
    subset_cols = ["cvm_id", "is_consolidated", "acc_code", "period_end"]

    dfo = (
        dfi.query("acc_code in @codes")
        .drop(columns=drop_cols)
        # .query("cvm_id == 9512 and is_consolidated")  # for testing
        .sort_values(by=sort_cols, ignore_index=True)
        .drop_duplicates(subset=subset_cols, keep="last", ignore_index=True)
        .astype({"acc_code": "string"})
    )
    return dfo


def pivot_df(df) -> pd.DataFrame:
    index_cols = ["cvm_id", "name_id", "is_annual", "is_consolidated", "period_end"]
    dfp = (
        pd.pivot(df, values="acc_value", index=index_cols, columns=["acc_code"])
        .fillna(0)
        .reset_index()
    )
    return dfp


def insert_annual_avg_col(col_name: str, df: pd.DataFrame) -> pd.DataFrame:
    gp_cols = ["cvm_id", "is_annual", "is_consolidated"]
    col_name_p = f"{col_name}_p"
    avg_col_name = f"avg_{col_name}"
    df[col_name_p] = df.groupby(by=gp_cols)[col_name].shift(1)
    df[col_name_p] = df[col_name_p]
    df[col_name_p].fillna(df[col_name], inplace=True)
    df[avg_col_name] = (df[col_name] + df[col_name_p]) / 2
    df.drop(columns=[col_name_p], inplace=True)
    return df


def insert_quarterly_avg_col(col_name: str, df: pd.DataFrame) -> pd.DataFrame:
    gp_cols = ["cvm_id", "is_annual", "is_consolidated"]
    col_name_p4 = f"{col_name}_p4"
    col_name_p1 = f"{col_name}_p1"
    col_name_p = f"{col_name}_p"
    avg_col_name = f"avg_{col_name}"
    df[col_name_p4] = df.groupby(by=gp_cols)[col_name].shift(4)
    df[col_name_p1] = df.groupby(by=gp_cols)[col_name].shift(1)
    df[col_name_p] = df[col_name_p4]
    df[col_name_p].fillna(df[col_name_p1], inplace=True)
    df[col_name_p].fillna(df[col_name], inplace=True)
    df[avg_col_name] = (df[col_name] + df[col_name_p]) / 2
    df.drop(columns=[col_name_p4, col_name_p1, col_name_p], inplace=True)
    return df


def insert_key_cols(df: pd.DataFrame) -> pd.DataFrame:
    df["total_cash"] = df["cash_equivalents"] + df["financial_investments"]
    df.drop(columns=["cash_equivalents", "financial_investments"], inplace=True)

    df["total_debt"] = df["short_term_debt"] + df["long_term_debt"]
    df["net_debt"] = df["total_debt"] - df["total_cash"]
    df.drop(columns=["short_term_debt", "long_term_debt"], inplace=True)

    df["working_capital"] = df["current_assets"] - df["current_liabilities"]
    df["effective_tax_rate"] = -1 * df["effective_tax"] / df["ebt"]
    df["ebitda"] = df["ebit"] + df["depreciation_amortization"]
    df["invested_capital"] = df["total_debt"] + df["equity"] - df["total_cash"]

    return df


def process_indicators(df, is_annual: bool, insert_avg_col) -> pd.DataFrame:
    df.rename(columns=INDICATORS_CODES, inplace=True)
    df = insert_key_cols(df)

    avg_cols = ["invested_capital", "total_assets", "equity"]
    for col_name in avg_cols:
        df = insert_avg_col(col_name, df)

    # For quarterly data, we need only the last row of each group
    if not is_annual:
        gp_cols = ["cvm_id", "is_annual", "is_consolidated"]
        df = df.groupby(by=gp_cols).tail(1).dropna().reset_index(drop=True)

    # Margin ratios
    CUT_OFF_VALUE = 1_000_000
    df["gross_margin"] = df["gross_profit"] / df["revenues"]
    df["ebitda_margin"] = df["ebitda"] / df["revenues"]
    df["operating_margin"] = df["ebit"] / df["revenues"]
    df["net_margin"] = df["net_income"] / df["revenues"]
    margin_cols = ["gross_margin", "ebitda_margin", "operating_margin", "net_margin"]
    df.loc[df["revenues"] <= CUT_OFF_VALUE, margin_cols] = 0

    # Return ratios
    df["return_on_assets"] = df["ebit"] * (1 - TAX_RATE) / df["avg_total_assets"]
    df.loc[df["avg_total_assets"] <= CUT_OFF_VALUE, "return_on_assets"] = 0
    df["return_on_equity"] = df["ebit"] * (1 - TAX_RATE) / df["avg_equity"]
    df.loc[df["avg_equity"] <= CUT_OFF_VALUE, "return_on_equity"] = 0
    df["roic"] = df["ebit"] * (1 - TAX_RATE) / df["avg_invested_capital"]
    df.loc[df["avg_invested_capital"] <= CUT_OFF_VALUE, "roic"] = 0

    # Drop avg_cols
    avg_cols = ["avg_total_assets", "avg_equity", "avg_invested_capital"]
    df.drop(columns=avg_cols, inplace=True)

    return df


def build_indicators(financials_df: pd.DataFrame) -> pd.DataFrame:
    """Build indicators dataframe."""
    start_df = filter_indicators_data(financials_df)

    # Construct pivot tables for annual and quarterly
    dfa = pivot_df(start_df.query("is_annual"))
    dfq = pivot_df(start_df.query("not is_annual"))

    # Build indicators
    dfai = process_indicators(dfa, True, insert_annual_avg_col)
    dfqi = process_indicators(dfq, False, insert_quarterly_avg_col)

    # Build output dataframe
    sort_cols = ["cvm_id", "is_consolidated", "period_end"]
    df = pd.concat([dfai, dfqi]).sort_values(by=sort_cols, ignore_index=True)
    df.columns.name = None
    return df


def adjust_unit(df: pd.DataFrame, unit: float) -> pd.DataFrame:
    currency_cols = [
        "total_assets",
        "current_assets",
        "current_liabilities",
        "equity",
        "revenues",
        "gross_profit",
        "ebit",
        "ebt",
        "effective_tax",
        "net_income",
        "operating_cash_flow",
        "depreciation_amortization",
        "total_cash",
        "total_debt",
        "net_debt",
        "working_capital",
        "ebitda",
        "invested_capital",
    ]
    df.loc[:, currency_cols] /= unit
    return df


def reorder_index(df: pd.DataFrame) -> pd.DataFrame:
    new_order = [
        "total_assets",
        "current_assets",
        "total_cash",
        "working_capital",
        "invested_capital",
        "current_liabilities",
        "total_debt",
        "net_debt",
        "equity",
        "revenues",
        "gross_profit",
        "net_income",
        "ebitda",
        "ebit",
        "ebt",
        "effective_tax",
        "operating_cash_flow",
        "depreciation_amortization",
        "effective_tax_rate",
        "return_on_assets",
        "return_on_equity",
        "roic",
        "gross_margin",
        "ebitda_margin",
        "operating_margin",
        "net_margin",
        "eps",
    ]
    return df.reindex(new_order)


def format_indicators(df: pd.DataFrame, unit: float) -> pd.DataFrame:
    df = adjust_unit(df, unit)
    melt_cols = ["cvm_id", "is_annual", "is_consolidated", "period_end"]
    df = pd.melt(df, id_vars=melt_cols, var_name="indicator", value_name="value")

    sort_cols = ["cvm_id", "is_consolidated", "period_end", "indicator"]
    df.sort_values(by=sort_cols, inplace=True)

    df["period_end"] = df["period_end"].astype("string")

    index_cols = ["cvm_id", "is_consolidated", "indicator"]
    df = (
        pd.pivot(df, values="value", index=index_cols, columns=["period_end"])
        .reset_index()
        .set_index("indicator")
    )
    df.columns.name = None
    df.index.name = None
    df = reorder_index(df)
    return df
