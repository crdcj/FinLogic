import pandas as pd
from . import data_manager as dm

TAX_RATE = 0.34


def build_indicators(is_annual: bool, insert_avg_col) -> pd.DataFrame:
    indicators_codes = {
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
    }
    codes = list(indicators_codes.keys())  # noqa: used in query below

    """There are 137 repeated entries in 208784 rows. These are from companies
    with some exotic period_end dates, as for cvm_id 3450. These entries will be
    removed in the next step, when we drop duplicates and the last entry
    published will be kept.
    """
    drop_cols = ["name_id", "tax_id", "acc_name", "report_type"]
    sort_cols = [
        "cvm_id",
        "is_consolidated",
        "acc_code",
        "period_end",
        "period_reference",
    ]
    subset_cols = ["cvm_id", "is_consolidated", "acc_code", "period_end"]

    df = (
        dm.get_reports()
        .query(f"is_annual == {is_annual}")
        .query("acc_code in @codes")
        .drop(columns=drop_cols)
        # .query("cvm_id == 9512")
        .sort_values(by=sort_cols, ignore_index=True)
        .drop_duplicates(subset=subset_cols, keep="last", ignore_index=True)
        .astype({"acc_code": "string"})
    )

    dfp = pivot_df(df)

    dfp.rename(columns=indicators_codes, inplace=True)
    dfp = insert_key_cols(dfp)

    avg_cols = ["invested_capital", "total_assets", "equity"]
    for col_name in avg_cols:
        dfp = insert_avg_col(col_name, dfp)

    # For quarterly data, we need only the last row of each group
    if not is_annual:
        gp_cols = ["cvm_id", "is_annual", "is_consolidated"]
        dfp = dfp.groupby(by=gp_cols).tail(1).dropna().reset_index(drop=True)

    dfp["roa"] = dfp["ebit"] * (1 - TAX_RATE) / dfp["avg_total_assets"]
    dfp["roe"] = dfp["ebit"] * (1 - TAX_RATE) / dfp["avg_equity"]
    dfp["roic"] = dfp["ebit"] * (1 - TAX_RATE) / dfp["avg_invested_capital"]

    # Drop avg_cols
    avg_cols = ["avg_total_assets", "avg_equity", "avg_invested_capital"]
    dfp.drop(columns=avg_cols, inplace=True)

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


def pivot_df(df) -> pd.DataFrame:
    # The min_end_period is used to fill the missing begin periods
    # in stock accounting entries. This is necessary for the pivot.
    # There a many cases where the begin period is not period_end - 1 year
    gr_cols = ["cvm_id", "is_annual", "is_consolidated", "period_end"]
    df["min_end_period"] = df.groupby(by=gr_cols)["period_begin"].transform("min")
    df["period_begin"].fillna(df["min_end_period"], inplace=True)
    df.drop(columns=["min_end_period"], inplace=True)

    dfp = pd.pivot(
        df,
        values="acc_value",
        index=[
            "cvm_id",
            "is_annual",
            "is_consolidated",
            # "period_begin",
            "period_end",
        ],
        columns=["acc_code"],
    ).reset_index()
    return dfp


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


def format_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = pd.melt(
        df,
        id_vars=["cvm_id", "is_annual", "is_consolidated", "period_end"],
        var_name="indicator",
        value_name="value",
    )

    sort_cols = ["cvm_id", "is_consolidated", "period_end", "indicator"]
    df.sort_values(by=sort_cols, inplace=True)

    df["period_end"] = df["period_end"].astype("string")

    df = (
        pd.pivot(
            df,
            values="value",
            index=["cvm_id", "is_consolidated", "indicator"],
            columns=["period_end"],
        )
        .reset_index()
        .set_index("indicator")
    )
    return df


def save_indicators() -> None:
    annual_indicators = build_indicators(True, insert_annual_avg_col)
    quarterly_indicators = build_indicators(False, insert_quarterly_avg_col)
    df = pd.concat([quarterly_indicators, annual_indicators]).sort_values(
        by=["cvm_id", "is_consolidated", "period_end"], ignore_index=True
    )
    # df.drop(columns=["period_begin"], inplace=True)
    # df = format_indicators(df)
    df.to_csv("indicators.csv", index=False)
    df.to_pickle("indicators.pkl")
    return df
