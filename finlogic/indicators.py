import pandas as pd
from . import data_manager as dm

TAX_RATE = 0.34


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


def build_pivot_table(df) -> pd.DataFrame:
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
    df = df.query("acc_code in @codes").drop(
        columns=["name_id", "tax_id", "acc_name", "period_begin", "report_type"]
    )
    df["acc_code"] = df["acc_code"].astype("string")
    # df["period_begin"].fillna(df["period_end"], inplace=True)

    dfp = (
        pd.pivot(
            df,
            values="acc_value",
            index=["cvm_id", "is_annual", "is_consolidated", "period_end"],
            columns=["acc_code"],
        )
        .reset_index()
        .rename(columns=indicators_codes)
    )
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


def build_indicators(is_annual: bool, insert_avg_col) -> pd.DataFrame:
    df = (
        dm.get_main_df()
        .query(f"is_annual == {is_annual}")
        .query("cvm_id == 9512")  # TODO: Remove this line
    )
    dfp = build_pivot_table(df)
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

    return dfp


def save_indicators() -> None:
    quarterly_indicators = build_indicators(False, insert_quarterly_avg_col)
    annual_indicators = build_indicators(True, insert_annual_avg_col)
    df = pd.concat([quarterly_indicators, annual_indicators], ignore_index=True)
    df.to_csv("indicators.csv", index=False)
    df.to_pickle("indicators.pkl")
    return df
