import pandas as pd
from . import data_manager as dm

TAX_RATE = 0.34


def build_indicators() -> pd.DataFrame:
    codes_dict = {
        "1.01.01": "cash_equivalents",
        "1.01.02": "financial_investments",
        "2.01.04": "short_term_debt",
        "2.02.01": "long_term_debt",
        "2.03": "equity",
        "3.01": "revenues",
        "3.05": "ebit",
    }
    codes = list(codes_dict.keys())  # noqa: used in query below
    df = (
        dm.get_main_df()
        .query("acc_code in @codes and not is_annual")
        .drop(columns=["name_id", "tax_id", "acc_name", "period_begin"])
    )
    df = df.query("cvm_id == 9512").copy()  # TODO: Remove this line
    df["acc_code"] = df["acc_code"].astype("string")
    # df["period_begin"].fillna(df["period_end"], inplace=True)

    dfp = pd.pivot_table(
        df,
        values="acc_value",
        index=["cvm_id", "is_annual", "is_consolidated", "period_end"],
        columns=["acc_code"],
    ).reset_index()

    dfp.rename(columns=codes_dict, inplace=True)

    dfp["total_cash"] = dfp["cash_equivalents"] + dfp["financial_investments"]
    dfp.drop(columns=["cash_equivalents", "financial_investments"], inplace=True)

    dfp["total_debt"] = dfp["short_term_debt"] + dfp["long_term_debt"]
    dfp.drop(columns=["short_term_debt", "long_term_debt"], inplace=True)

    dfp["invested_capital"] = dfp["total_debt"] + dfp["equity"] - dfp["total_cash"]
    dfp.drop(columns=["total_debt", "equity", "total_cash"], inplace=True)

    gp_cols = ["cvm_id", "is_annual", "is_consolidated"]
    dfp["ic_prev_4"] = dfp.groupby(by=gp_cols)["invested_capital"].shift(4)
    dfp["ic_prev_1"] = dfp.groupby(by=gp_cols)["invested_capital"].shift(1)

    dfp["ic_prev"] = dfp["ic_prev_4"]
    dfp["ic_prev"].fillna(dfp["ic_prev_1"], inplace=True)
    dfp["ic_prev"].fillna(dfp["invested_capital"], inplace=True)

    dfp["average_invested_capital"] = (dfp["invested_capital"] + dfp["ic_prev"]) / 2

    # Drop temporary columns
    dfp.drop(columns=["ic_prev", "ic_prev_1", "ic_prev_4"], inplace=True)

    dfp = dfp.groupby(by=gp_cols).tail(1).dropna().reset_index(drop=True)

    dfp["roic"] = (1 - TAX_RATE) * dfp["ebit"] / dfp["average_invested_capital"]

    return dfp
