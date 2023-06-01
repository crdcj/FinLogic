import pandas as pd
from . import data_manager as dm

TAX_RATE = 0.34


def build_indicators() -> pd.DataFrame:
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

    dfp.rename(columns=indicators_codes, inplace=True)

    dfp["working_capital"] = dfp["current_assets"] - dfp["current_liabilities"]

    dfp["total_cash"] = dfp["cash_equivalents"] + dfp["financial_investments"]
    dfp.drop(columns=["cash_equivalents", "financial_investments"], inplace=True)

    dfp["total_debt"] = dfp["short_term_debt"] + dfp["long_term_debt"]
    dfp["net_debt"] = dfp["total_debt"] - dfp["total_cash"]
    dfp.drop(columns=["short_term_debt", "long_term_debt"], inplace=True)

    dfp["effective_tax_rate"] = -1 * dfp["effective_tax"] / dfp["ebt"]

    dfp["ebitda"] = dfp["ebit"] + dfp["depreciation_amortization"]

    dfp["invested_capital"] = dfp["total_debt"] + dfp["equity"] - dfp["total_cash"]

    gp_cols = ["cvm_id", "is_annual", "is_consolidated"]
    dfp["invested_capital_p4"] = dfp.groupby(by=gp_cols)["invested_capital"].shift(4)
    dfp["invested_capital_p1"] = dfp.groupby(by=gp_cols)["invested_capital"].shift(1)

    dfp["invested_capital_p"] = dfp["invested_capital_p4"]
    dfp["invested_capital_p"].fillna(dfp["invested_capital_p1"], inplace=True)
    dfp["invested_capital_p"].fillna(dfp["invested_capital"], inplace=True)

    dfp["avg_invested_capital"] = (
        dfp["invested_capital"] + dfp["invested_capital_p"]
    ) / 2

    dfp["equity_p4"] = dfp.groupby(by=gp_cols)["equity"].shift(4)
    dfp["equity_p1"] = dfp.groupby(by=gp_cols)["equity"].shift(1)

    dfp["equity_p"] = dfp["equity_p4"]
    dfp["equity_p"].fillna(dfp["equity_p1"], inplace=True)
    dfp["equity_p"].fillna(dfp["equity"], inplace=True)
    dfp["avg_equity"] = (dfp["equity"] + dfp["equity_p"]) / 2

    # Drop temporary columns
    dfp.drop(
        columns=[
            "invested_capital_p",
            "invested_capital_p1",
            "invested_capital_p4",
            "equity_p",
            "equity_p1",
            "equity_p4",
        ],
        inplace=True,
    )

    dfp = dfp.groupby(by=gp_cols).tail(1).dropna().reset_index(drop=True)

    dfp["roic"] = dfp["ebit"] * (1 - TAX_RATE) / dfp["avg_invested_capital"]
    dfp["roe"] = dfp["ebit"] * (1 - TAX_RATE) / dfp["avg_equity"]

    return dfp
