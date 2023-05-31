import pandas as pd
from . import data_manager as dm


def get_indicators(df: pd.DataFrame) -> pd.DataFrame:
    stock_codes = ["2.03", "1.01.01", "1.01.02", "2.01.04", "2.02.01"]
    flow_codes = ["3.05"]
    codes = stock_codes + flow_codes  # noqa
    df = dm.get_main_df().query("acc_code in @codes")
    df.drop(columns=["name_id", "tax_id", "acc_name"], inplace=True)
    df["acc_code"] = df["acc_code"].astype("string")
    df["period_begin"] = df["period_begin"].fillna(df.period_end)

    index_cols = ["cvm_id", "is_annual", "is_consolidated", "period_end"]
    dfs = pd.pivot_table(
        df.query("acc_code in @stock_codes"),
        values="acc_value",
        index=index_cols,
        columns=["acc_code"],
    ).reset_index()

    dfs["total_cash"] = dfs["1.01.01"] + dfs["1.01.02"]
    dfs["total_debt"] = dfs["2.01.04"] + dfs["2.02.01"]
    dfs.drop(columns=["1.01.01", "1.01.02", "2.01.04", "2.02.01"], inplace=True)
    dfs["equity"] = dfs["2.03"]
    dfs.drop(columns=["2.03"], inplace=True)
    dfs["invested_capital"] = dfs["total_debt"] + dfs["equity"] - dfs["total_cash"]
    dfs.drop(columns=["total_debt", "equity", "total_cash"], inplace=True)

    by_cols = ["cvm_id", "is_annual", "is_consolidated"]
    dfs["invested_capital_p"] = dfs.groupby(by=by_cols)["invested_capital"].shift(4)
    dfs.groupby(by=by_cols)[
        ["invested_capital", "invested_capital_p"]
    ].last().reset_index()
    return dfs
