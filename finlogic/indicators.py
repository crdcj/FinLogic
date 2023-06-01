import pandas as pd
from . import data_manager as dm


def build_indicators() -> pd.DataFrame:
    stock_codes = ["2.03", "1.01.01", "1.01.02", "2.01.04", "2.02.01"]
    flow_codes = ["3.01", "3.05"]
    codes = stock_codes + flow_codes  # noqa
    df = (
        dm.get_main_df()
        .query("acc_code in @codes and not is_annual")
        .drop(columns=["name_id", "tax_id", "acc_name", "period_begin"])
    )
    df = df.query("cvm_id == 9512").copy()  # TODO: Remove this line
    df["acc_code"] = df["acc_code"].astype("string")
    # df["period_begin"].fillna(df["period_end"], inplace=True)

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

    gp_cols = ["cvm_id", "is_annual", "is_consolidated"]
    # Divide in annual and quarterly, because the shit is different
    # Annual data will be shifted 1 year and quarterly 4 quarters
    # Annual dataframe

    dfs["ic_prev_4"] = dfs.groupby(by=gp_cols)["invested_capital"].shift(4)
    dfs["ic_prev_1"] = dfs.groupby(by=gp_cols)["invested_capital"].shift(1)
    dfs["ic_prev"] = dfs["ic_prev_4"]
    dfs["ic_prev"].fillna(dfs["ic_prev_1"], inplace=True)
    dfs["ic_prev"].fillna(dfs["invested_capital"], inplace=True)
    dfs["average_invested_capital"] = (dfs["invested_capital"] + dfs["ic_prev"]) / 2
    # Drop intermediary columns
    dfs.drop(columns=["ic_prev", "ic_prev_1", "ic_prev_4"], inplace=True)
    dfs = dfs.groupby(by=gp_cols).tail(1).dropna().reset_index(drop=True)

    dff = pd.pivot_table(
        df.query("acc_code in @flow_codes"),
        values="acc_value",
        index=index_cols,
        columns=["acc_code"],
    ).reset_index()

    dff = dff.groupby(by=gp_cols).tail(1)
    # dff.drop()
    on_cols = ["cvm_id", "is_annual", "is_consolidated", "period_end"]
    df = dfs.merge(dff, how="inner", on=on_cols)
    return df
