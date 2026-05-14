import polars as pl

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


def filter_indicators_data(dfi: pl.DataFrame) -> pl.DataFrame:
    codes = list(INDICATORS_CODES.keys())
    drop_cols = ["tax_id", "acc_name", "period_begin"]
    sort_cols = ["cvm_id", "is_consolidated", "acc_code", "period_end"]
    subset_cols = ["cvm_id", "is_consolidated", "acc_code", "period_end"]

    return (
        dfi.filter(pl.col("acc_code").is_in(codes))
        .drop(drop_cols)
        .sort(sort_cols)
        .unique(subset=subset_cols, keep="last", maintain_order=True)
        .with_columns(pl.col("acc_code").cast(pl.String))
    )


def pivot_df(df: pl.DataFrame) -> pl.DataFrame:
    index_cols = ["cvm_id", "name_id", "is_annual", "is_consolidated", "period_end"]
    return df.pivot(
        values="acc_value", index=index_cols, on="acc_code", aggregate_function="first"
    ).fill_null(0)


def insert_annual_avg_col(col_name: str, df: pl.DataFrame) -> pl.DataFrame:
    gp_cols = ["cvm_id", "is_annual", "is_consolidated"]
    avg_col_name = f"avg_{col_name}"
    col_shifted = pl.col(col_name).shift(1).over(gp_cols)
    col_prev = (
        pl.when(col_shifted.is_null()).then(pl.col(col_name)).otherwise(col_shifted)
    )
    return df.with_columns(((pl.col(col_name) + col_prev) / 2).alias(avg_col_name))


def insert_quarterly_avg_col(col_name: str, df: pl.DataFrame) -> pl.DataFrame:
    gp_cols = ["cvm_id", "is_annual", "is_consolidated"]
    avg_col_name = f"avg_{col_name}"
    col_p4 = pl.col(col_name).shift(4).over(gp_cols)
    col_p1 = pl.col(col_name).shift(1).over(gp_cols)
    col_prev = (
        pl.when(col_p4.is_not_null())
        .then(col_p4)
        .when(col_p1.is_not_null())
        .then(col_p1)
        .otherwise(pl.col(col_name))
    )
    return df.with_columns(((pl.col(col_name) + col_prev) / 2).alias(avg_col_name))


def insert_key_cols(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.with_columns(
            total_cash=pl.col("cash_equivalents") + pl.col("financial_investments"),
            total_debt=pl.col("short_term_debt") + pl.col("long_term_debt"),
        )
        .drop(
            "cash_equivalents",
            "financial_investments",
            "short_term_debt",
            "long_term_debt",
        )
        .with_columns(
            working_capital=pl.col("current_assets") - pl.col("current_liabilities"),
            effective_tax_rate=-pl.col("effective_tax") / pl.col("ebt"),
            ebitda=pl.col("ebit") + pl.col("depreciation_amortization"),
            invested_capital=pl.col("total_debt")
            + pl.col("equity")
            - pl.col("total_cash"),
            net_debt=pl.col("total_debt") - pl.col("total_cash"),
        )
    )


def process_indicators(df: pl.DataFrame, is_annual: bool) -> pl.DataFrame:
    df = df.rename(INDICATORS_CODES)
    df = insert_key_cols(df)

    avg_cols = ["invested_capital", "total_assets", "equity"]
    for col_name in avg_cols:
        if is_annual:
            df = insert_annual_avg_col(col_name, df)
        else:
            df = insert_quarterly_avg_col(col_name, df)

    if not is_annual:
        gp_cols = ["cvm_id", "is_annual", "is_consolidated"]
        original_cols = df.columns
        df = (
            df.sort("cvm_id", "is_annual", "is_consolidated", "period_end")
            .group_by(gp_cols, maintain_order=True)
            .tail(1)
            .drop_nulls()
            .select(original_cols)
        )

    CUT_OFF_VALUE = 1_000_000
    df = df.with_columns(
        gross_margin=pl.when(pl.col("revenues") > CUT_OFF_VALUE)
        .then(pl.col("gross_profit") / pl.col("revenues"))
        .otherwise(0.0),
        ebitda_margin=pl.when(pl.col("revenues") > CUT_OFF_VALUE)
        .then(pl.col("ebitda") / pl.col("revenues"))
        .otherwise(0.0),
        operating_margin=pl.when(pl.col("revenues") > CUT_OFF_VALUE)
        .then(pl.col("ebit") / pl.col("revenues"))
        .otherwise(0.0),
        net_margin=pl.when(pl.col("revenues") > CUT_OFF_VALUE)
        .then(pl.col("net_income") / pl.col("revenues"))
        .otherwise(0.0),
    )

    df = df.with_columns(
        return_on_assets=pl.when(pl.col("avg_total_assets") > CUT_OFF_VALUE)
        .then(pl.col("ebit") * (1 - TAX_RATE) / pl.col("avg_total_assets"))
        .otherwise(0.0),
        return_on_equity=pl.when(pl.col("avg_equity") > CUT_OFF_VALUE)
        .then(pl.col("ebit") * (1 - TAX_RATE) / pl.col("avg_equity"))
        .otherwise(0.0),
        roic=pl.when(pl.col("avg_invested_capital") > CUT_OFF_VALUE)
        .then(pl.col("ebit") * (1 - TAX_RATE) / pl.col("avg_invested_capital"))
        .otherwise(0.0),
    )

    return df.drop("avg_total_assets", "avg_equity", "avg_invested_capital")


def build_indicators(financials_df: pl.DataFrame) -> pl.DataFrame:
    """Build indicators dataframe."""
    start_df = filter_indicators_data(financials_df)

    dfa = pivot_df(start_df.filter(pl.col("is_annual")))
    dfq = pivot_df(start_df.filter(~pl.col("is_annual")))

    dfai = process_indicators(dfa, True)
    dfqi = process_indicators(dfq, False)

    return pl.concat([dfai, dfqi]).sort("cvm_id", "is_consolidated", "period_end")


def adjust_unit(df: pl.DataFrame, unit: float) -> pl.DataFrame:
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
    existing_cols = [c for c in currency_cols if c in df.columns]
    return df.with_columns(pl.col(existing_cols) / unit)


def reorder_index(df: pl.DataFrame) -> pl.DataFrame:
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
    order_df = pl.DataFrame({"indicator": new_order, "_order": range(len(new_order))})
    return df.join(order_df, on="indicator", how="inner").sort("_order").drop("_order")


def format_indicators(df: pl.DataFrame, unit: float) -> pl.DataFrame:
    df = adjust_unit(df, unit)
    melt_cols = ["cvm_id", "name_id", "is_annual", "is_consolidated", "period_end"]
    df = df.unpivot(index=melt_cols, variable_name="indicator", value_name="value")
    df = df.sort("cvm_id", "is_consolidated", "period_end", "indicator")
    df = df.with_columns(pl.col("period_end").cast(pl.String))
    index_cols = ["cvm_id", "is_consolidated", "indicator"]
    df = df.pivot(
        values="value", index=index_cols, on="period_end", aggregate_function="first"
    )
    df = reorder_index(df)
    return df
