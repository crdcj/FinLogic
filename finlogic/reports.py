import pandas as pd
from . import config as cfg

SORT_COLS = [
    "cvm_id",
    "is_annual",
    "is_consolidated",
    "acc_code",
    "period_reference",
    "period_begin",
    "period_end",
]


def read_all_processed_files() -> pd.DataFrame:
    """Read all processed CVM files."""
    # list filepaths in processed folder
    filepaths = sorted(cfg.CVM_PROCESSED_DIR.glob("*.pickle"))
    df = pd.concat([pd.read_pickle(f, compression="zstd") for f in filepaths])
    # df.query("cvm_id == 9512 and acc_code.isin(['1', '3.01'])", inplace=True)
    # df.query("cvm_id == 9512", inplace=True)
    return df.reset_index(drop=True)


def drop_duplicated_entries(df: pd.DataFrame) -> pd.DataFrame:
    """Drop duplicated accounting entries before building database

    Because the report holds accounting entries for the year before, we can
    remove entries that are not most recent one. By doing this, we guarantee
    that there is only one valid accounting entry, the most recent one.
    """
    df.sort_values(by=SORT_COLS, inplace=True, ignore_index=True)
    subset_cols = SORT_COLS.copy()
    subset_cols.remove("period_reference")
    df.drop_duplicates(subset=subset_cols, keep="last", ignore_index=True, inplace=True)

    return df


def insert_auxiliary_cols(df: pd.DataFrame):
    """Insert aux. columns to help filter the dataframe."""
    mask = ~df["is_annual"]
    gr_last_quarter = df[mask].groupby(["cvm_id"])["period_end"].max()
    df["last_quarter"] = df["cvm_id"].map(gr_last_quarter)

    mask = df["is_annual"]
    gr_last_annual = df[mask].groupby(["cvm_id"])["period_end"].max()
    df["last_annual"] = df["cvm_id"].map(gr_last_annual)


def drop_uncessary_quarters(df: pd.DataFrame) -> pd.DataFrame:
    """Drop quarters that will not be used for building the reports and the
    indicators dataframes."""

    # Divide dataframe between annual and quarterly reports
    dfa = df.query("is_annual").copy()
    dfq = df.query("not is_annual").copy()

    # The quarterly reports have two types of periods: accumulated and not
    # accumulated. Only the accumulated periods will be used.
    dfq.sort_values(by=SORT_COLS, inplace=True, ignore_index=True)
    subset_cols = SORT_COLS.copy()
    subset_cols.remove("period_begin")
    dfq.drop_duplicates(
        subset=subset_cols, keep="first", ignore_index=True, inplace=True
    )

    # Quarter mask
    mask1 = dfq["last_quarter"] > dfq["last_annual"]
    mask2 = dfq["period_end"] == dfq["last_quarter"]
    mask3 = dfq["period_end"] == (dfq["last_quarter"] - pd.DateOffset(years=1))
    mask = mask1 & (mask2 | mask3)

    dfq = dfq.loc[mask].reset_index(drop=True)

    return pd.concat([dfa, dfq], ignore_index=True)


def get_ltm_mask(df: pd.DataFrame) -> pd.Series:
    """Build a mask to divide the dataframe between data used in the LTM adjustment
    and data that it not used in LTM adjustment.
    The annual reports are not adjusted to LTM, only the quarterly reports are
    adjusted. But we need to use the last annual report to adjust the quarterly
    reports.
    Conditions that need to be met for each company:
        1  - Last quarter > last annual and
        2a - Last "period_end" of annual reports or
        2b - Last "period_reference" of quarterly reports and
        3  - Only income, cash flow statements and EPS are adjusted to LTM
    """
    # Condition (1)
    cond1 = df["last_quarter"] > df["last_annual"]

    # Condition (2a)
    mask1 = df["is_annual"]
    mask2 = df["period_end"] == df["last_annual"]
    cond2a = mask1 & mask2

    # Condition (2b)
    mask1 = ~df["is_annual"]
    mask2 = df["period_reference"] == df["last_quarter"]
    cond2b = mask1 & mask2

    # Condition (3)
    cond3 = df["acc_code"].str.startswith(("3", "6", "8"))

    return cond1 & (cond2a | cond2b) & cond3


def adjust_ltm(df: pd.DataFrame) -> pd.DataFrame:
    """Adjust income and cash flow statements to LTM (Last Twelve Months).
    To get the LTM values, we need to sum the current accumulated quarter with
    the difference between the last annual value and the previous accumulated
    quarter. Provided that quarterly values are always accumulated, we can use
    the following formula:
    LTM = current quarter + (last annual - previous quarter)
    Example for 1Q23: LTM = 1Q23 + (A22 - 1Q22)
    Example for 3Q23: LTM = 3Q23 + (A23 - 3Q23)
    """
    # Split dataframe between annual and quarterly reports
    dfa = df.query("is_annual").copy()
    dfq = df.query("not is_annual").reset_index(drop=True)

    dfq["min_period_end"] = dfq.groupby("cvm_id")["period_end"].transform("min")
    # Get prior quarter and invert the values
    prior_quarter = dfq.query("period_end == min_period_end").copy()
    prior_quarter["acc_value"] = -1 * prior_quarter["acc_value"]

    # Get current quarter
    current_quarter = dfq.query("period_end == last_quarter").copy()

    # Build LTM adjusted dataframe
    ltm = (
        pd.concat([current_quarter, dfa, prior_quarter])[
            ["cvm_id", "is_consolidated", "acc_code", "acc_value"]
        ]
        .groupby(by=["cvm_id", "is_consolidated", "acc_code"])
        .sum()
        .reset_index()
    )

    # Current quarter receives LTM values
    current_quarter.drop(columns=["acc_value"], inplace=True)
    ltm = pd.merge(current_quarter, ltm)
    ltm["period_begin"] = ltm["min_period_end"]

    # Prior quarter will not be used anymore
    df = pd.concat([dfa, ltm], ignore_index=True)

    # Remove auxiliary columns
    df.drop(columns=["min_period_end"], inplace=True)

    return df


def save_reports():
    """Save FinLogic Database from processed CVM files."""
    df = read_all_processed_files()
    df = drop_duplicated_entries(df)
    insert_auxiliary_cols(df)
    df = drop_uncessary_quarters(df)

    ltm_mask = get_ltm_mask(df)
    # Separate the df in LTM and non-LTM
    ltm = df[ltm_mask].reset_index(drop=True)
    not_ltm = df[~ltm_mask].reset_index(drop=True)

    ltm = adjust_ltm(ltm)

    # Concatenate the adjusted and non-adjusted dataframes
    df = pd.concat([ltm, not_ltm], ignore_index=True)

    df.sort_values(by=SORT_COLS, ignore_index=True, inplace=True)

    # Drop auxiliary columns
    df.drop(columns=["last_quarter", "last_annual"], inplace=True)

    cat_cols = [c for c in df.columns if df[c].dtype in ["object"]]
    df[cat_cols] = df[cat_cols].astype("category")

    df.to_pickle(cfg.REPORTS_PATH, compression="zstd")


def get_reports() -> pd.DataFrame:
    """Return a DataFrame with all accounting data"""
    if cfg.REPORTS_PATH.is_file():
        df = pd.read_pickle(cfg.REPORTS_PATH, compression="zstd")
    else:
        df = pd.DataFrame()
    return df
