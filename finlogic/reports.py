import pandas as pd
import numpy as np
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
    # df.query("cvm_id == 9512", inplace=True)
    return df.reset_index(drop=True)


def drop_duplicated_entries(df: pd.DataFrame) -> pd.DataFrame:
    """Drop duplicated accounting entries before building database

    Because the report holds accounting entries for the year before, we can
    remove entries that are not most recent one. By doing this, we guarantee
    that there is only one valid accounting entry, the most recent one.
    """
    df.sort_values(by=SORT_COLS, ascending=True, inplace=True, ignore_index=True)
    subset_cols = SORT_COLS.copy()
    subset_cols.remove("period_reference")
    df.drop_duplicates(subset=subset_cols, keep="last", ignore_index=True, inplace=True)

    return df


def drop_uncessary_quarters(df: pd.DataFrame) -> pd.DataFrame:
    """Drop quarters that will not be used for building the reports and the
    indicators dataframes."""
    mask = ~df["is_annual"]
    gr_last_quarter = df[mask].groupby(["cvm_id"])["period_end"].max()
    df["last_quarter"] = df["cvm_id"].map(gr_last_quarter)

    mask = df["is_annual"]
    gr_last_annual = df[mask].groupby(["cvm_id"])["period_end"].max()
    df["last_annual"] = df["cvm_id"].map(gr_last_annual)

    # Annual mask
    annual_mask = df["is_annual"]

    # Quarter mask
    mask1 = ~df["is_annual"]
    mask2 = df["last_quarter"] > df["last_annual"]
    mask3 = df["period_end"] == df["last_quarter"]
    mask4 = df["period_end"] == (df["last_quarter"] - pd.DateOffset(years=1))
    quarter_mask = mask1 & mask2 & (mask3 | mask4)

    mask = annual_mask | quarter_mask

    # df.drop(columns=["last_quarter", "last_annual"], inplace=True)
    df = df[mask].reset_index(drop=True).copy()
    return df


def get_ltm_mask(df: pd.DataFrame) -> pd.Series:
    """Build a mask to divide the dataframe between data used in the LTM adjustment
    and data that it not used in LTM adjustment.
    The annual reports are not adjusted to LTM, only the quarterly reports are
    adjusted. But we need to use the last annual report to adjust the quarterly
    reports.
    Conditions that need to be met for each company:
        (1) Last "period_end" of annual reports
        (2) Last "period_reference" of quarterly reports
        (3) Only income and cash flow statements are adjusted to LTM
    """
    # Condition (1)
    mask1 = df["is_annual"]
    mask2 = df["period_end"] == df["last_annual"]
    cond1 = mask1 & mask2

    # Condition (2)
    mask1 = ~df["is_annual"]
    mask2 = df["period_reference"] == df["last_quarter"]
    cond2 = mask1 & mask2

    # Condition (3)
    cond3 = df["report_type"].isin([3, 6])

    return (cond1 | cond2) & cond3


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
    # Get previous quarter dataframe and invert the values
    mask1 = ~df["is_annual"]
    mask2 = df["period_end"] == (df["last_quarter"] - pd.DateOffset(years=1))
    mask = mask1 & mask2
    df["acc_value"] = np.where(mask, -1 * df["acc_value"], df["acc_value"])

    # Build LTM adjusted dataframe
    ltm = (
        df[["cvm_id", "is_consolidated", "acc_code", "acc_value"]]
        .groupby(by=["cvm_id", "is_consolidated", "acc_code"])
        .sum()
        .reset_index()
    )
    # current_quarter receives LTM values
    current_quarter = df.query("not is_annual and period_end == period_end.max()").drop(
        columns="acc_value"
    )
    ltm = pd.merge(current_quarter, ltm)
    ltm["period_begin"] = ltm["period_end"] - pd.DateOffset(years=1)

    # Get annuals dataframe
    annuals = df.query("is_annual").copy()

    # Previous quarter will not be used anymore
    return pd.concat([annuals, ltm], ignore_index=True)


def build_reports_df():
    """Build FinLogic Database from processed CVM files."""
    df = read_all_processed_files()
    df = drop_duplicated_entries(df)
    df = drop_uncessary_quarters(df)

    ltm_mask = get_ltm_mask(df)
    # Separate the df in LTM and non-LTM
    ltm = df[ltm_mask].reset_index(drop=True)
    not_ltm = df[~ltm_mask].reset_index(drop=True)

    ltm = adjust_ltm(ltm)

    # Concatenate the adjusted and non-adjusted dataframes
    df = pd.concat([ltm, not_ltm], ignore_index=True)

    # After the drop_unecessary entries, period_reference is not necessary anymore
    # df.drop(columns=["period_reference"], inplace=True)

    df.sort_values(by=SORT_COLS, ascending=True, inplace=True, ignore_index=True)

    cat_cols = [c for c in df.columns if df[c].dtype in ["object"]]
    df[cat_cols] = df[cat_cols].astype("category")

    df.to_pickle(cfg.DF_PATH, compression="zstd")
