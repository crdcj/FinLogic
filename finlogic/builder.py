import pandas as pd
from . import config as cfg


def read_all_processed_files() -> pd.DataFrame:
    """Read all processed CVM files."""
    # list filepaths in processed folder
    filepaths = sorted(cfg.CVM_PROCESSED_DIR.glob("*.pickle"))
    df = pd.concat([pd.read_pickle(f, compression="zstd") for f in filepaths])
    return df


def drop_duplicated_entries(df: pd.DataFrame) -> pd.DataFrame:
    """Drop duplicated accounting entries before building database

    Because the report holds accounting entries for the year before, we can
    remove entries that are not most recent one. By doing this, we guarantee
    that there is only one valid accounting entry, the most recent one.
    """
    sort_cols = [
        "cvm_id",
        "is_annual",
        "is_consolidated",
        "report_type",
        "acc_code",
        "period_reference",
        "period_begin",
        "period_end",
    ]
    df.sort_values(by=sort_cols, ascending=True, inplace=True, ignore_index=True)

    subset_cols = sort_cols.copy()
    subset_cols.remove("period_reference")

    return df.drop_duplicates(subset=subset_cols, keep="last", ignore_index=True)


def adjust_ltm(df: pd.DataFrame) -> pd.DataFrame:
    """Adjust income and cash flow statements to LTM (Last Twelve Months).
    To get the LTM values, we     need to sum the current accumulated quarter
    with the difference between the last annual value and the previous
    accumulated quarter. Provided that quarterly values are always accumulated,
    we can use the following formula:
    LTM = current quarter + ( last annual - previous quarter)
    Example for 1Q23: LTM = 1Q23 + (A22 - 1Q22)
    Example for 3Q23: LTM = 3Q23 + (A23 - 3Q23)
    """
    # Quarters dataframe
    quarters = df.query("not is_annual").reset_index(drop=True)

    # Get current quarter dataframe
    grouped = quarters.groupby(["cvm_id", "is_consolidated", "acc_code"])["period_end"]
    mask = quarters["period_end"] == grouped.transform("max")
    current_quarter = quarters[mask].reset_index(drop=True)

    # Get previous quarter dataframe
    mask = quarters["period_end"] == grouped.transform("min")
    previous_quarter = quarters[mask].reset_index(drop=True)
    previous_quarter["acc_value"] = -previous_quarter["acc_value"]

    # Annuals dataframe
    annuals = df.query("is_annual").reset_index(drop=True)
    # Last annual dataframe
    grouped = annuals.groupby(["cvm_id", "is_consolidated", "acc_code"])["period_end"]
    last_annual = annuals[annuals["period_end"] == grouped.transform("max")]

    # Build LTM adjusted dataframe
    ltm = (
        pd.concat([current_quarter, previous_quarter, last_annual], ignore_index=True)[
            ["cvm_id", "is_consolidated", "acc_code", "acc_value"]
        ]
        .groupby(by=["cvm_id", "is_consolidated", "acc_code"])
        .sum()
        .reset_index()
    )
    # Use current_quarter as reference to insert the LTM values
    current_quarter.drop(columns="acc_value", inplace=True)
    ltm = pd.merge(current_quarter, ltm)
    ltm["period_begin"] = quarters["period_end"] - pd.DateOffset(years=1)

    return pd.concat([annuals, ltm], ignore_index=True)


def drop_not_last_quarter_end_period(df: pd.DataFrame) -> pd.DataFrame:
    """Drop not last period end entries from df."""
    # Separate the df in annual and quarterly
    mask = df["is_annual"]
    annual = df[mask].reset_index(drop=True)
    quarterly = df[~mask].reset_index(drop=True)

    # Remove not last period end entries from quarterly
    grouped = quarterly.groupby(["cvm_id"])["period_end"]
    mask = quarterly["period_end"] == grouped.transform("max")
    adj_quarterly = quarterly[mask].reset_index(drop=True)

    # Concatenate the adjusted and non-adjusted dataframes
    return pd.concat([annual, adj_quarterly], ignore_index=True)


def get_ltm_mask(df: pd.DataFrame) -> pd.Series:
    """Build a mask to divide the dataframe in what needs to be adjusted to LTM
    and what does not.
    Conditions that need to be met to adjust to LTM:
        - For annuals reports-> last "period_reference" in the annuals group
        - For quarterly reports -> last "period_reference" for the company
        - Only income and cash flow statements are adjusted to LTM (report_type
          3 and 6)
    """
    # Create columns for both max periods types
    gr_cols = ["cvm_id", "is_annual"]
    df["max_gr_period"] = df.groupby(gr_cols)["period_reference"].transform("max")
    df["max_co_period"] = df.groupby("cvm_id")["period_reference"].transform("max")
    # Filter annual reports
    mask1 = df["is_annual"]
    mask2 = df["period_reference"] == df["max_gr_period"]
    mask_annual = mask1 & mask2
    # Filter quarterly reports
    mask1 = ~df["is_annual"]
    mask2 = df["period_reference"] == df["max_co_period"]
    mask_quarterly = mask1 & mask2
    # Join annual and quarterly reports
    mask_reports = mask_annual | mask_quarterly
    mask_report_type = df["report_type"].isin([3, 6])
    # Final mask for the LTM entries
    mask = mask_reports & mask_report_type
    df.drop(columns=["max_gr_period", "max_co_period"], inplace=True)
    return mask


def build_main_df():
    """Build FinLogic Database from processed CVM files."""
    df = read_all_processed_files()
    df = drop_duplicated_entries(df)

    ltm_mask = get_ltm_mask(df)
    # Separate the df in LTM and non-LTM
    ltm = df[ltm_mask].reset_index(drop=True)
    not_ltm = df[~ltm_mask].reset_index(drop=True)

    ltm = adjust_ltm(ltm)

    # Concatenate the adjusted and non-adjusted dataframes
    df = pd.concat([ltm, not_ltm], ignore_index=True)

    # After the drop_unecessary entries, period_reference is not necessary anymore
    # df.drop(columns=["period_reference"], inplace=True)

    sort_cols = [
        "cvm_id",
        "is_annual",
        "is_consolidated",
        "report_type",
        "acc_code",
        "period_reference",
        "period_begin",
        "period_end",
    ]
    df.sort_values(by=sort_cols, ascending=True, inplace=True, ignore_index=True)

    cat_cols = [c for c in df.columns if df[c].dtype in ["object"]]
    df[cat_cols] = df[cat_cols].astype("category")

    df.to_pickle(cfg.DF_PATH, compression="zstd")
