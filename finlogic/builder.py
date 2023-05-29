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

    Because the report holds accounting entries for the year before, we can keep only
    the most recent one in the database. By doing this, we guarantee
    that there is only one valid accounting value in the database -> the last one
    """
    sort_cols = [
        "cvm_id",
        "is_annual",
        "is_consolidated",
        "acc_code",
        "period_reference",
        "is_annual",
        "period_begin",
        "period_end",
    ]
    df.sort_values(by=sort_cols, ascending=True, inplace=True, ignore_index=True)

    subset_cols = [
        "cvm_id",
        "is_annual",
        "is_consolidated",
        "acc_code",
        "period_begin",
        "period_end",
    ]
    return df.drop_duplicates(subset=subset_cols, keep="last", ignore_index=True)


def drop_not_last_quarter_reports(df: pd.DataFrame) -> pd.DataFrame:
    """Keep the only last quarter reports for each company, since they are
    the only ones that will be used to calculate LTM values.
    """
    # Create a temporary column with the max. period_reference for each company
    df["max_period"] = df.groupby("cvm_id")["period_reference"].transform("max")
    mask1 = df["period_reference"] == df["max_period"]
    mask2 = df["is_annual"]
    mask = mask1 | mask2
    return df[mask].drop(columns=["max_period"]).reset_index(drop=True)


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


def build_main_df():
    """Build FinLogic Database from processed CVM files.
    dfa = annual dataframe
    dfq = quarterly dataframe
    """
    df = read_all_processed_files()
    df = drop_duplicated_entries(df)

    # Only last reports are used for quarterly data
    df = drop_not_last_quarter_reports(df)

    # Divide the dataframe in what needs to be adjusted to LTM and what does not
    # Only last "period_reference" entries are used in the LTM calculation
    # Only income and cash flow statements are adjusted to LTM
    df["max_period"] = df.groupby("cvm_id")["period_reference"].transform("max")
    mask1 = df["period_reference"] == df["max_period"]
    mask2 = df["acc_code"].str.startswith(("3", "6"))
    mask = mask1 & mask2
    ltm = df[mask].reset_index(drop=True).drop(columns=["max_period"])
    not_ltm = df[~mask].reset_index(drop=True).drop(columns=["max_period"])
    # Remove not last period end entries from quarterly
    # not_ltm = drop_not_last_quarter_end_period(not_ltm)

    ltm = adjust_ltm(ltm)

    # Concatenate the adjusted and non-adjusted dataframes
    df = pd.concat([ltm, not_ltm], ignore_index=True)

    # After the drop_unecessary entries, period_reference is not necessary anymore
    # df.drop(columns=["period_reference"], inplace=True)
    sort_cols = [
        "cvm_id",
        "is_annual",
        "is_consolidated",
        "acc_code",
        "period_reference",
        "is_annual",
        "period_begin",
        "period_end",
    ]
    df.sort_values(by=sort_cols, ascending=True, inplace=True, ignore_index=True)

    cat_cols = [c for c in df.columns if df[c].dtype in ["object"]]
    df[cat_cols] = df[cat_cols].astype("category")

    df.to_pickle(cfg.DF_PATH, compression="zstd")
