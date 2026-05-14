from datetime import date

import polars as pl

import finlogic as fl
from finlogic import data

fl.load()


def test_info():
    """Test the info method of the Database module."""
    info_df = data.info()

    def get_info(key: str) -> str:
        return info_df.filter(pl.col("key") == key)["FinLogic Info"][0]

    first_report = date.fromisoformat(get_info("first_report"))
    assert first_report.year == 2009
    assert int(get_info("accounting_entries")) > 100_000


def test_search_company():
    """Test the search_company method of the Database module."""
    search_result = fl.search_company("petrobras")
    """
        name_id                              cvm_id  tax_id              segment       is_restructuring  most_traded_stock
        PETROLEO BRASILEIRO S.A. PETROBRAS   9512    33.000.167/0001-01  exploration   False             PETR4
    """
    # Check results
    assert set(search_result["cvm_id"].to_list()) == {9512}


def test_load_is_traded_false_loads_more_companies():
    """Test that is_traded=False keeps non-traded companies in FINANCIALS_DF."""
    fl.load(is_traded=True)
    traded_count = data.FINANCIALS_DF["cvm_id"].n_unique()

    fl.load(is_traded=False)
    all_count = data.FINANCIALS_DF["cvm_id"].n_unique()

    assert all_count > traded_count
