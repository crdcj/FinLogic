from datetime import date
import finlogic as fl
from finlogic import data

fl.load()


def test_info():
    """Test the info method of the Database module."""
    info = data.info()["FinLogic Info"]
    first_report = date.fromisoformat(info["first_report"])
    assert first_report.year == 2009
    assert info["accounting_entries"] > 100_000


def test_search_company():
    """Test the search_company method of the Database module."""
    search_result = fl.search_company("3r")
    """
        name_id cvm_id  tax_id  segment is_restructuring    most_traded_stock
    0	3R ...  25291   12...   expl... False	            RRRP3
    """
    # Check results
    assert set(search_result["cvm_id"]) == {25291}
