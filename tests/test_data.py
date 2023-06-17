import unittest
from datetime import datetime
import finlogic as fl
from finlogic import data_manager as dm


class TestData(unittest.TestCase):
    def test_info(self):
        """Test the info method of the Database module."""
        info = dm.info()["FinLogic Info"]
        first_report = datetime.strptime(info["first_report"], "%Y-%m-%d")
        self.assertTrue(first_report.year <= 2009)
        self.assertTrue(info["accounting_entries"] > 1_000_000)

    def test_search_company(self):
        """Test the search_company method of the Database module."""
        search_result = fl.search_company("3r")
        """
            name_id cvm_id  tax_id  segment is_restructuring    most_traded_stock
        0	3R ...  25291   12...   expl... False	            RRRP3
        """
        # Check results
        self.assertEqual(set(search_result["cvm_id"]), {25291})


if __name__ == "__main__":
    unittest.main()
