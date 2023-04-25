import unittest
import finlogic as fl


class TestCompany(unittest.TestCase):
    def setUp(self):
        self.petro_sep = fl.Company(9512, acc_method="separate", acc_unit="billion")
        self.petro_con = fl.Company(9512, acc_method="consolidated", acc_unit="billion")

    def test_identifier(self):
        """Test the identifier method of the Company class.""" ""
        test_cvm_id = 4170
        test_fiscal_id = "33.592.510/0001-54"
        self.petro_sep.identifier = test_cvm_id
        self.assertEqual(self.petro_sep._co_id, test_cvm_id)
        self.assertEqual(self.petro_sep._co_fiscal_id, test_fiscal_id)

        self.petro_sep.identifier = test_fiscal_id
        self.assertEqual(self.petro_sep._co_id, test_cvm_id)
        self.assertEqual(self.petro_sep._co_fiscal_id, test_fiscal_id)

        invalid_identifier = 99999999  # Use an invalid identifier here

        with self.assertRaises(
            KeyError, msg="Should raise a KeyError with an invalid identifier"
        ):
            self.petro_sep.identifier = invalid_identifier

    def test_info(self):
        """Test the info method of the Company class."""
        # Get the info
        petro_info = self.petro_sep.info()
        # Check the info
        self.assertEqual(petro_info["Name"], "PETROLEO BRASILEIRO S.A. PETROBRAS")
        self.assertEqual(petro_info["CVM ID"], 9512)
        self.assertEqual(petro_info["Fiscal ID (CNPJ)"], "33.000.167/0001-01")

    def test_report(self):
        """Test if the report method of the Company class returns the correct
        values."""
        petro_report = self.petro_con.report(report_type="assets")
        # Get Total Assets
        assets_2009 = round(petro_report.at["1", "2009-12-31"], 3)
        assets_2015 = round(petro_report.at["1", "2015-12-31"], 3)
        assets_2020 = round(petro_report.at["1", "2020-12-31"], 3)
        # Check the reports
        self.assertEqual(assets_2009, 350.419)
        self.assertEqual(assets_2015, 900.135)
        self.assertEqual(assets_2020, 987.419)

    def test_indicators(self):
        # Petro indicators (separate)
        petro_indicators_sep = self.petro_sep.indicators()
        petro_indicators_con = self.petro_con.indicators()
        # Get the indicators (rounded to 4 decimals)
        revenues_2009_sep = round(petro_indicators_sep.at["revenues", "2009-12-31"], 4)
        total_debt_2015_sep = round(
            petro_indicators_sep.at["total_debt", "2015-12-31"], 4
        )
        roic_2021_sep = round(
            petro_indicators_sep.at["return_on_capital", "2021-12-31"], 4
        )
        # Get the indicators (rounded to 4 decimals)
        revenues_2009_con = round(petro_indicators_con.at["revenues", "2009-12-31"], 4)
        total_debt_2015_con = round(
            petro_indicators_con.at["total_debt", "2015-12-31"], 4
        )
        roic_2021_con = round(
            petro_indicators_con.at["return_on_capital", "2021-12-31"], 4
        )
        # Check the indicators
        self.assertEqual(roic_2021_sep, 0.1623)
        self.assertEqual(revenues_2009_sep, 134.0339)
        self.assertEqual(total_debt_2015_sep, 305.3460)
        # Check the indicators
        self.assertEqual(roic_2021_con, 0.2176)
        self.assertEqual(revenues_2009_con, 182.8338)
        self.assertEqual(total_debt_2015_con, 493.0230)


if __name__ == "__main__":
    unittest.main()
