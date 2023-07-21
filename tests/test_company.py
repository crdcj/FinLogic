import finlogic as fl
import pytest

fl.load()


def test_identifier():
    """Test the identifier method of the Company class."""
    petro_sep = fl.Company(9512, is_consolidated=False, acc_unit="b")

    test_cvm_id = 4170
    test_tax_id = "33.592.510/0001-54"
    petro_sep.identifier = test_cvm_id
    assert petro_sep._cvm_id == test_cvm_id
    assert petro_sep.tax_id == test_tax_id

    petro_sep.identifier = test_tax_id
    assert petro_sep._cvm_id == test_cvm_id
    assert petro_sep.tax_id == test_tax_id

    invalid_identifier = 99999999  # Use an invalid identifier here

    with pytest.raises(KeyError):
        petro_sep.identifier = invalid_identifier


def test_attributes():
    """Test the info method of the Company class."""
    petro_sep = fl.Company(9512, is_consolidated=False, acc_unit="b")

    # Check the attributes
    assert petro_sep.name_id == "PETROLEO BRASILEIRO S.A. PETROBRAS"
    assert petro_sep._cvm_id == 9512
    assert petro_sep.tax_id == "33.000.167/0001-01"


def test_report():
    """Test if the report method of the Company class returns the correct values."""
    petro_con = fl.Company(9512, is_consolidated=True, acc_unit="b")

    petro_report = petro_con.report(report_type="assets")
    # Get Total Assets
    assets_2009 = round(petro_report.query("acc_code == '1'")["2009-12-31"][0], 3)
    assets_2015 = round(petro_report.query("acc_code == '1'")["2015-12-31"][0], 3)
    assets_2020 = round(petro_report.query("acc_code == '1'")["2020-12-31"][0], 3)
    # Check the reports
    assert assets_2009 == 350.419
    assert assets_2015 == 900.135
    assert assets_2020 == 987.419


def test_indicators():
    """Test the indicators method of the Company class."""
    petro_sep = fl.Company(9512, is_consolidated=False, acc_unit="b")
    petro_con = fl.Company(9512, is_consolidated=True, acc_unit="b")
    # Petro indicators (separate)
    petro_indicators_sep = petro_sep.indicators()
    petro_indicators_con = petro_con.indicators()
    # Get the indicators (rounded to 4 decimals)
    revenues_2009_sep = round(petro_indicators_sep.at["revenues", "2009-12-31"], 4)
    total_debt_2015_sep = round(petro_indicators_sep.at["total_debt", "2015-12-31"], 4)
    roic_2021_sep = round(petro_indicators_sep.at["roic", "2021-12-31"], 4)
    # Get the indicators (rounded to 4 decimals)
    revenues_2009_con = round(petro_indicators_con.at["revenues", "2009-12-31"], 4)
    total_debt_2015_con = round(petro_indicators_con.at["total_debt", "2015-12-31"], 4)
    roic_2021_con = round(petro_indicators_con.at["roic", "2021-12-31"], 4)
    # Check the indicators
    assert roic_2021_sep == 0.1512
    assert revenues_2009_sep == 134.0339
    assert total_debt_2015_sep == 305.3460
    # Check the indicators
    assert roic_2021_con == 0.2149
    assert revenues_2009_con == 182.8338
    assert total_debt_2015_con == 493.0230
