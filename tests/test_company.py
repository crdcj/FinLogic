import finlogic as fl


def test_petro_info():
    # Petro info
    petro = fl.Company(9512, acc_method="separate", acc_unit="billion")
    # Get the info
    petro_info = petro.info()
    # Check the info
    assert petro_info["Name"] == "PETROLEO BRASILEIRO S.A. PETROBRAS"
    assert petro_info["CVM ID"] == 9512
    assert petro_info["Fiscal ID (CNPJ)"] == "33.000.167/0001-01"


def test_petro_report():
    # Petro reports
    petro = fl.Company(9512, acc_method="consolidated", acc_unit="billion")
    petro_report = petro.report(report_type="assets")
    # Get Total Assets
    assets_2009 = round(petro_report.at["1", "2009-12-31"], 3)
    assets_2015 = round(petro_report.at["1", "2015-12-31"], 3)
    assets_2020 = round(petro_report.at["1", "2020-12-31"], 3)
    # Check the reports
    assert assets_2009 == 350.419
    assert assets_2015 == 900.135
    assert assets_2020 == 987.419


def test_petro_indicators_separate():
    # Petro indicators (separate)
    petro = fl.Company(9512, acc_method="separate", acc_unit="billion")
    petro_indicators = petro.indicators()
    # Get the indicators (rounded to 4 decimals)
    revenues_2009 = round(petro_indicators.at["revenues", "2009-12-31"], 4)
    total_debt_2015 = round(petro_indicators.at["total_debt", "2015-12-31"], 4)
    roic_2021 = round(petro_indicators.at["return_on_capital", "2021-12-31"], 4)
    # Check the indicators
    assert roic_2021 == 0.1623
    assert revenues_2009 == 134.0339
    assert total_debt_2015 == 305.3460


def test_petro_indicators_consolidated():
    # Petro indicators (consolidated)
    petro = fl.Company(9512, acc_method="consolidated", acc_unit="billion")
    petro_indicators = petro.indicators()
    # Get the indicators (rounded to 4 decimals)
    revenues_2009 = round(petro_indicators.at["revenues", "2009-12-31"], 4)
    total_debt_2015 = round(petro_indicators.at["total_debt", "2015-12-31"], 4)
    roic_2021 = round(petro_indicators.at["return_on_capital", "2021-12-31"], 4)
    # Check the indicators
    assert roic_2021 == 0.2176
    assert revenues_2009 == 182.8338
    assert total_debt_2015 == 493.0230
