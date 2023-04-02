import finlogic as fl


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
