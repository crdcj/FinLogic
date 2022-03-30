import os
import pandas as pd
from pandas.testing import assert_frame_equal
from finlogic import Company

RESULTS_PATH = os.path.dirname(__file__) + '/results/'
print(RESULTS_PATH)

def test_petro_indicators_consolidated():
    # Petro indicators (consolidated)
    co = Company(9512, acc_unit='billion')
    result = co.indicators()
    expected = pd.read_pickle(RESULTS_PATH + 'petr_ind_con.pkl')
    assert_frame_equal(expected, result)

def test_petro_indicators_separate():
    # Petro indicators (separate)
    co = Company(9512, acc_unit='billion', acc_method='separate')
    result = co.indicators()
    expected = pd.read_pickle(RESULTS_PATH + 'petr_ind_sep.pkl')
    assert_frame_equal(result, expected)

def test_agro_indicators_consolidated():
    # Agro indicators (consolidated)
    co = Company(20036, acc_unit='million')
    result = co.indicators()
    expected = pd.read_pickle(RESULTS_PATH + 'agro_ind_con.pkl')
    assert_frame_equal(result, expected)

def test_agro_indicators_separate():
    # Agro indicators (separate)
    co = Company(20036, acc_unit='million', acc_method='separate')
    result = co.indicators()
    expected = pd.read_pickle(RESULTS_PATH + 'agro_ind_sep.pkl')
    assert_frame_equal(result, expected)
