import os
import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
import finlogic as fi

DATA_PATH = os.path.dirname(__file__)+ '/data/'

def test_petro_indicators_consolidated():
    # Petro indicators (consolidated)
    co = fi.Company(9512, acc_unit='billion')
    result = co.indicators()
    expected = pd.read_pickle(DATA_PATH + 'petr_ind_con.pkl')
    assert_frame_equal(expected, result)

def test_petro_indicators_separate():
    # Petro indicators (separate)
    co = fi.Company(9512, acc_unit='billion', acc_method='separate')
    result = co.indicators()
    expected = pd.read_pickle(DATA_PATH + 'petr_ind_sep.pkl')
    assert_frame_equal(result, expected)

def test_agro_indicators_consolidated():
    # Agro indicators (consolidated)
    co = fi.Company(20036, acc_unit='million')
    result = co.indicators()
    expected = pd.read_pickle(DATA_PATH + 'agro_ind_con.pkl')
    assert_frame_equal(result, expected)

def test_agro_indicators_separate():
    # Agro indicators (separate)
    co = fi.Company(20036, acc_unit='million', acc_method='separate')
    result = co.indicators()
    expected = pd.read_pickle(DATA_PATH + 'agro_ind_sep.pkl')
    assert_frame_equal(result, expected)
