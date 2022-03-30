import os
import pandas as pd
from pandas.testing import assert_frame_equal
import finlogic as fi

DATA_PATH = os.path.dirname(__file__)+ '/data/'

# Petro indicators (consolidated)
co = fi.Company(9512, acc_unit='billion')
df_expected = pd.read_pickle(DATA_PATH + 'petr_ind_con.pkl')
assert_frame_equal(co.indicators(), df_expected)

# Petro indicators (separate)
co.acc_method = 'separate'
df_expected = pd.read_pickle(DATA_PATH + 'petr_ind_sep.pkl')
assert_frame_equal(co.indicators(), df_expected)

# Agro indicators (consolidated)
co = fi.Company(20036, acc_unit='million')
df_expected = pd.read_pickle(DATA_PATH + 'agro_ind_con.pkl')
assert_frame_equal(co.indicators(), df_expected)

# Agro indicators (separate)
co.acc_method = 'separate'
df_expected = pd.read_pickle(DATA_PATH + 'agro_ind_sep.pkl')
assert_frame_equal(co.indicators(), df_expected)
