"""Tests for the main class (Company)."""
import importlib
# import pandas as pd
import context  # noqa
import brfin.finance
from brfin.finance import Finance
importlib.reload(brfin.finance)

petro = Finance(9512, min_end_period='2015-12-31')
df = petro.assets
print(df)
print(df.info())
# df = petro.liabilities_and_equity
# df.sort_values('DS_CONTA').tail(10)
# petro.equity
