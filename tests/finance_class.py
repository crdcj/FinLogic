"""Tests for the main class (Company)."""
import importlib
# import pandas as pd
import context  # noqa
import brfinance.finance
from brfinance.finance import Finance
importlib.reload(brfinance.finance)

company = Finance(25291, min_end_period='2015-12-31')
df = company.income
print(df)
print(df.info())
# df = petro.liabilities_and_equity
# df.sort_values('DS_CONTA').tail(10)
# petro.equity
