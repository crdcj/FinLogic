"""Tests for the main class (Company)."""
import pandas as pd
from context import Company
# import brfin.company as co
pd.options.display.max_colwidth = 20
pd.options.display.max_rows = 40
petro = Company(9512, start_period='2015-12-31')
df = petro.assets
print(df)
print(df.info())
# df = petro.liabilities_and_equity
# df.sort_values('DS_CONTA').tail(10)
# petro.equity
