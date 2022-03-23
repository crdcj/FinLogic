"""Tests for the main class (Company)."""
import importlib
import context # noqa
import brfinance as bf
importlib.reload(bf)

corp = bf.Corporation(20036)
df = corp.report('income')
print(df)
print(df.info())
# df = petro.liabilities_and_equity
# df.sort_values('DS_CONTA').tail(10)
# petro.equity
