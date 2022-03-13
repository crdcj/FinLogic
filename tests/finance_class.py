"""Tests for the main class (Company)."""
import importlib
import context # noqa
import brfinance.finance as bf
importlib.reload(bf)

corporation = bf.Finance(
    corporation_id=20036,
    account_basis='consolidated',
    unit=1_000_000,
)
df = corporation.income
print(df)
print(df.info())
# df = petro.liabilities_and_equity
# df.sort_values('DS_CONTA').tail(10)
# petro.equity
