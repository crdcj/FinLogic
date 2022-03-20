# Brazilian corporation financial statements and performance indicators


<table border=1 cellpadding=10><tr><td>

#### \*\*\* IMPORTANT LEGAL DISCLAIMER \*\*\*

---

BrFinance is **not** affiliated, endorsed, or vetted by the Securities and
Exchange Commission of Brazil (CVM). It's an open-source tool that uses CVM
publicly available data and is intended for research and educational
purposes.

</td></tr></table>

---

**BrFinance** offers a Pythonic way to generate financial information, in
Brazilian currency (BRL) and output in Pandas dataframe format, from data made
publicly avaible by the securities market authority in Brazil (CVM)

---

## Quick Start

### The dataset module

```python
import brfinance as bf

### Starting the dataset
bf.create_dataset()

## Show dataset info
bf.dataset_info()

## Search a corporation in dataset by name
bf.search_in_dataset('petro')

### The Corporation Class
## CVM (regulator) ID or Fiscal ID can be used for corporation identity
petro = bf.Corporation(9512)

# show corporation info
petro.info()

# show corporation assets
# default arguments: consolidated basis, one BRL as unit and all accounts
petro.report(rtype='assets')

# show corporation liabilities with different arguments
petro.report(
    rtype='liabilities',
    accounting_method='separate', # in separate accounting basis
    unit=1_000_000, # in BRL million
    account_level=3, # show account details only up to level 3
    first_period='2015-01-01'
)

## Installation

Install `brfinance` using `pip`:

``` {.sourceCode .bash}
$ pip install brfinance --upgrade --no-cache-dir
```

To install `brfinance` using `conda`, see
[this](https://anaconda.org/carloscarvalho/brfinance).

### Requirements

-   [Python](https://www.python.org) \>= 3.8+
-   [Pandas](https://github.com/pydata/pandas) (tested to work with \>=1.4.0)
-   [Numpy](http://www.numpy.org) (tested to work with \>= 1.20)
-   [requests](http://docs.python-requests.org/en/master/) \>= 2.27.1
-   [zstandard](https://pypi.org/project/zstandard/) \>= 0.17.0


---

### Legal Stuff

**yfinance** is distributed under the **MIT License**. See
the [LICENSE.txt](./LICENSE.txt) file in the release for details.

---

### P.S.

All contributors are welcome, from beginner to advanced.

**Carlos Carvalho**