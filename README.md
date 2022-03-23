## BFinance: finance toolkit for Brazilian listed companies


<table border=1 cellpadding=10><tr><td>

#### \*\*\* IMPORTANT LEGAL DISCLAIMER \*\*\*

---

BFinance is **not** affiliated, endorsed, or vetted by the Securities and
Exchange Commission of Brazil (CVM). It's an open-source tool that uses CVM
publicly available data and is intended for research and educational
purposes.

</td></tr></table>

---

**BFinance** offers a Pythonic way to analyze financial data of listed
companies in Brazil from information made publicly avaible by local securities
market authority (CVM).

---

## Quick Start

### Create Dataset
#### The create_dataset function is responsible for downloading raw financial
files from CVM, processesing these files and saving the data for local access:

```python
import bfinance as bf

### Starting the dataset
bf.create_dataset()

## Show dataset info
bf.dataset_info()

## Search a company in dataset by name
bf.search_company('petro')
```

### The Company Class
```python
# Both CVM (regulator) ID or Fiscal ID can be used for company identity
import bfinance as bf

petro = bf.Company(9512)

# show company info
petro.info()

# show company assets in Brazilian currency 
petro.report(rtype='assets')

# show company liabilities with different arguments
petro.report(
    rtype='liabilities',
    accounting_method='separate', # in separate accounting basis
    unit=1_000_000, # in BRL million
    account_level=3, # show account up to level 3 of detail
    first_period='2015-01-01'
)
```
---
## Installation

Install `bfinance` using `pip`:

``` {.sourceCode .bash}
$ pip install bfinance --upgrade --no-cache-dir
```

To install `bfinance` using `conda`, see
[this](https://anaconda.org/carloscarvalho/BFinance).

### Requirements

-   [Python](https://www.python.org) \>= 3.8+
-   [Pandas](https://github.com/pydata/pandas) (tested to work with \>= 1.4.0)
-   [Numpy](http://www.numpy.org) (tested to work with \>= 1.18.5)
-   [requests](http://docs.python-requests.org/en/master/) \>= 2.27.1
-   [zstandard](https://pypi.org/project/zstandard/) \>= 0.17.0


---

### Legal Stuff

**BFinance** is distributed under the **MIT License**. See
the [LICENSE.txt](./LICENSE.txt) file in the release for details.

---

### P.S.

All contributors are welcome, from beginner to advanced.

**Carlos Carvalho**