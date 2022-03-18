# Brazilian corporation financial statements and performance indicators in
# Pandas dataframe format


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
Pandas dataframe format, from data made publicly avaible by the securities
market authority in Brazil (CVM)

---

## Quick Start

### The dataset module

```python
import brfinance as bf

### The dataset module


### The corporation module
petro = bf.Corporation("MSFT")

# get corporation info
petro.info


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