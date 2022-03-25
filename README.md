## Finatic: finance data analysis toolkit for listed Brazilian companies


<table border=1 cellpadding=10><tr><td>

#### \*\*\* IMPORTANT LEGAL DISCLAIMER \*\*\*

---

Finatic is **not** affiliated, endorsed, or vetted by the Securities and
Exchange Commission of Brazil (CVM). It's an open-source tool that uses CVM
publicly available data and is intended for research and educational purposes.

</td></tr></table>

---

**Finatic** offers a Pythonic/Pandas way to analyze financial data of listed
companies in Brazil from information made publicly avaible by local securities
market authority (CVM).

---

## Quick Start

### Create Finatic local database

The 'update_database' function is responsible for downloading raw financial
files from CVM, processesing aprox. 18 millions rows of accounting values and
storing it into a single Pandas DataFrame compressed file for local data
analysis. The update process can take some minutes depending on CVM Server
connection and local data processing power.

```python
import finatic as fi

### Starting Finatic for the first time
fi.update_database()

## Show database info
fi.database_info()

## Search a company in database by name
fi.search_company('petro')
```

### The Company Class

The Company Class allows you to easily access financial data from Brazilian
companies. All values are in local currency (Real).
```python

import finatic as fi
# Create a Company object to acces its fiancial data.
# Both CVM (regulator) ID or Fiscal ID can be used as an identifier.
petro = fi.Company(9512, acc_method='separate', acc_unit='million')

# Change company accouting method to 'separate' (default is 'consolidated')
petro.acc_method = 'consolidated'

# Change company accouting unit to billion (default is 1)
petro.acc_unit = 'billion'

# show company info
petro.info()

# show company assets in Brazilian currency 
petro.report(report_type='assets')

# show company liabilities with custom arguments
petro.report(
    report_type='liabilities',
    acc_level=3, # show accounts up to level 3 of detail (default is to show all accounts)
    num_years=5
)
# show company main indicators
petro.indicators(num_years=5)
```
---
## Installation

Install `finatic` using `pip`:

``` {.sourceCode .bash}
$ pip install finatic --upgrade --no-cache-dir
```

### Requirements

-   [Python](https://www.python.org) \>= 3.8+
-   [Pandas](https://github.com/pydata/pandas) (tested to work with \>= 1.4.0)
-   [Numpy](http://www.numpy.org) (tested to work with \>= 1.18.5)
-   [requests](http://docs.python-requests.org/en/master/) \>= 2.27.1
-   [zstandard](https://pypi.org/project/zstandard/) \>= 0.17.0

---

### Legal Stuff

**Finatic** is distributed under the **MIT License**. See
the [LICENSE.txt](./LICENSE.txt) file in the release for details.

---

### P.S.

All contributors are welcome, from beginner to advanced.

**Carlos Carvalho**