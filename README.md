[![PyPI version](https://img.shields.io/pypi/v/finlogic.svg)](https://pypi.python.org/pypi/finlogic)
[![Anaconda-Server Badge](https://anaconda.org/conda-forge/finlogic/badges/version.svg)](https://anaconda.org/conda-forge/finlogic) 
[![PyPI Status](https://img.shields.io/pypi/status/finlogic.svg)](https://pypi.python.org/pypi/finlogic)
[![PyPI Downloads](https://img.shields.io/pypi/dm/finlogic.svg)](https://pypi.python.org/pypi/finlogic)
[![Python Version](https://img.shields.io/badge/python-3.10-blue.svg)](https://pypi.python.org/pypi/finlogic)
[![Anaconda License](https://anaconda.org/conda-forge/finlogic/badges/license.svg)](https://anaconda.org/conda-forge/finlogic)
[![Code Style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Twitter](https://img.shields.io/twitter/follow/CRCarvalhoJ.svg?style=social&label=Follow)](https://twitter.com/CRCarvalhoJ)

## FinLogic: finance data analysis toolkit for listed Brazilian companies


<table border=1 cellpadding=10><tr><td>

#### \*\*\* IMPORTANT LEGAL DISCLAIMER \*\*\*

---

**FinLogic** is **not** affiliated, endorsed or vetted by the Securities and Exchange Commission of Brazil (CVM). It's an open-source tool that uses CVM publicly available data and is intended for research and educational purposes. This finance tool is distributed under the **MIT License** (see the [LICENSE](./LICENSE) file in the release for details).

---

</td></tr></table>

---

**FinLogic** offers a Pythonic way to analyze financial data of listed companies in Brazil from information made publicly avaible by local securities market authority (CVM). This finance tool is built on top of Pandas.

---

## Installation
The source code is currently hosted on GitHub at:
https://github.com/crdcj/FinLogic

Binary installers for the latest released version are available at the [Python
Package Index (PyPI)](https://pypi.org/project/finlogic) and on [Conda](https://anaconda.org/conda-forge/finlogic).

```sh
# conda
conda install -c conda-forge finlogic 
```

```sh
# or PyPI
pip install finlogic
```

### Requirements
-   [Python](https://www.python.org) \>= 3.10
-   [Pandas](https://github.com/pydata/pandas) \>= 1.4.0)
-   [Numpy](http://www.numpy.org) \>= 1.18.5)
-   [requests](http://docs.python-requests.org/en/master/) \>= 2.27.0
-   [zstandard](https://pypi.org/project/zstandard/) \>= 0.17.0

---

## Quick Start
### Create FinLogic Local Database
The 'update_database' function is responsible for downloading raw financial files from CVM, processesing aprox. 18 millions rows of accounting values and storing it into a single Pandas DataFrame compressed file for local data analysis.
In the firt run, the update process can take some minutes depending on CVM Server connection and local CPU power. The database generation needs at least 8 GB of free memory and a quad core processor is recommended.
For subsequent updates, only updated CVM files will be processed and inserted into the database, which will consume less CPU power.

```python
import finlogic as fl

# Starting FinLogic for the first time
fl.update_database()

Updating CVM raw files...
...
FinLogic database updated ✅

# Show database info
fl.database_info()
```

| FinLogic Database Info      |        Value |
|:----------------------------|-------------:|
| Size in Memory (MB)         |        626.2 |
| Accounting Rows             | 18,754,523.0 |
| Unique Accounting Codes     |      2,007.0 |
| Companies                   |      1,093.0 |
| Unique Financial Statements |     12,137.0 |
| First Financial Statement   |   2009-01-31 |
| Last Financial Statement    |   2022-03-31 |

```python
# Search a company name in database
fl.search_company('petro')
```

|   | co_name                                | cvm_id | fiscal_id          |
|--:|:---------------------------------------|-------:|:-------------------|
| 0 | 3R PETROLEUM ÓLEO E GÁS S.A.           |  25291 | 12.091.809/0001-55 |
| 1 | PETRO RIO S.A.                         |  22187 | 10.629.105/0001-68 |
| 2 | PETROBRAS DISTRIBUIDORA S/A            |  24295 | 34.274.233/0001-02 |
| 3 | PETROLEO BRASILEIRO S.A. PETROBRAS     |   9512 | 33.000.167/0001-01 |
| 4 | PETROLEO LUB DO NORDESTE SA            |   9520 | 07.275.159/0001-68 |
| 5 | PETRORECÔNCAVO S.A.                    |  25780 | 03.342.704/0001-30 |
| 6 | PRONOR PETROQUIMICA SA                 |   9784 | 13.552.070/0001-02 |
| 7 | REFINARIA DE PETROLEOS MANGUINHOS S.A. |   9989 | 33.412.081/0001-96 |

### The Company Class
The Company Class allows you to easily access financial data from Brazilian companies. All values are in local currency (Real).

```python
import finlogic as fl
# Create a Company object to acces its fiancial data.
# Both CVM (regulator) ID or Fiscal ID can be used as an identifier.
petro = fl.Company(9512, acc_method='separate', acc_unit='million')

# Change company accouting method back to default = 'consolidated'
petro.acc_method = 'consolidated'

# Change company accouting unit to billion (default is 1)
petro.acc_unit = 'billion'

# show company info
petro.info()
```

| Company Info               |                             Values |
|:---------------------------|-----------------------------------:|
| Name                       | PETROLEO BRASILEIRO S.A. PETROBRAS |
| CVM ID                     |                               9512 |
| Fiscal ID (CNPJ)           |                 33.000.167/0001-01 |
| Total Accounting Rows      |                             39,292 |
| Selected Tax Rate          |                               0.34 |
| Selected Accounting Method |                       consolidated |
| Selected Accounting Unit   |                      1,000,000,000 |
| First Annual Report        |                         2009-12-31 |
| Last Annual Report         |                         2021-12-31 |
| Last Quarterly Report      |                         2021-09-30 |

```python
# show company assets in Brazilian currency 
petro.report(report_type='assets')
...
# show company liabilities with custom arguments
petro.report(report_type='debt', acc_level=4, num_years=3)
```

|   | acc_name               | acc_code   | acc_fixed | 2019-12-31 | 2020-12-31 | 2021-12-31 |
|:--|:-----------------------|:-----------|:---------:|-----------:|-----------:|-----------:|
| 0 | Empréstimos e Financi. | 2.01.04    |   True    |     41.139 |     51.364 |     50.631 |
| 1 | Empréstimos e Financi. | 2.01.04.01 |   True    |     18.013 |     21.751 |     20.316 |
| 2 | Debêntures             | 2.01.04.02 |   True    |          0 |          0 |      0     |
| 3 | Financiamento por Arr. | 2.01.04.03 |   True    |     23.126 |     29.613 |     30.315 |
| 4 | Empréstimos e Financi. | 2.02.01    |   True    |    310.022 |    341.184 |    277.187 |
| 5 | Empréstimos e Financi. | 2.02.01.01 |   True    |    236.969 |    258.287 |    178.908 |
| 6 | Debêntures             | 2.02.01.02 |   True    |          0 |          0 |      0     |
| 7 | Financiamento por Arr. | 2.02.01.03 |   True    |     73.053 |     82.897 |     98.279 |

```python
# show company main indicators
petro.indicators(num_years=3)
```

| Company Financial Indicators | 2019-12-31 | 2020-12-31 | 2021-12-31 |
|:-----------------------------|-----------:|-----------:|-----------:|
| revenues                     |    302.245 |    272.069 |    452.668 |
| operating_cash_flow          |    101.766 |    148.106 |    203.126 |
| ebitda                       |    140.203 |    107.926 |    273.879 |
| ebit                         |     81.701 |     49.621 |    210.831 |
| net_income                   |     40.970 |      6.246 |    107.264 |
| total_cash                   |     33.294 |     64.280 |     62.040 |
| total_debt                   |    351.161 |    392.548 |    327.818 |
| net_debt                     |    317.867 |    328.268 |    265.778 |
| working_capital              |     -4.046 |      6.036 |     33.334 |
| invested_capital             |    617.004 |    639.418 |    655.359 |
| return_on_assets             |      0.062 |      0.035 |      0.140 |
| return_on_capital            |      0.097 |      0.053 |      0.217 |
| return_on_equity             |      0.144 |      0.020 |      0.344 |
| gross_margin                 |      0.403 |      0.455 |      0.485 |
| ebitda_margin                |      0.463 |      0.396 |      0.605 |
| operating_margin             |      0.178 |      0.120 |      0.307 |
| net_margin                   |      0.135 |      0.022 |      0.236 |

---

P.S.: All contributors are welcome, from beginner to advanced.

**Carlos Carvalho**
