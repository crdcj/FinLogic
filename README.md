[![PyPI version](https://img.shields.io/pypi/v/finlogic.svg)](https://pypi.python.org/pypi/finlogic)
[![Made with Python](https://img.shields.io/badge/Python->=3.10-blue?logo=python&logoColor=white)](https://python.org "Go to Python homepage")
[![License](https://img.shields.io/badge/License-MIT-blue)](#license)
[![Code Style: black](https://img.shields.io/badge/code%20style-black-blue.svg)](https://github.com/psf/black)

<!-- [![Anaconda-Server Badge](https://anaconda.org/conda-forge/finlogic/badges/version.svg)](https://anaconda.org/conda-forge/finlogic)" -->

## FinLogic: finance toolkit for listed Brazilian companies.

---

**FinLogic** offers a Pythonic way to analyze financial data of listed companies in Brazil from information made publicly avaible by local securities market authority (CVM). FinLogic uses DuckDB as an OLAP database management system and Pandas to process the data to and from the locally builted accounting database (aprox. 21 million rows).

---

## Installation

The source code is currently hosted on GitHub at:
https://github.com/crdcj/FinLogic

Binary installers for the latest released version are available at the [Python
Package Index (PyPI)](https://pypi.org/project/finlogic) and on [Conda](https://anaconda.org/conda-forge/finlogic).

```sh
# PyPI:
pip install finlogic
```

<!--
```sh
# Conda:
conda install -c conda-forge finlogic
```
-->

### Requirements

- [Python](https://www.python.org) \>= 3.10
- [DuckDB](https://github.com/pydata/pandas) \>= 0.7.0)
- [Pandas](https://github.com/pydata/pandas) \>= 1.4.0)
- [Requests](http://docs.python-requests.org/en/master/) \>= 2.27.0

---

## Quick Start

### Create FinLogic Local Database

The 'update_database' function is responsible for downloading raw financial files from CVM, processesing aprox. 21 millions rows of accounting values and loading it into FinLogic Database for local data analysis.
In the firt run, the process can take more than 1 minute depending on CVM Server connection and local CPU power. For subsequent updates, only updated CVM files will be processed and loaded into the database, which will be a lot faster.

```python
>>> import finlogic as fl

# Compile FinLogic database for the first time:
>>> fl.update_database()

Updating CVM raw files...
...
FinLogic database updated ✅

# Show database info:
>>> fl.database_info()
```

| FinLogic Database Info      |                       Value |
| :-------------------------- | --------------------------: |
| Data path                   | /.../FinLogic/finlogic/data |
| File size (MB)              |                       301.0 |
| Last update call            |         2022-04-20 07:29:08 |
| Last modified               |         2022-04-20 07:31:48 |
| Last updated data           |         2022-04-17 13:09:01 |
| Accounting rows             |                  18,757,249 |
| Unique accounting codes     |                       2,008 |
| Number of companies         |                       1,037 |
| Unique Financial Statements |                      13,172 |
| First Financial Statement   |                  2009-01-31 |
| Last Financial Statement    |                  2023-03-31 |

```python
# Search for a company in database:
>>> fl.search_company('petro')
```

|     | name                                   |    id | fiscal_id          |
| --: | :------------------------------------- | ----: | :----------------- |
|   0 | 3R PETROLEUM ÓLEO E GÁS S.A.           | 25291 | 12.091.809/0001-55 |
|   1 | PETRO RIO S.A.                         | 22187 | 10.629.105/0001-68 |
|   2 | PETROBRAS DISTRIBUIDORA S/A            | 24295 | 34.274.233/0001-02 |
|   3 | PETROLEO BRASILEIRO S.A. PETROBRAS     |  9512 | 33.000.167/0001-01 |
|   4 | PETROLEO LUB DO NORDESTE SA            |  9520 | 07.275.159/0001-68 |
|   5 | PETRORECÔNCAVO S.A.                    | 25780 | 03.342.704/0001-30 |
|   6 | PRONOR PETROQUIMICA SA                 |  9784 | 13.552.070/0001-02 |
|   7 | REFINARIA DE PETROLEOS MANGUINHOS S.A. |  9989 | 33.412.081/0001-96 |

### The Company Class

The Company Class allows you to easily access financial data from Brazilian companies. All values are in local currency (Real).

```python
# Create a Company object to acces its financial data:
# Both CVM (regulator) ID or Fiscal ID can be used as an identifier.
>>> petro = fl.Company(9512, acc_method='separate', acc_unit='million')

# Change company accounting method back to consolidated (default):
>>> petro.acc_method = 'consolidated'

# Change company accounting unit to billion (default is 1):
>>> petro.acc_unit = 'billion'

# Show company info:
>>> petro.info()
```

| Company Info               |                             Values |
| :------------------------- | ---------------------------------: |
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
# Show company assets in Brazilian currency:
>>> petro.report(report_type='assets')
...
# Show company liabilities with custom arguments:
>>> petro.report(report_type='debt', acc_level=4, num_years=3)
```

| acc_code   | acc_name            | acc_fixed | 2020-12-31 | 2021-12-31 | 2022-12-31 |
| :--------- | :------------------ | :-------- | ---------: | ---------: | ---------: |
| 2.01.04    | Loans and Financing | True      |     51.364 |     50.631 |      47.65 |
| 2.01.04.01 | Loans and Financing | True      |     21.751 |     20.316 |     18.656 |
| 2.01.04.02 | Debentures          | True      |          0 |          0 |          0 |
| 2.01.04.03 | Lease Financing     | True      |     29.613 |     30.315 |     28.994 |
| 2.02.01    | Loans and Financing | True      |    341.184 |    277.187 |    233.053 |
| 2.02.01.01 | Loans and Financing | True      |    258.287 |    178.908 |     137.63 |
| 2.02.01.02 | Debentures          | True      |          0 |          0 |          0 |
| 2.02.01.03 | Lease Financing     | True      |     82.897 |     98.279 |     95.423 |

```python
# Change account names to Portuguese:
>>> petro.language = "portuguese"
>>> petro.report(report_type='debt', acc_level=4, num_years=3)
```

| acc_code   | acc_name                       | acc_fixed | 2020-12-31 | 2021-12-31 | 2022-12-31 |
| ---------- | ------------------------------ | --------- | ---------: | ---------: | ---------: |
| 2.01.04    | Empréstimos e Financiamentos   | True      |     51.364 |     50.631 |      47.65 |
| 2.01.04.01 | Empréstimos e Financiamentos   | True      |     21.751 |     20.316 |     18.656 |
| 2.01.04.02 | Debêntures                     | True      |          0 |          0 |          0 |
| 2.01.04.03 | Financiamento por Arrendamento | True      |     29.613 |     30.315 |     28.994 |
| 2.02.01    | Empréstimos e Financiamentos   | True      |    341.184 |    277.187 |    233.053 |
| 2.02.01.01 | Empréstimos e Financiamentos   | True      |    258.287 |    178.908 |     137.63 |
| 2.02.01.02 | Debêntures                     | True      |          0 |          0 |          0 |
| 2.02.01.03 | Financiamento por Arrendamento | True      |     82.897 |     98.279 |     95.423 |

```python
# Show company main indicators:
>>> petro.indicators(num_years=3)
```

| Company Financial Indicators | 2019-12-31 | 2020-12-31 | 2021-12-31 |
| :--------------------------- | ---------: | ---------: | ---------: |
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

**Felipe Costa and Carlos Carvalho**

<table border=1 cellpadding=10><tr><td>

#### \*\*\* IMPORTANT LEGAL DISCLAIMER \*\*\*

---

**FinLogic** is **not** affiliated, endorsed or vetted by the Securities and Exchange Commission of Brazil (CVM). It's an open-source tool that uses CVM publicly available data and is intended for research and educational purposes. This finance tool is distributed under the **MIT License** (see the [LICENSE](./LICENSE) file in the release for details).

---

</td></tr></table>
