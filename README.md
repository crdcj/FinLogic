[![PyPI version](https://img.shields.io/pypi/v/finlogic.svg)](https://pypi.python.org/pypi/finlogic)
[![Made with Python](https://img.shields.io/badge/Python->=3.10-blue?logo=python&logoColor=white)](https://python.org "Go to Python homepage")
[![License](https://img.shields.io/badge/License-MIT-blue)](#license)
[![Code Style: black](https://img.shields.io/badge/code%20style-black-blue.svg)](https://github.com/psf/black)

<!-- [![Anaconda-Server Badge](https://anaconda.org/conda-forge/finlogic/badges/version.svg)](https://anaconda.org/conda-forge/finlogic)" -->

## FinLogic: finance toolkit for listed Brazilian companies.

---

**FinLogic** provides a Pythonic approach to analyzing the financial data of companies listed in Brazil. The library pre-processes approximately 50 million accounting entries from the local securities market authority data repository.

The extensive pre-processing stage is automated using Polars within an AWS Lambda Function, and it is scheduled to check for updates overnight. After the CVM repository data is updated and pre-processed, the job saves the cleaned data in FinLogic data folder on GitHub. This arrangement allows the library to access the data quickly and easily.

---

## Installation

The source code is currently hosted on GitHub at:
https://github.com/crdcj/FinLogic

Binary installers for the latest released version are available at the [Python Package Index (PyPI)](https://pypi.org/project/finlogic).

```sh
# PyPI:
pip install finlogic
```

### Requirements

- [Python](https://www.python.org) \>= 3.10
- [Pandas](https://github.com/pydata/pandas) \>= 1.5.0

---

## Quick Start

### Load FinLogic Data

The 'load' function is responsible for downloading and reading the financial data stored on GitHub data folder.

```python
>>> import finlogic as fl

# Load the accounting data in memory:
>>> fl.load()
```
    Loading "language" data...
    Loading trading data...
    Loading financials data...
    Building indicators data...
    ✔ FinLogic is ready!

```python
# Show database info:
>>> fl.info()
```

|                     |                    FinLogic Info |
| :------------------ | -------------------------------: |
| data_url            | https://raw.githubusercontent... |
| memory_usage        |                         255.1 MB |
| accounting_entries  |                          755,635 |
| number_of_reports   |                            2,635 |
| first_report        |                       2009-01-31 |
| last_report         |                       2023-03-31 |
| number_of_companies |                              210 |

```python
# Search for a company in database:
>>> fl.search_company('petro')
```

| name_id                            | cvm_id | tax_id             | segment        | is_restructuring | most_traded_stock |
|:-----------------------------------|-------:|:-------------------|:---------------|:-----------------|:------------------|
| PETROLEO BRASILEIRO S.A. PETROBRAS |   9512 | 33.000.167/0001-01 | exploration... | False            | PETR4             |
| 3R PETROLEUM ÓLEO E GÁS S.A.       |  25291 | 12.091.809/0001-55 | exploration... | False            | RRRP3             |
| PETRORECÔNCAVO S.A.                |  25780 | 03.342.704/0001-30 | exploration... | False            | RECV3             |

```python
# Search company by segment:
fl.search_company(search_by="segment", search_value="electric")
```
| name_id                                | cvm_id | tax_id             | segment            | is_restructuring | most_traded_stock |
|:---------------------------------------|-------:|:-------------------|:-------------------|:-----------------|:------------------|
| CENTRAIS ELET BRAS S.A. - ELETROBRAS   |   2437 | 00.001.180/0001-26 | electric utilities | False            | ELET3             |
| CIA ENERGETICA DE MINAS GERAIS - CEMIG |   2453 | 17.155.730/0001-64 | electric utilities | False            | CMIG4             |
| CIA PARANAENSE DE ENERGIA - COPEL      |  14311 | 76.483.817/0001-20 | electric utilities | False            | CPLE6             |
| CIA ENERGETICA DO CEARA - COELCE       |  14869 | 07.047.251/0001-70 | electric utilities | False            | COCE5             |
| ENERGISA S.A.                          |  15253 | 00.864.214/0001-06 | electric utilities | False            | ENGI11            |
| NEOENERGIA S.A.                        |  15539 | 01.083.200/0001-18 | electric utilities | False            | NEOE3             |
| ENGIE BRASIL ENERGIA S.A.              |  17329 | 02.474.103/0001-19 | electric utilities | False            | EGIE3             |
...

### The Company Class

The Company Class allows you to easily access financial data from Brazilian companies. All values are in local currency (Real).

```python
# Create a Company object to acces its financial data:
# Both CVM (regulator) ID or Fiscal ID can be used as an identifier.
>>> petro = fl.Company(9512, is_consolidated=False, acc_unit='m')

# Change company accounting method back to consolidated (default):
>>> petro.is_consolidated = True

# Change company accounting unit to billion (default is 1):
>>> petro.acc_unit = 'b'

# Show company info:
>>> petro.info()
```

|                            |                       Company Info |
| :------------------------- | ---------------------------------: |
| Name                       | PETROLEO BRASILEIRO S.A. PETROBRAS |
| CVM ID                     |                               9512 |
| Tax ID (CNPJ)              |                 33.000.167/0001-01 |
| Total Accounting Rows      |                              3,292 |
| Selected Tax Rate          |                               0.34 |
| Selected Accounting Method |                       consolidated |
| Selected Accounting Unit   |                      1,000,000,000 |
| First Report               |                         2009-12-31 |
| Last Report                |                         2023-03-31 |

```python
# Show company assets in Brazilian currency:
>>> petro.report(report_type='assets')
...
# Show company liabilities with custom arguments:
>>> petro.report(report_type='debt', acc_level=4, num_years=3)
```

| acc_code   | acc_name            | 2020-12-31 | 2021-12-31 | 2022-12-31 |
| :--------- | :------------------ | ---------: | ---------: | ---------: |
| 2.01.04    | Loans and Financing |     51.364 |     50.631 |      47.65 |
| 2.01.04.01 | Loans and Financing |     21.751 |     20.316 |     18.656 |
| 2.01.04.02 | Debentures          |          0 |          0 |          0 |
| 2.01.04.03 | Lease Financing     |     29.613 |     30.315 |     28.994 |
| 2.02.01    | Loans and Financing |    341.184 |    277.187 |    233.053 |
| 2.02.01.01 | Loans and Financing |    258.287 |    178.908 |     137.63 |
| 2.02.01.02 | Debentures          |          0 |          0 |          0 |
| 2.02.01.03 | Lease Financing     |     82.897 |     98.279 |     95.423 |

```python
# Change account names to Portuguese:
>>> petro.language = "portuguese"
>>> petro.report(report_type='debt', acc_level=4, num_years=3)
```

| acc_code   | acc_name                       | 2020-12-31 | 2021-12-31 | 2022-12-31 |
| ---------- | ------------------------------ | ---------: | ---------: | ---------: |
| 2.01.04    | Empréstimos e Financiamentos   |     51.364 |     50.631 |      47.65 |
| 2.01.04.01 | Empréstimos e Financiamentos   |     21.751 |     20.316 |     18.656 |
| 2.01.04.02 | Debêntures                     |          0 |          0 |          0 |
| 2.01.04.03 | Financiamento por Arrendamento |     29.613 |     30.315 |     28.994 |
| 2.02.01    | Empréstimos e Financiamentos   |    341.184 |    277.187 |    233.053 |
| 2.02.01.01 | Empréstimos e Financiamentos   |    258.287 |    178.908 |     137.63 |
| 2.02.01.02 | Debêntures                     |          0 |          0 |          0 |
| 2.02.01.03 | Financiamento por Arrendamento |     82.897 |     98.279 |     95.423 |

```python
# Show company main indicators:
>>> petro.indicators(num_years=3)
```

| 2021-12-31 | 2022-12-31 | 2023-03-31 |
|-----------:|-----------:|-----------:|
|    972.951 |    976.709 |    978.577 |
|    168.247 |    163.052 |    157.194 |
|     62.04  |     56.193 |     66.906 |
|     33.334 |     -0.679 |     28.744 |
|    655.359 |    588.895 |    607.53  |
|    134.913 |    163.731 |    128.45  |
|    327.818 |    280.703 |    271.031 |
|    265.778 |    224.51  |    204.125 |
|    389.581 |    364.385 |    403.405 |
|    452.668 |    641.256 |    638.683 |
|    219.637 |    334.1   |    332.645 |
|    107.264 |    189.005 |    182.529 |
|    273.879 |    362.457 |    355.838 |
|    210.831 |    294.255 |    289.054 |
|    151.575 |    274.998 |    263.614 |
|    -44.311 |    -85.993 |    -81.085 |
|    203.126 |    255.41  |    256.345 |
|     63.048 |     68.202 |     66.784 |
|      0.292 |      0.312 |      0.307 |
|      0.141 |      0.199 |      0.192 |
|      0.397 |      0.515 |      0.453 |
|      0.214 |      0.312 |      0.309 |
|      0.485 |      0.521 |      0.520 |
|      0.605 |      0.565 |      0.557 |
|      0.465 |      0.458 |      0.452 |
|      0.236 |      0.294 |      0.285 |
|      8.18  |     14.44  |     13.95  |

---

P.S.: All contributors are welcome, from beginner to advanced.

**Felipe Costa and Carlos Carvalho**

<table border=1 cellpadding=10><tr><td>

#### \*\*\* IMPORTANT LEGAL DISCLAIMER \*\*\*

---

**FinLogic** is **not** affiliated, endorsed or vetted by the Securities and Exchange Commission of Brazil (CVM). It's an open-source tool that uses CVM publicly available data and is intended for research and educational purposes. This finance tool is distributed under the **MIT License** (see the [LICENSE](./LICENSE) file in the release for details).

---

</td></tr></table>
