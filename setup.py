#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# FinLogic - finance toolkit for listed Brazilian companies
# https://github.com/crdcj/FinLogic

"""FinLogic - finance toolkit for listed Brazilian companies"""

from setuptools import setup, find_packages
import io
from os import path

# --- get version ---
version = "unknown"
with open("finlogic/version.py") as f:
    line = f.read().strip()
    version = line.replace("version = ", "").replace('"', "")
# --- /get version ---

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with io.open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="finlogic",
    version=version,
    description="Finance toolkit for listed Brazilian companies",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/crdcj/FinLogic",
    author="Carlos Carvalho",
    author_email="cr.cj@outlook.com",
    license="MIT",
    classifiers=[
        # 'Development Status :: 3 - Alpha',
        # 'Development Status :: 4 - Beta',
        "Development Status :: 5 - Production/Stable",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: MIT License",
        "Intended Audience :: Developers",
        "Topic :: Office/Business :: Financial",
        "Topic :: Office/Business :: Financial :: Investment",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python :: 3.10",
    ],
    platforms=["any"],
    keywords="pandas, requests, cvm, finance, investment, accounting",
    packages=find_packages(exclude=["docs", "tests", "dev", "backup"]),
    python_requires=">=3.10",
    install_requires=[
        "pandas>=1.4.0",
        "numpy>=1.18.5",
        "requests>=2.27.0",
        "zstandard>=0.17.0",
    ],
)

print(
    """
NOTE: FinLogic is **not** affiliated, endorsed, or vetted by the Securities
and Exchange Commission of Brazil (CVM). It's an open-source tool that uses CVM
publicly available data and is intended for research and educational purposes.
"""
)
