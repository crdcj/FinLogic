#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# BrFinance - finance toolkit for Brazilian corporations
# https://github.com/crdcj/BrFinance

"""BrFinance - finance toolkit for Brazilian corporations"""

from setuptools import setup, find_packages
import io
from os import path

# --- get version ---
version = "unknown"
with open("brfinance/version.py") as f:
    line = f.read().strip()
    version = line.replace("version = ", "").replace('"', '')
# --- /get version ---

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with io.open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='BrFinance',
    version=version,
    description='Finance toolkit for Brazilian corporations',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/crdcj/BrFinance',
    author='Carlos Carvalho',
    author_email='carlos.r.carvalho@outlook.com.br',
    license='MIT',
    classifiers=[
        # 'Development Status :: 3 - Alpha',
        # 'Development Status :: 4 - Beta',
        'Development Status :: 5 - Production/Stable',
        'Operating System :: OS Independent',
        'Intended Audience :: Developers',
        'Topic :: Office/Business :: Financial',
        'Topic :: Office/Business :: Financial :: Investment',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python :: 3.10',
    ],
    platforms=['any'],
    keywords='pandas, requests, cvm, finance, investment',
    packages=find_packages(exclude=['contrib', 'docs', 'tests', 'examples']),
    install_requires=[
        'pandas>=1.4.0', 'numpy>=1.2.0', 'requests>=2.27.1', 'zstandard>=0.17'
    ],
    entry_points={
        'console_scripts': [
            'sample=sample:main',
        ],
    },
)

print("""
NOTE: BrFinance is **not** affiliated, endorsed, or vetted by the Securities
and Exchange Commission of Brazil (CVM). It's an open-source tool that uses CVM
publicly available data and is intended for research and educational purposes.
""")
