# -*- coding: utf-8 -*-
#
# BFinance - Brazilian Company Financial Data
# https://github.com/crdcj/BFinance
#
# Copyright 2022 Carlos Carvalho
#
from . import version
from .dataset import update_dataset
from .dataset import create_dataset
from .dataset import search_company
from .dataset import dataset_info
from .company import Company

__version__ = version.version
__author__ = "Carlos Carvalho"
__all__ = [
    'Company',
    'update_dataset',
    'create_dataset',
    'search_company',
    'dataset_info'
]
