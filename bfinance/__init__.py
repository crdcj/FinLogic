# -*- coding: utf-8 -*-
#
# BFinance - Brazilian Corporation Financial Data
# https://github.com/crdcj/BFinance
#
# Copyright 2022 Carlos Carvalho
#
from . import version
from .dataset import update_dataset
from .dataset import create_dataset
from .dataset import search_in_dataset
from .dataset import dataset_info
from .corporation import Corporation

__version__ = version.version
__author__ = "Carlos Carvalho"
__all__ = [
    'Corporation',
    'update_dataset',
    'create_dataset',
    'search_in_dataset',
    'dataset_info'
]
