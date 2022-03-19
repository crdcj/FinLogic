# -*- coding: utf-8 -*-
#
# brfinance - Brazilian Corporation Financial Data
# https://github.com/crdcj/BrFinance
#
# Copyright 2022 Carlos Carvalho
#
from . import version
from .dataset import update_dataset
from .corporation import Corporation

# create_dataset points to update_dataset
create_dataset = update_dataset

__version__ = version.version
__author__ = "Carlos Carvalho"
__all__ = ['Corporation', 'update_dataset']