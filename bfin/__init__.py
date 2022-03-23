# -*- coding: utf-8 -*-
#
# BFin - Brazilian Corporation Financial Data
# https://github.com/crdcj/bfin
#
# Copyright 2022 Carlos Carvalho
#
from . import version
from .dataset import update_dataset, search_in_dataset, dataset_info
from .corporation import Corporation

# create_dataset points to update_dataset
create_dataset = update_dataset

__version__ = version.version
__author__ = "Carlos Carvalho"
__all__ = ['Corporation', 'update_dataset']