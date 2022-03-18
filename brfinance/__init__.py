# -*- coding: utf-8 -*-
#
# brfinance - Brazilian Finance Data for Corporations
# https://github.com/...
#
# Copyright 2022 Carlos Carvalho
#
from . import version
from .dataset import update_dataset
from .corporation import Corporation

__version__ = version.version
__author__ = "Carlos Carvalho"
__all__ = ['Finance', 'update_dataset']