# -*- coding: utf-8 -*-
#
# FinLogic - Brazilian Company Financial Data
# https://github.com/crdcj/FinLogic
#
# Copyright 2024 Carlos Carvalho
#
from importlib.metadata import version

from .company import Company
from .data import info, load, rank, search_company, search_segment

__version__ = version("FinLogic")
__author__ = "Carlos Carvalho"

__all__ = ["Company", "load", "search_company", "info", "search_segment", "rank"]
