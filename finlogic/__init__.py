# -*- coding: utf-8 -*-
#
# FinLogic - Brazilian Company Financial Data
# https://github.com/crdcj/FinLogic
#
# Copyright 2022 Carlos Carvalho
#
from . import config  # noqa
from .data_manager import update, search_company, info, search_segment, rank
from .company import Company


__version__ = "0.5.2"
__author__ = "Carlos Carvalho"

__all__ = ["Company", "update", "search_company", "info", "search_segment", "rank"]
