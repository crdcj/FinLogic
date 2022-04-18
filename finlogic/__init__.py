# -*- coding: utf-8 -*-
#
# FinLogic - Brazilian Company Financial Data
# https://github.com/crdcj/FinLogic
#
# Copyright 2022 Carlos Carvalho
#
from . import config  # noqa
from . import version
from .database import update_database, search_company, database_info
from .company import Company


__version__ = version.version
__author__ = "Carlos Carvalho"

__all__ = [
    "Company",
    "update_database",
    "search_company",
    "database_info",
]
