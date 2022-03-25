# -*- coding: utf-8 -*-
#
# Finatic - Brazilian Company Financial Data
# https://github.com/crdcj/Finatic
#
# Copyright 2022 Carlos Carvalho
#
from . import version
from .dp import update_database, search_company, info
from .company import Company

__version__ = version.version
__author__ = "Carlos Carvalho"
__all__ = ['Company', 'update_database', 'search_company', 'info']
