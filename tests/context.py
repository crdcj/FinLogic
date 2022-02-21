"""Testing module paths."""
import os
import sys
import importlib
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import brfin.company  # noqa
from brfin.company import Company  # noqa
importlib.reload(brfin.company)
