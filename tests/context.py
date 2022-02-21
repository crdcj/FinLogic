"""Testing module paths."""
import os
import sys
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
for item in sys.path:
    print(item)

from brfin.company import Company  # noqa
