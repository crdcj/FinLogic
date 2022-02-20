"""Testing module paths."""
import sys
import os
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
for item in sys.path:
    print(item)
