"""Pytest bootstrap.

Adds the repository root to ``sys.path`` so tests can import the ``core``
and ``nodes`` packages regardless of how pytest is invoked (the project is
not installed as a package).
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)