import os
import sys

_MY_DIR = os.path.realpath(os.path.dirname(__file__))
# Allow import of the test utilities packages
sys.path.insert(0, os.path.join(_MY_DIR, os.pardir, "helpers"))
# Test the parent package
sys.path.insert(0, os.path.join(_MY_DIR, os.pardir))
# Verify whylabs is importable
import whylabs
