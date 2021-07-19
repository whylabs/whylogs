import os
import sys

import pytest

import whylogs

_MY_DIR = os.path.realpath(os.path.dirname(__file__))
# Allow import of the test utilities packages
sys.path.insert(0, os.path.join(_MY_DIR, os.pardir, "helpers"))
# Test the parent package
sys.path.insert(0, os.path.join(_MY_DIR, os.pardir))
# Verify whylogs is importable


@pytest.fixture(scope="session")
def df_lending_club():
    import pandas as pd

    df = pd.read_csv(os.path.join(_MY_DIR, "lending_club_1000.csv"))
    return df.head(20)
