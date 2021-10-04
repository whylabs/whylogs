import os
import sys

import pytest

_MY_DIR = os.path.realpath(os.path.dirname(__file__))
# Allow import of the test utilities packages
sys.path.insert(0, os.path.join(_MY_DIR, os.pardir, "helpers"))
# Test the parent package
sys.path.insert(0, os.path.join(_MY_DIR, os.pardir))
# Verify whylogs is importable


def pytest_addoption(parser):
    parser.addoption("--load", action="store_true", default=False, help="run load tests")


def pytest_configure(config):
    config.addinivalue_line("markers", "load: mark test as load to skip running with unit tests")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--load"):
        # --integ specified on command line: do not skip integ tests
        return
    skip_load_test = pytest.mark.skip(reason="need --load option to run")
    for item in items:
        if "load" in item.keywords:
            item.add_marker(skip_load_test)


@pytest.fixture(scope="session")
def df_lending_club():
    import pandas as pd

    df = pd.read_csv(os.path.join(_MY_DIR, "lending_club_1000.csv"))
    return df.head(20)
