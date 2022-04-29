import os

import pandas as pd
import pytest

_MY_DIR = os.path.realpath(os.path.dirname(__file__))
_DATA_DIR = os.path.join(_MY_DIR, "testdata")

os.environ["HEAP_APPID_DEV"] = "3422045963"


def pytest_addoption(parser) -> None:  # type: ignore
    parser.addoption("--load", action="store_true", default=False, help="run load tests")


def pytest_configure(config) -> None:  # type: ignore
    config.addinivalue_line("markers", "load: mark test as load to skip running with unit tests")


def pytest_collection_modifyitems(config, items) -> None:  # type: ignore
    if config.getoption("--load"):
        # --integ specified on command line: do not skip integ tests
        return
    skip_load_test = pytest.mark.skip(reason="need --load option to run")
    for item in items:
        if "load" in item.keywords:
            item.add_marker(skip_load_test)


@pytest.fixture(scope="session")
def lending_club_df() -> pd.DataFrame:
    df = pd.read_csv(os.path.join(_DATA_DIR, "lending_club_1000.csv"))
    return df


@pytest.fixture(scope="session")
def v0_profile_path() -> str:
    return os.path.join(_DATA_DIR, "v0_profile.bin")
