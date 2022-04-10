import pytest


def pytest_addoption(parser) -> None:
    parser.addoption("--load", action="store_true", default=False, help="run load tests")


def pytest_configure(config) -> None:
    config.addinivalue_line("markers", "load: mark test as load to skip running with unit tests")


def pytest_collection_modifyitems(config, items) -> None:
    if config.getoption("--load"):
        # --integ specified on command line: do not skip integ tests
        return
    skip_load_test = pytest.mark.skip(reason="need --load option to run")
    for item in items:
        if "load" in item.keywords:
            item.add_marker(skip_load_test)
