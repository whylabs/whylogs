import os

import pandas as pd
import pytest

import whylogs as why
from whylogs import ResultSet
from whylogs.core.constraints.metric_constraints import (
    Constraints,
    ConstraintsBuilder,
    MetricConstraint,
    MetricsSelector,
)
from whylogs.core.view.dataset_profile_view import DatasetProfileView
from whylogs.viz import SummaryDriftReport

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


@pytest.fixture(scope="session")
def pandas_dataframe() -> pd.DataFrame:
    data = {
        "animal": ["cat", "hawk", "snake", "cat"],
        "legs": [4, 2, 0, 4],
        "weight": [4.3, 1.8, None, 4.1],
    }

    df = pd.DataFrame(data)
    return df


@pytest.fixture(scope="session")
def pandas_constraint_dataframe() -> pd.DataFrame:
    data = {
        "animal": ["cat", "hawk", "snake", "cat", "mosquito"],
        "legs": [4, 2, 0, 4, 6],
        "weight": [4.3, 1.8, 1.3, 4.1, 5.5e-6],
    }

    df = pd.DataFrame(data)
    return df


@pytest.fixture(scope="session")
def max_less_than_equal_constraints(profile_view) -> Constraints:
    constraints_builder = ConstraintsBuilder(dataset_profile_view=profile_view)
    metric_constraint = MetricConstraint(
        name="legs less than 12",
        condition=lambda x: x.max < 12,
        metric_selector=MetricsSelector(metric_name="distribution", column_name="legs"),
    )
    constraints_builder.add_constraint(metric_constraint)
    return constraints_builder.build()


@pytest.fixture(scope="session")
def result_set(pandas_dataframe) -> ResultSet:
    results = why.log(pandas=pandas_dataframe)
    return results


@pytest.fixture(scope="session")
def profile_view(result_set) -> DatasetProfileView:
    profile_view = result_set.view()
    return profile_view


@pytest.fixture(scope="session")
def profile_view_zero_mean() -> DatasetProfileView:
    data = {
        "animal": ["cat", "hawk", "snake", "cat"],
        "legs": [4, 2, 0, 4],
        "weight": [1, -1, 2, -2],
    }

    df = pd.DataFrame(data)

    results = why.log(pandas=df)
    profile_view = results.view()

    return profile_view


@pytest.fixture(scope="session")
def html_report(profile_view):
    report = SummaryDriftReport(target_view=profile_view, ref_view=profile_view)
    return report
