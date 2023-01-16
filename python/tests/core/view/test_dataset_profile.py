import datetime
from logging import getLogger

import numpy as np
import pandas as pd
import pytest

from whylogs.core import DatasetProfile, DatasetSchema

FLOAT_TYPES = [float, np.float16, np.float32, np.float64, np.floating, np.float_, np.longdouble]
INTEGER_TYPES = [int, np.intc, np.uintc, np.int_, np.uint, np.longlong, np.ulonglong]
TEST_LOGGER = getLogger(__name__)


@pytest.fixture
def profile():
    return DatasetProfile()


@pytest.mark.xfail(raises=ValueError)
def test_profile_creation_timestamp_utc():
    DatasetProfile(creation_timestamp=datetime.datetime.now())


@pytest.mark.xfail(raises=ValueError)
def test_profile_dataset_timestamp_utc():
    DatasetProfile(dataset_timestamp=datetime.datetime.now())


def test_basic_dataset_profile(profile) -> None:
    d = {"col1": [1, 2], "col2": [3.0, 4.0], "col3": ["a", "b"]}
    df = pd.DataFrame(data=d)

    profile.track(pandas=df)

    assert profile._columns["col1"]._schema.dtype == np.int64
    assert profile._columns["col2"]._schema.dtype == np.float64
    assert profile._columns["col3"]._schema.dtype.name == "object"


def test_override_schema_col2_as_string() -> None:
    d = {"col1": [1, 2, 3], "col2": [3.0, 4.0, "c"], "col3": ["a", "b", "c"]}
    df = pd.DataFrame(data=d)

    profile = DatasetProfile(DatasetSchema(types={"col2": str}))
    profile.track(pandas=df)
    view = profile.view()
    assert view.get_column("col1") is not None
    view2 = profile.view()
    assert view2.get_column("col1") is not None
    view.merge(view).merge(view)

    assert profile._columns["col1"]._schema.dtype == np.int64
    assert profile._columns["col2"]._schema.dtype == str
    assert profile._columns["col3"]._schema.dtype.name == "object"
    pdf = view.to_pandas()
    TEST_LOGGER.info(pdf)


def test_basic_iter_row(profile) -> None:
    d = {"col1": [1, 2, 3], "col2": [3.0, 4.0, "c"], "col3": ["a", "b", "c"]}
    df = pd.DataFrame(data=d)

    for row in df.iterrows():
        profile.track(row=row[1].to_dict())  # type: ignore

    assert profile._columns["col1"]._schema.dtype == int
    assert profile._columns["col2"]._schema.dtype == float
    assert profile._columns["col3"]._schema.dtype == str


@pytest.mark.parametrize("data_type", FLOAT_TYPES)
def test_different_float_types(profile, data_type) -> None:
    d = {"col1": [1.2, 2.2, 3.0]}
    df = pd.DataFrame(data=d, dtype=data_type)

    for row in df.iterrows():
        profile.track(row=row[1].to_dict())  # type: ignore

    assert profile._columns["col1"]._schema.dtype == float


@pytest.mark.parametrize("data_type", INTEGER_TYPES)
def test_different_int_types(profile, data_type) -> None:
    d = {"col1": [1, 2, 3, 4, 5]}
    df = pd.DataFrame(data=d, dtype=data_type)
    for row in df.iterrows():
        profile.track(row=row[1].to_dict())  # type: ignore
    assert profile._columns["col1"]._schema.dtype == int


def test_track_with_custom_schema() -> None:
    schema = DatasetSchema(types={"col1": str, "col2": np.int32, "col3": str})
    prof = DatasetProfile(schema=schema)
    df = pd.DataFrame({"col1": ["foo"], "col2": np.array([1], dtype=np.int32), "col3": ["bar"]})
    prof.track(pandas=df)
    assert prof._columns.keys() == prof._schema._columns.keys()


def test_default_dataset_timestamp() -> None:
    prof = DatasetProfile()
    t1 = datetime.datetime.now(datetime.timezone.utc)
    assert prof.dataset_timestamp is not None
    assert prof.dataset_timestamp.tzinfo
    timestamp_delta = t1.timestamp() - prof.dataset_timestamp.timestamp()

    assert timestamp_delta >= 0  # no default timestamps in the future!

    # the time in seconds between DatasetProfile creation and t1 assignment should be relatively small
    assert timestamp_delta < 30
