import os.path
from typing import Any

import numpy as np
import pandas as pd

import whylogs as why


def test_basic_log() -> None:
    d = {"col1": [1, 2], "col2": [3.0, 4.0], "col3": ["a", "b"]}
    df = pd.DataFrame(data=d)

    results = why.log(df)

    profile = results.profile()

    assert profile._columns["col1"]._schema.dtype == np.int64
    assert profile._columns["col2"]._schema.dtype == np.float64
    assert profile._columns["col3"]._schema.dtype.name == "object"


def test_basic_log_row() -> None:
    d = {"col1": [1, 2], "col2": [3.0, 4.0], "col3": ["a", "b"]}

    results = why.log(row=d)

    profile = results.profile()

    assert profile._columns["col1"]._schema.dtype == list
    assert profile._columns["col2"]._schema.dtype == list
    assert profile._columns["col3"]._schema.dtype == list


def test_basic_log_dict_of_lists() -> None:
    d = {"col1": [1, 2], "col2": [3.0, 4.0], "col3": ["a", "b"]}

    results = why.log(d)

    profile = results.profile()

    assert profile._columns["col1"]._schema.dtype == list
    assert profile._columns["col2"]._schema.dtype == list
    assert profile._columns["col3"]._schema.dtype == list


def test_basic_log_dictionary() -> None:
    d = {"a": 1.0, "b": 2.0}

    results = why.log(d)

    profile = results.profile()

    assert profile._columns["a"]._schema.dtype == float
    assert profile._columns["b"]._schema.dtype == float


def test_lending_club(lending_club_df: pd.DataFrame) -> None:
    res = why.log(lending_club_df)
    view = res.view()
    df = view.to_pandas()
    assert len(df) == 151


def test_roundtrip_resultset(tmp_path: Any) -> None:
    d = {"col1": [1, 2], "col2": [3.0, 4.0], "col3": ["a", "b"]}
    df = pd.DataFrame(data=d)

    results = why.log(df)
    results.writer("local").option(base_dir=tmp_path).write(dest="profile.bin")
    path = os.path.join(tmp_path, "profile.bin")
    roundtrip_result_set = why.read(path)
    assert len(results.view().to_pandas()) == len(roundtrip_result_set.view().to_pandas())
