import os.path
from typing import Any

import numpy as np
import pandas as pd

import whylogs as ylog


def test_basic_log() -> None:
    d = {"col1": [1, 2], "col2": [3.0, 4.0], "col3": ["a", "b"]}
    df = pd.DataFrame(data=d)

    results = ylog.log(df)

    profile = results.profile()

    assert profile._columns["col1"]._schema.dtype == np.int64
    assert profile._columns["col2"]._schema.dtype == np.float64
    assert profile._columns["col3"]._schema.dtype.name == "object"


def test_roundtrip_resultset(tmp_path: Any) -> None:
    d = {"col1": [1, 2], "col2": [3.0, 4.0], "col3": ["a", "b"]}
    df = pd.DataFrame(data=d)

    results = ylog.log(df)
    results.writer("local").option(base_dir=tmp_path).write(dest="profile.bin")
    path = os.path.join(tmp_path, "profile.bin")
    roundtrip_result_set = ylog.read(path)
    assert len(results.view().to_pandas()) == len(roundtrip_result_set.view().to_pandas())
