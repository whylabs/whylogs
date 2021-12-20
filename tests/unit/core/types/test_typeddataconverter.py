import numpy as np
import pandas as pd
import pytest

from whylogs.core.types import TypedDataConverter

_TEST_NULL_DATA = [
    ([None, np.nan, None] * 3, 9),
    ([pd.Series(data={"a": None, "b": None}, index=["x", "y"]), pd.Series(data={"c": None, "d": 1}, index=["x", "y"])], 2),
    ([[None, np.nan], [np.nan], [None]], 3),
    ([[None, 1], [None]], 1),
    ([np.zeros(3)], 0),
]


def test_invalid_yaml_returns_string():
    x = " \tSr highway safety Specialist"
    assert x == TypedDataConverter.convert(x)

    # Just verify that `x` is invalid yaml
    import yaml

    try:
        yaml.safe_load(x)
        raise RuntimeError("Should raise exception")
    except yaml.scanner.ScannerError:
        pass


@pytest.mark.parametrize("data,nulls_expected", _TEST_NULL_DATA)
def test_are_nulls(data, nulls_expected):
    null_count = 0
    for val in data:
        if TypedDataConverter._are_nulls(val):
            null_count += 1

    assert null_count == nulls_expected
