import unittest
from logging import getLogger
from typing import Any, List, Optional, Union

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_series_equal

from whylogs.core.preprocessing import PreprocessedColumn

TEST_LOGGER = getLogger(__name__)

FLOAT_TYPES = [float, np.float16, np.float32, np.float64, np.floating, np.float_, np.longdouble]


def assert_zero_len(values: Optional[Union[List[Any], np.ndarray, pd.Series]]) -> None:
    if values is None:
        return
    assert len(values) == 0


class TestListElements(object):
    def test_floats_and_ints_and_str(self) -> None:
        mixed = pd.Series([1.0, 1, 2.0, 2, "str", None])

        res = PreprocessedColumn.apply(mixed)

        assert res.numpy.floats.tolist() == [1.0, 2.0]  # type: ignore
        assert res.numpy.ints.tolist() == [1, 2]  # type: ignore

        assert res.pandas.strings.tolist() == ["str"]  # type: ignore
        assert_zero_len(res.pandas.objs)  # type: ignore
        assert res.null_count == 1

    def test_bools_and_ints(self) -> None:
        mixed = pd.Series([True, True, False, 2, 1, 0])

        res = PreprocessedColumn.apply(mixed)
        TEST_LOGGER.info(f"{res}")
        assert res.numpy.floats.tolist() == []  # type: ignore
        assert res.numpy.ints.tolist() == [2, 1, 0]  # type: ignore
        assert res.bool_count == 3
        assert res.bool_count_where_true == 2
        assert_zero_len(res.pandas.strings)  # type: ignore
        assert_zero_len(res.pandas.objs)  # type: ignore
        assert res.null_count == 0

    @pytest.mark.parametrize("data_type", FLOAT_TYPES)
    def test_floats_no_null(self, data_type) -> None:
        floats = pd.Series([1.0, 2.0, 3.0], dtype=data_type).astype(float)
        res = PreprocessedColumn.apply(floats)

        assert_series_equal(res.numpy.floats, floats)
        assert_zero_len(res.pandas.strings)
        assert_zero_len(res.numpy.ints)
        assert res.null_count == 0

    @pytest.mark.parametrize("data_type", FLOAT_TYPES)
    def test_floats_with_null(self, data_type) -> None:
        f_with_none = pd.Series([1.0, 2.0, 3.0, None], dtype=data_type)
        res = PreprocessedColumn.apply(f_with_none)
        assert_series_equal(res.numpy.floats, pd.Series([1.0, 2.0, 3.0]))
        assert_zero_len(res.pandas.strings)
        assert_zero_len(res.numpy.ints)
        assert res.null_count == 1

    def test_strings(self) -> None:
        strings = pd.Series(["foo", "bar"])

        res = PreprocessedColumn.apply(strings)

        assert_series_equal(res.pandas.strings, strings)
        assert_zero_len(res.numpy.floats)
        assert_zero_len(res.numpy.ints)
        assert_zero_len(res.pandas.objs)

        assert res.null_count == 0

    def test_strings_with_none(self) -> None:
        strings = pd.Series(["foo", "bar", None, None])

        res = PreprocessedColumn.apply(strings)

        assert_series_equal(res.pandas.strings, pd.Series(["foo", "bar"]))
        assert_zero_len(res.numpy.floats)
        assert_zero_len(res.numpy.ints)
        assert_zero_len(res.pandas.objs)
        assert res.null_count == 2


if __name__ == "__main__":
    unittest.main()
