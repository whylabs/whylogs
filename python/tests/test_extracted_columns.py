import unittest
from typing import Any, List, Optional, Union

import numpy as np
import pandas as pd
from pandas.testing import assert_series_equal

from whylogs_v1.core.extraction import ExtractedColumn


def assert_zero_len(values: Optional[Union[List[Any], np.ndarray, pd.Series]]) -> None:
    if values is None:
        return
    assert len(values) == 0


class TestListElements(unittest.TestCase):
    def test_floats_and_ints_and_str(self) -> None:
        mixed = pd.Series([1.0, 1, 2.0, 2, "str", None])

        res = ExtractedColumn.apply(mixed)

        self.assertListEqual(res.numpy.floats.tolist(), [1.0, 2.0])  # type: ignore
        self.assertListEqual(
            res.numpy.ints.tolist(),  # type: ignore
            [
                1,
                2,
            ],
        )  # type: ignore
        self.assertListEqual(res.pandas.strings.tolist(), ["str"])  # type: ignore
        self.assertListEqual(res.pandas.strings.tolist(), ["str"])  # type: ignore
        assert_zero_len(res.pandas.objs)  # type: ignore
        assert res.null_count == 1

    def test_floats_no_null(self) -> None:
        floats = pd.Series([1.0, 2.0, 3.0])

        res = ExtractedColumn.apply(floats)

        assert_series_equal(res.numpy.floats, floats)
        assert_zero_len(res.pandas.strings)
        assert_zero_len(res.numpy.ints)
        assert res.null_count == 0

    def test_floats_with_null(self) -> None:
        f_with_none = pd.Series([1.0, 2.0, 3.0, None])
        res = ExtractedColumn.apply(f_with_none)
        assert_series_equal(res.numpy.floats, pd.Series([1.0, 2.0, 3.0]))
        assert_zero_len(res.pandas.strings)
        assert_zero_len(res.numpy.ints)
        assert res.null_count == 1

    def test_strings(self) -> None:
        strings = pd.Series(["foo", "bar"])

        res = ExtractedColumn.apply(strings)

        assert_series_equal(res.pandas.strings, strings)
        assert_zero_len(res.numpy.floats)
        assert_zero_len(res.numpy.ints)
        assert_zero_len(res.pandas.objs)

        assert res.null_count == 0

    def test_strings_with_none(self) -> None:
        strings = pd.Series(["foo", "bar", None, None])

        res = ExtractedColumn.apply(strings)

        assert_series_equal(res.pandas.strings, pd.Series(["foo", "bar"]))
        assert_zero_len(res.numpy.floats)
        assert_zero_len(res.numpy.ints)
        assert_zero_len(res.pandas.objs)
        assert res.null_count == 2


if __name__ == "__main__":
    unittest.main()
