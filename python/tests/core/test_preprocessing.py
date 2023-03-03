import math
import unittest
from decimal import Decimal
from logging import getLogger
from typing import Any, List, Optional, Union

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_series_equal

from whylogs.core.preprocessing import (
    ListView,
    NumpyView,
    PandasView,
    PreprocessedColumn,
)
from whylogs.core.stubs import NumpyStub, PandasStub

TEST_LOGGER = getLogger(__name__)

FLOAT_TYPES = [float, np.float16, np.float32, np.float64, np.floating, np.float_, np.longdouble]


def assert_zero_len(values: Optional[Union[List[Any], np.ndarray, pd.Series]]) -> None:
    if values is None:
        return
    assert len(values) == 0


def assert_list_view_is_all_nones(view: ListView) -> None:
    assert view.ints is None
    assert view.floats is None
    assert view.strings is None
    assert view.tensors is None
    assert view.objs is None


def assert_pandas_view_is_all_nones(view: PandasView) -> None:
    assert view.strings is None
    assert view.tensors is None
    assert view.objs is None


def assert_numpy_view_is_all_nones(view: NumpyView) -> None:
    assert view.ints is None
    assert view.floats is None
    assert view.strings is None


class TestListElements(object):
    def test_floats_and_ints_and_str(self) -> None:
        mixed = pd.Series([1.0, 1, 2.0, 2, "str", None])

        res = PreprocessedColumn.apply(mixed)

        assert res.numpy.floats.tolist() == [1.0, 2.0]  # type: ignore
        assert res.numpy.ints.tolist() == [1, 2]  # type: ignore
        assert res.numpy.strings is None

        assert res.pandas.strings.tolist() == ["str"]  # type: ignore
        assert_zero_len(res.pandas.tensors)
        assert_zero_len(res.pandas.objs)  # type: ignore

        assert res.null_count == 1

        assert_list_view_is_all_nones(res.list)

    def test_none_and_math_nan(self) -> None:
        mixed = pd.Series([None, math.nan, "hi"])

        res = PreprocessedColumn.apply(mixed)
        assert len(res.pandas.strings) == 1
        assert_zero_len(res.pandas.tensors)
        assert_zero_len(res.pandas.objs)  # type: ignore
        assert res.null_count == 2
        assert res.nan_count == 1

        assert res.numpy.ints.shape == (0,)
        assert res.numpy.floats.shape == (0,)
        assert res.numpy.strings is None

        assert_list_view_is_all_nones(res.list)

    def test_none_and_math_inf(self) -> None:
        mixed = pd.Series([math.inf, None])  # when all types are numeric None->NaN

        res = PreprocessedColumn.apply(mixed)
        assert_zero_len(res.pandas.strings)
        assert_zero_len(res.pandas.tensors)
        assert_zero_len(res.pandas.objs)  # type: ignore
        assert res.null_count == 1
        assert res.nan_count == 1
        assert res.inf_count == 1

        assert res.numpy.ints is None
        assert res.numpy.floats.tolist() == [math.inf]  # TODO: hmmm...
        assert res.numpy.strings is None

        assert_list_view_is_all_nones(res.list)

    def test_none_and_np_nan(self) -> None:
        mixed = pd.Series([np.nan, None, "test"])

        res = PreprocessedColumn.apply(mixed)
        assert len(res.pandas.strings) == 1
        assert_zero_len(res.pandas.tensors)
        assert_zero_len(res.pandas.objs)  # type: ignore
        assert res.null_count == 2
        assert res.nan_count == 1

        assert res.numpy.ints.shape == (0,)
        assert res.numpy.floats.shape == (0,)
        assert res.numpy.strings is None

        assert_list_view_is_all_nones(res.list)

    def test_none_and_np_inf_mixed(self) -> None:
        mixed = pd.Series([np.inf, None, "t"])

        res = PreprocessedColumn.apply(mixed)
        assert len(res.pandas.strings) == 1
        assert_zero_len(res.pandas.tensors)
        assert_zero_len(res.pandas.objs)  # type: ignore
        assert res.null_count == 1
        assert res.nan_count == 0
        assert res.inf_count == 1

        assert res.numpy.ints.shape == (0,)
        assert res.numpy.floats.tolist() == [np.inf]  # TODO: hmmm...
        assert res.numpy.strings is None

        assert_list_view_is_all_nones(res.list)

    def test_none_and_np_inf_and_nan(self) -> None:
        mixed = pd.Series([np.inf, None, float("nan"), "t"])

        res = PreprocessedColumn.apply(mixed)
        assert len(res.pandas.strings) == 1
        assert_zero_len(res.pandas.tensors)
        assert_zero_len(res.pandas.objs)  # type: ignore
        assert res.null_count == 2
        assert res.nan_count == 1
        assert res.inf_count == 1

        assert res.numpy.ints.shape == (0,)
        assert res.numpy.floats.tolist() == [np.inf]  # TODO: hmmm...
        assert res.numpy.strings is None

        assert_list_view_is_all_nones(res.list)

    def test_bools_and_ints(self) -> None:
        mixed = pd.Series([True, True, False, 2, 1, 0])

        res = PreprocessedColumn.apply(mixed)
        TEST_LOGGER.info(f"{res}")
        assert res.numpy.floats.tolist() == []  # type: ignore
        assert res.numpy.ints.tolist() == [2, 1, 0]  # type: ignore
        assert res.numpy.strings is None

        assert res.bool_count == 3
        assert res.bool_count_where_true == 2
        assert res.null_count == 0

        assert_zero_len(res.pandas.strings)  # type: ignore
        assert_zero_len(res.pandas.objs)  # type: ignore
        assert_zero_len(res.pandas.tensors)

        assert_list_view_is_all_nones(res.list)

    @pytest.mark.parametrize("data_type", FLOAT_TYPES)
    def test_floats_no_null(self, data_type) -> None:
        floats = pd.Series([1.0, 2.0, 3.0], dtype=data_type).astype(float)
        res = PreprocessedColumn.apply(floats)

        assert_series_equal(res.numpy.floats, floats)
        assert res.numpy.ints is None
        assert res.numpy.strings is None

        assert_zero_len(res.pandas.tensors)
        assert_zero_len(res.pandas.strings)
        assert_zero_len(res.numpy.ints)
        assert res.null_count == 0

        assert_list_view_is_all_nones(res.list)

    @pytest.mark.parametrize("data_type", FLOAT_TYPES)
    def test_floats_with_null(self, data_type) -> None:
        f_with_none = pd.Series([1.0, 2.0, 3.0, None], dtype=data_type)
        res = PreprocessedColumn.apply(f_with_none)
        assert res.null_count == 1

        assert_series_equal(res.numpy.floats, pd.Series([1.0, 2.0, 3.0]))
        assert res.numpy.ints is None
        assert res.numpy.strings is None

        assert_zero_len(res.pandas.tensors)
        assert_zero_len(res.pandas.objs)
        assert_zero_len(res.pandas.strings)

        assert_list_view_is_all_nones(res.list)

    def test_strings(self) -> None:
        strings = pd.Series(["foo", "bar"])

        res = PreprocessedColumn.apply(strings)
        assert res.null_count == 0

        assert_series_equal(res.pandas.strings, strings)
        assert_zero_len(res.pandas.tensors)
        assert_zero_len(res.pandas.objs)

        assert_zero_len(res.numpy.floats)
        assert_zero_len(res.numpy.ints)
        assert res.numpy.strings is None

        assert_list_view_is_all_nones(res.list)

    def test_strings_with_none(self) -> None:
        strings = pd.Series(["foo", "bar", None, None])

        res = PreprocessedColumn.apply(strings)
        assert res.null_count == 2

        assert_series_equal(res.pandas.strings, pd.Series(["foo", "bar"]))
        assert_zero_len(res.pandas.objs)
        assert_zero_len(res.pandas.tensors)

        assert_zero_len(res.numpy.floats)
        assert_zero_len(res.numpy.ints)
        assert res.numpy.strings is None

        assert_list_view_is_all_nones(res.list)


@pytest.mark.parametrize(
    "value,np_stubbed,pd_stubbed",
    [
        (12, False, False),
        (12, False, True),
        (12, True, False),
        (12, True, True),
        (True, False, False),
        (True, False, True),
        (True, True, False),
        (True, True, True),
        (3.14, False, False),
        (3.14, False, True),
        (3.14, True, False),
        (3.14, True, True),
        (Decimal(42), False, False),
        (Decimal(42), False, True),
        (Decimal(42), True, False),
        (Decimal(42), True, True),
        ("foo", False, False),
        ("foo", False, True),
        ("foo", True, False),
        ("foo", True, True),
    ],
)
def test_process_scalar_called_with_scalar_nonobject(
    value: Any, np_stubbed: bool, pd_stubbed: bool, monkeypatch
) -> None:
    monkeypatch.setattr("whylogs.core.preprocessing.np", NumpyStub() if np_stubbed else np)
    # the _process_scalar_value() code path doesn't use Pandas, just covering all cases out of paranoia
    monkeypatch.setattr("whylogs.core.preprocessing.pd", PandasStub() if pd_stubbed else pd)

    column = PreprocessedColumn._process_scalar_value(value)

    if isinstance(value, (int)):
        if isinstance(value, (bool)):
            assert column.bool_count == 1
            assert column.bool_count_where_true == (1 if value else 0)

            # bools are only counted, they're not added to any views.
            # When numpy is stubbed, the empty list of ints (aka bools) goes to list view.
            # Otherwise it goes to numpy view. Depending on np stubbing, the lack of
            # integers may end up as None or empty list/ndarray.
            if np_stubbed:
                assert column.list.ints == []
                assert column.numpy.ints is None
            else:
                assert column.list.ints is None
                assert column.numpy.ints.shape == (0,)
                assert column.numpy.ints.dtype == np.dtype("int64")
        else:
            assert column.bool_count == 0
            assert column.bool_count_where_true == 0

            # A single integer value goes to numpy view or list view depending on
            # numpy stubbing. In this case, the unused int view is None.
            if np_stubbed:
                assert column.list.ints == [value]
                assert column.numpy.ints is None
            else:
                assert column.list.ints is None
                assert column.numpy.ints == np.asarray([value], dtype=int)

        # TODO: should we make None/[] more consistent?
        assert column.list.strings == []
        assert column.list.tensors == []
        assert column.list.objs == []

        # The empty float list goes to numpy view or list view depending on
        # numpy stubbing. Depending on np stubbing, the lack of floats may
        # end up as None or empty list/ndarray.
        if np_stubbed:
            assert column.numpy.floats is None
            assert column.list.floats == []
        else:
            assert column.numpy.floats.shape == (0,)
            assert column.numpy.floats.dtype == np.dtype("float64")
            assert column.list.floats is None

        assert column.numpy.strings is None  # TODO: should strings be in NumpyView instead?

    elif isinstance(value, (float, Decimal)):
        assert column.bool_count == 0
        assert column.bool_count_where_true == 0

        # A single float goes to numpy view or list view depending on numpy stubbing.
        # The empty/unused float/int components of list view and numpy view are None
        # or empty list/ndarray depending on numpy stubbing.
        if np_stubbed:
            assert column.numpy.ints is None
            assert column.numpy.floats is None

            assert column.list.ints == []
            assert column.list.floats == [value]
        else:
            assert column.numpy.ints.shape == (0,)
            assert column.numpy.ints.dtype == np.dtype("int64")
            assert column.numpy.floats == np.asarray([value], dtype=float)

            assert column.list.ints is None
            assert column.list.floats is None

        assert column.list.strings == []
        assert column.list.tensors == []
        assert column.list.objs == []

        assert column.numpy.strings is None

    elif isinstance(value, (str)):
        assert column.bool_count == 0
        assert column.bool_count_where_true == 0

        # A single string goes to list view.
        # The empty/unused float/int components of list view and numpy view are None
        # or empty list/ndarray depending on numpy stubbing.
        if np_stubbed:
            assert column.numpy.ints is None
            assert column.numpy.floats is None

            assert column.list.ints == []
            assert column.list.floats == []
        else:
            assert column.numpy.ints.shape == (0,)
            assert column.numpy.ints.dtype == np.dtype("int64")
            assert column.numpy.floats.shape == (0,)
            assert column.numpy.floats.dtype == np.dtype("float64")

            assert column.list.ints is None
            assert column.list.floats is None

        assert column.list.strings == [value]
        assert column.list.tensors == []
        assert column.list.objs == []

        assert column.numpy.strings is None  # TODO: should strings be in NumpyView instead?

    # scalar code path never produces Pandas
    assert_pandas_view_is_all_nones(column.pandas)


@pytest.mark.parametrize(
    "value,np_stubbed,pd_stubbed",
    [
        ([1, 2, 3], False, False),
        ([[1, 0, 0], [0, 1, 0], [0, 0, 1]], False, False),
        (np.asarray([1, 2, 3]), False, False),
        (np.asarray([[1, 0, 0], [0, 1, 0], [0, 0, 1]]), False, False),
        ([1, 2, 3], True, False),
        ([[1, 0, 0], [0, 1, 0], [0, 0, 1]], True, False),
        (np.asarray([1, 2, 3]), True, False),
        (np.asarray([[1, 0, 0], [0, 1, 0], [0, 0, 1]]), True, False),
        ([1, 2, 3], False, True),
        ([[1, 0, 0], [0, 1, 0], [0, 0, 1]], False, True),
        (np.asarray([1, 2, 3]), False, True),
        (np.asarray([[1, 0, 0], [0, 1, 0], [0, 0, 1]]), False, True),
        ([1, 2, 3], True, True),
        ([[1, 0, 0], [0, 1, 0], [0, 0, 1]], True, True),
        (np.asarray([1, 2, 3]), True, True),
        (np.asarray([[1, 0, 0], [0, 1, 0], [0, 0, 1]]), True, True),
    ],
)
def test_process_scalar_called_with_ternsorable(value: Any, np_stubbed: bool, pd_stubbed: bool, monkeypatch) -> None:
    monkeypatch.setattr("whylogs.core.preprocessing.np", NumpyStub() if np_stubbed else np)
    # the _process_scalar_value() code path doesn't use Pandas, just covering all cases out of paranoia
    monkeypatch.setattr("whylogs.core.preprocessing.pd", PandasStub() if pd_stubbed else pd)

    column = PreprocessedColumn._process_scalar_value(value)

    assert column.bool_count == 0
    assert column.bool_count_where_true == 0

    # A single tensorizable ndarray or list goes to list view. Tensorizability requires np is not stubbed.
    # Untensorizable lists go to list view's object list.
    if np_stubbed:
        assert column.list.tensors == []
        assert column.list.objs == [value]
    else:
        assert len(column.list.tensors) == 1
        assert column.list.tensors[0].tolist() == np.asarray(value).tolist()
        assert column.list.objs == []

    # The empty/unused float/int components of list view and numpy view are None
    # or empty list/ndarray depending on numpy stubbing.
    if np_stubbed:
        assert column.numpy.ints is None
        assert column.numpy.floats is None

        assert column.list.ints == []
        assert column.list.floats == []
    else:
        assert column.numpy.ints.shape == (0,)
        assert column.numpy.ints.dtype == np.dtype("int64")
        assert column.numpy.floats.shape == (0,)
        assert column.numpy.floats.dtype == np.dtype("float64")

        assert column.list.ints is None
        assert column.list.floats is None

    assert column.list.strings == []
    assert column.numpy.strings is None

    # scalar code path never produces Pandas
    assert_pandas_view_is_all_nones(column.pandas)


class _UnknownType:
    pass


# If we treat list[str] or np.ndarray(_, dtype=str) as bag of words, they
# end up in list.objs


@pytest.mark.parametrize(
    "value,np_stubbed,pd_stubbed",
    [
        (_UnknownType(), False, False),
        (["a", "b", "c"], False, False),  # non-Tensorable list
        (_UnknownType(), False, True),
        (["a", "b", "c"], False, True),  # non-Tensorable list
        (_UnknownType(), True, False),
        (["a", "b", "c"], True, False),  # non-Tensorable list
        (_UnknownType(), True, True),
        (["a", "b", "c"], True, True),  # non-Tensorable list
        # non-numeric ndarrays are objects
        (np.asarray(["a", "b", "c"]), False, False),
        (np.asarray([["a", "b"], ["c", "d"]]), False, False),
        (np.asarray(["a", "b", "c"]), False, True),
        (np.asarray([["a", "b"], ["c", "d"]]), False, True),
        (np.asarray(["a", "b", "c"]), True, False),
        (np.asarray([["a", "b"], ["c", "d"]]), True, False),
        (np.asarray(["a", "b", "c"]), True, True),
        (np.asarray([["a", "b"], ["c", "d"]]), True, True),
        # Tensorability requires non-stubbed numpy, so these are all objects
        ([1, 2, 3], True, False),
        ([[1, 0, 0], [0, 1, 0], [0, 0, 1]], True, False),
        (np.asarray([1, 2, 3]), True, False),
        (np.asarray([[1, 0, 0], [0, 1, 0], [0, 0, 1]]), True, False),
        ([1, 2, 3], True, True),
        ([[1, 0, 0], [0, 1, 0], [0, 0, 1]], True, True),
        (np.asarray([1, 2, 3]), True, True),
        (np.asarray([[1, 0, 0], [0, 1, 0], [0, 0, 1]]), True, True),
    ],
)
def test_process_scalar_called_with_scalar_object(value: Any, np_stubbed: bool, pd_stubbed: bool, monkeypatch) -> None:
    monkeypatch.setattr("whylogs.core.preprocessing.np", NumpyStub() if np_stubbed else np)
    # the _process_scalar_value() code path doesn't use Pandas, just covering all cases out of paranoia
    monkeypatch.setattr("whylogs.core.preprocessing.pd", PandasStub() if pd_stubbed else pd)

    column = PreprocessedColumn._process_scalar_value(value)

    assert column.bool_count == 0
    assert column.bool_count_where_true == 0

    # A single value that's not of type int/bool, float, decimal, string, tensorable list/ndarray
    # goes to list view's object list.
    assert column.list.objs == [value]

    # The empty/unused float/int components of list view and numpy view are None
    # or empty list/ndarray depending on numpy stubbing.
    if np_stubbed:
        assert column.numpy.ints is None
        assert column.numpy.floats is None

        assert column.list.ints == []
        assert column.list.floats == []
    else:
        assert column.numpy.ints.shape == (0,)
        assert column.numpy.ints.dtype == np.dtype("int64")
        assert column.numpy.floats.shape == (0,)
        assert column.numpy.floats.dtype == np.dtype("float64")

        assert column.list.ints is None
        assert column.list.floats is None

    assert column.list.strings == []
    assert column.list.tensors == []

    assert column.numpy.strings is None

    # scalar code path never produces Pandas
    assert_pandas_view_is_all_nones(column.pandas)


@pytest.mark.parametrize(
    "column",
    [
        ([[1, 2, 3], [4.0, 5.0, 6.0, 7.0]]),
        ([[[1, 0], [0, 1]], [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]]]),
        ([np.array([1, 2, 3]), np.array([4.0, 5.0, 6.0, 7.0])]),
        ([np.array([[1, 0], [0, 1]]), np.array([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]])]),
    ],
)
def test_apply_tensorable_series(column: List[Any]) -> None:
    res = PreprocessedColumn.apply(pd.Series(column))
    assert len(res.pandas.tensors) == len(column)
    for i in range(len(column)):
        X = res.pandas.tensors[i]
        Y = column[i] if isinstance(column[i], np.ndarray) else np.asarray(column[i])
        assert X.shape == Y.shape

    assert res.numpy.floats.shape == (0,)
    assert res.numpy.ints.shape == (0,)
    assert res.numpy.strings is None

    assert len(res.pandas.strings) == 0
    assert len(res.pandas.objs) == 0

    # list view is not used in the apply() code path
    assert_list_view_is_all_nones(res.list)


@pytest.mark.parametrize(
    "column",
    [
        ([["a", "b", "c"], ["d", "e"]]),
        ([np.asarray(["a", "b", "c"]), np.asarray(["d", "e"])]),
        ([_UnknownType(), _UnknownType()]),
        ([[_UnknownType(), _UnknownType()], [_UnknownType(), _UnknownType()]]),
    ],
)
def test_apply_nontensorable_series(column: Any) -> None:
    res = PreprocessedColumn.apply(pd.Series(column))

    assert len(res.pandas.objs) == len(column)
    assert_zero_len(res.pandas.strings)
    assert_zero_len(res.pandas.tensors)

    assert res.numpy.floats.shape == (0,)
    assert res.numpy.ints.shape == (0,)
    assert res.numpy.strings is None

    # list view is not used in the apply() code path
    assert_list_view_is_all_nones(res.list)


@pytest.mark.parametrize(
    "column",
    [
        (np.asarray([1, 0, 0])),
        (np.asarray([[1, 0, 0], [0, 1, 0], [0, 0, 1]])),
        (np.asarray([1.0, 0.0, 0.0])),
        (np.asarray([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]])),
        (np.asarray(["a", "b", "c"])),
    ],
)
def test_apply_ndarray(column: np.ndarray) -> None:
    res = PreprocessedColumn.apply(column)

    if issubclass(column.dtype.type, np.floating):
        assert res.numpy.floats.tolist() == column.tolist()
        assert res.numpy.ints is None
        assert res.numpy.strings is None
    elif issubclass(column.dtype.type, np.integer):
        assert res.numpy.floats is None
        assert res.numpy.ints.tolist() == column.tolist()
        assert res.numpy.strings is None
    else:
        assert res.numpy.floats is None
        assert res.numpy.ints is None
        assert res.numpy.strings.tolist() == column.tolist()

    assert_list_view_is_all_nones(res.list)
    assert_pandas_view_is_all_nones(res.pandas)


""" pandas implies numpy
([1, 2, 3], True, False, [1, 2, 3], [], [], [], []),
([4.0, 5.0, "a", "b"], True, False, [], [4.0, 5.0], ["a", "b"], [], []),
([[1, 2, 3]], True, False, [], [], [], [], [[1, 2, 3]]),  # no numpy means no tensors
([[[1, 0], [0, 1]]], True, False, [], [], [], [], [[[1, 0], [0, 1]]]),
([_UnknownType, ["a", "b"]], True, False, [], [], [], [], [_UnknownType, ["a", "b"]]),
"""


@pytest.mark.parametrize(
    "column,np_stubbed,pd_stubbed,ints,floats,strings,tensors,objs",
    [
        ([1, 2, 3], False, True, [1, 2, 3], [], [], [], []),
        ([4.0, 5.0, "a", "b"], False, True, [], [4.0, 5.0], ["a", "b"], [], []),
        ([[1, 2, 3]], False, True, [], [], [], [np.asarray([1, 2, 3])], []),
        ([[[1, 0], [0, 1]]], False, True, [], [], [], [np.asarray([[1, 0], [0, 1]])], []),
        ([_UnknownType, ["a", "b"]], False, True, [], [], [], [], [_UnknownType, ["a", "b"]]),
        ([1, 2, 3], True, True, [1, 2, 3], [], [], [], []),
        ([4.0, 5.0, "a", "b"], True, True, [], [4.0, 5.0], ["a", "b"], [], []),
        ([[1, 2, 3]], True, True, [], [], [], [], [[1, 2, 3]]),  # no numpy means no tensors
        ([[[1, 0], [0, 1]]], True, True, [], [], [], [], [[[1, 0], [0, 1]]]),
        ([_UnknownType, ["a", "b"]], True, True, [], [], [], [], [_UnknownType, ["a", "b"]]),
        ([1, 2, 3], False, False, [1, 2, 3], [], [], [], []),
        ([4.0, 5.0, "a", "b"], False, False, [], [4.0, 5.0], ["a", "b"], [], []),
        ([[1, 2, 3]], False, False, [], [], [], [np.asarray([1, 2, 3])], []),
        ([[[1, 0], [0, 1]]], False, False, [], [], [], [np.asarray([[1, 0], [0, 1]])], []),
        ([_UnknownType, ["a", "b"]], False, False, [], [], [], [], [_UnknownType, ["a", "b"]]),
        # "scalars" get wrapped in a list and apply called recursively
        (1, False, False, [1], [], [], [], []),
        (1, False, True, [1], [], [], [], []),
        (1, True, True, [1], [], [], [], []),
        (1.0, False, False, [], [1.0], [], [], []),
        (1.0, False, True, [], [1.0], [], [], []),
        (1.0, True, True, [], [1.0], [], [], []),
        ("a", False, False, [], [], ["a"], [], []),
        ("a", False, True, [], [], ["a"], [], []),
        ("a", True, True, [], [], ["a"], [], []),
    ],
)
def test_apply_list(
    column: Union[List[Any], int, float, str],
    np_stubbed: bool,
    pd_stubbed: bool,
    ints: List[int],
    floats: List[float],
    strings: List[str],
    tensors: List[np.ndarray],
    objs: List[Any],
    monkeypatch,
) -> None:
    monkeypatch.setattr("whylogs.core.preprocessing.np", NumpyStub() if np_stubbed else np)
    monkeypatch.setattr("whylogs.core.preprocessing.pd", PandasStub() if pd_stubbed else pd)
    res = PreprocessedColumn.apply(column)
    if pd_stubbed:
        assert_pandas_view_is_all_nones(res.pandas)

        if np_stubbed:
            assert_numpy_view_is_all_nones(res.numpy)
            assert res.list.ints == ints
            assert res.list.floats == floats
        else:
            assert res.numpy.ints.tolist() == ints
            assert res.numpy.floats.tolist() == floats
            assert res.numpy.strings is None

            assert res.list.ints is None
            assert res.list.floats is None

        assert res.list.strings == strings
        assert len(res.list.tensors) == len(tensors)
        for i in range(len(tensors)):
            assert res.list.tensors[i].tolist() == tensors[i].tolist()

        assert res.list.objs == objs

    else:  # pandas is not stubbed; list is wrapped in pd.Series(column, dtype="object")
        assert_list_view_is_all_nones(res.list)

        assert res.numpy.ints.tolist() == ints
        assert res.numpy.floats.tolist() == floats
        assert res.numpy.strings is None

        assert res.pandas.strings.tolist() == strings
        assert res.pandas.objs.tolist() == objs
        assert len(res.pandas.tensors) == len(tensors)
        for i in range(len(tensors)):
            X = res.pandas.tensors[i]
            Y = tensors[i]
            assert X.shape == Y.shape


def test_apply_iterable() -> None:
    pass


def test_apply_scalar() -> None:
    pass


if __name__ == "__main__":
    unittest.main()
