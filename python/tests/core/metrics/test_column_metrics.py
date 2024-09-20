import numpy as np
import pandas as pd
import pytest

from whylogs.core import ColumnSchema
from whylogs.core.metrics import ColumnCountsMetric, TypeCountersMetric
from whylogs.core.metrics.metric_components import IntegralComponent
from whylogs.core.preprocessing import PreprocessedColumn
from whylogs.core.proto import MetricMessage

INTEGER_TYPES = [int, np.intc, np.uintc, np.int_, np.uint, np.longlong, np.ulonglong]
FLOAT_TYPES = [float, np.double, np.longdouble, np.float16, np.float64]
STRING_TYPE = [str, pd.CategoricalDtype()]


@pytest.fixture
def counts():
    return ColumnCountsMetric.zero(ColumnSchema(dtype=int))


def test_null_count(counts) -> None:
    p_col = PreprocessedColumn.apply([None, None, None, None, 1, 2])
    counts.columnar_update(p_col)

    assert counts.null.value == 4
    assert counts.n.value == 6


def test_type_counters_success_count() -> None:
    counts = TypeCountersMetric.zero(ColumnSchema(dtype=int))
    test_bool_input = [True, False]
    int_range = 3
    test_int_input = [i for i in range(int_range)]

    test_mixed_input = test_int_input + test_bool_input
    p_col = PreprocessedColumn.apply(test_bool_input)
    operation_result = None
    iterations = 2
    for _ in range(iterations):
        operation_result = counts.columnar_update(p_col)

    bools_per_input = len(test_bool_input)
    expected_bool_count = iterations * bools_per_input
    assert operation_result is not None
    assert counts.boolean.value == expected_bool_count
    assert operation_result.ok
    assert operation_result.successes == bools_per_input

    p_col = PreprocessedColumn.apply(test_mixed_input)
    counts.columnar_update(p_col)
    operation_result = counts.columnar_update(p_col)
    assert counts.boolean.value == expected_bool_count + (2 * bools_per_input)
    assert counts.integral.value == int_range * 2
    assert operation_result.successes == len(test_mixed_input)


@pytest.mark.parametrize("data_type", [*INTEGER_TYPES, *FLOAT_TYPES, *STRING_TYPE])
def test_different_types_col_update_on_series(counts, data_type) -> None:
    series_input = pd.Series([1, 2, 33331, 5], dtype=data_type)
    p_col = PreprocessedColumn.apply(series_input)
    operation_result = None
    iterations = 2
    for _ in range(iterations):
        operation_result = counts.columnar_update(p_col)

    integers_per_input = len(series_input)
    expected_int_count = iterations * integers_per_input
    assert operation_result is not None
    assert counts.n.value == expected_int_count
    assert operation_result.ok
    assert operation_result.successes == integers_per_input


def test_type_counts_with_and_without_tensor_field():
    type_counts = TypeCountersMetric(
        boolean=IntegralComponent(1),
        integral=IntegralComponent(2),
        fractional=IntegralComponent(3),
        string=IntegralComponent(4),
        object=IntegralComponent(5),
    )
    assert type_counts.tensor.value == 0
    type_counts.tensor.set(1)
    assert type_counts.boolean.value == 1
    full_type_counts = TypeCountersMetric(
        boolean=IntegralComponent(1),
        integral=IntegralComponent(2),
        fractional=IntegralComponent(3),
        string=IntegralComponent(4),
        object=IntegralComponent(5),
        tensor=IntegralComponent(6),
    )
    assert full_type_counts.tensor.value == 6
    merged_counts = full_type_counts.merge(type_counts)
    assert merged_counts.tensor.value == 7


def test_column_counts_with_and_without_nan_field():
    column_counts = ColumnCountsMetric(
        n=IntegralComponent(1),
        null=IntegralComponent(2),
    )
    assert column_counts.nan.value == 0
    assert column_counts.inf.value == 0
    column_counts.nan.set(1)
    assert column_counts.nan.value == 1
    assert column_counts.null.value == 2
    full_column_counts = ColumnCountsMetric(
        n=IntegralComponent(1),
        null=IntegralComponent(2),
        nan=IntegralComponent(3),
        inf=IntegralComponent(4),
    )
    assert full_column_counts.nan.value == 3
    assert full_column_counts.inf.value == 4


def test_column_counts_true_count() -> None:
    counts = ColumnCountsMetric.zero()
    p_col = PreprocessedColumn.apply(["twelve", 12, True, False, True, "True", None])
    operation_result = counts.columnar_update(p_col)
    assert operation_result is not None
    assert counts.n.value == 7
    assert counts.true.value == 2
    assert operation_result.ok

    # This is a serialized ColumnCountsMetric from before adding the true component
    msg = b'\n\t\n\x03nan\x12\x02\x10\x00\n\n\n\x04null\x12\x02\x10\x01\n\t\n\x03inf\x12\x02\x10\x00\n\x07\n\x01n\x12\x02\x10\x07'
    buf = MetricMessage()
    buf.ParseFromString(msg)
    deserialized = ColumnCountsMetric.from_protobuf(buf)
    assert deserialized.n.value == 7
    assert deserialized.true.value == 0
