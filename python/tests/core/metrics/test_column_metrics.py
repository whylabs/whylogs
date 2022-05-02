from whylogs.core import ColumnSchema
from whylogs.core.metrics import ColumnCountsMetric, TypeCountersMetric
from whylogs.core.preprocessing import PreprocessedColumn


def test_null_count() -> None:
    counts = ColumnCountsMetric.zero(ColumnSchema(dtype=int))
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
