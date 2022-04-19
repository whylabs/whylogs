from whylogs.core import ColumnSchema
from whylogs.core.metrics import ColumnCountsMetric
from whylogs.core.preprocessing import PreprocessedColumn


def test_null_count() -> None:
    counts = ColumnCountsMetric.zero(ColumnSchema(dtype=int))
    p_col = PreprocessedColumn.apply([None, None, None, None, 1, 2])
    counts.columnar_update(p_col)

    assert counts.null.value == 4
    assert counts.n.value == 6
