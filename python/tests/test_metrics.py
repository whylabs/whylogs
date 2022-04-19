import numpy as np

from whylogs.core import ColumnSchema
from whylogs.core.metrics.maths import VarianceM2Result, parallel_variance_m2
from whylogs.core.metrics.metrics import DistributionMetric
from whylogs.core.preprocessing import PreprocessedColumn


def test_distribution_metrics_numpy() -> None:
    dist = DistributionMetric.zero(ColumnSchema(dtype=int))
    data = list(range(0, 100))
    arr = np.array(data)
    col = PreprocessedColumn.apply(arr)
    dist.columnar_update(col)

    assert dist.kll.value.get_n() == 100
    assert dist.mean.value == arr.mean()
    assert dist.variance == arr.var()


def test_distribution_metrics_list() -> None:
    dist = DistributionMetric.zero(ColumnSchema(dtype=int))
    col = PreprocessedColumn()
    data = list(range(0, 100))
    col.list.ints = data
    dist.columnar_update(col)

    assert dist.kll.value.get_n() == 100
    assert dist.mean.value == np.array(data).mean()
    assert dist.variance == np.array(data).var()


def test_distribution_metrics_mixed_np_and_list() -> None:
    dist = DistributionMetric.zero(ColumnSchema(dtype=int))
    col = PreprocessedColumn()
    col.list.ints = list(range(0, 50))
    col.numpy.ints = np.array(range(50, 100))
    dist.columnar_update(col)

    assert dist.kll.value.get_n() == 100
    a = np.array(col.list.ints)
    b = col.numpy.ints

    assert dist.mean.value == np.array(np.concatenate([a, b])).mean()

    m2_a = a.var() * (len(a) - 1)
    m2_b = b.var() * (len(b) - 1)
    a_var = VarianceM2Result(n=len(a), mean=a.mean(), m2=m2_a)
    b_var = VarianceM2Result(n=len(b), mean=b.mean(), m2=m2_b)
    overall = parallel_variance_m2(first=a_var, second=b_var)
    assert dist.variance == overall.m2 / (overall.n - 1)
