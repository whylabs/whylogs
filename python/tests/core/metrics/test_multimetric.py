from typing import Optional

import numpy as np
import pytest

from whylogs.core.metrics.multimetric import MultiMetric
from whylogs.core.metrics.column_metrics import ColumnCountsMetric, TypeCountersMetric
from whylogs.core.metrics.metrics import (
    CardinalityMetric,
    DistributionMetric,
    FrequentItemsMetric,
    IntsMetric,
    MetricConfig,
    custom_metric,
)
from whylogs.core.preprocessing import PreprocessedColumn


@custom_metric
class GoodMM(MultiMetric):
    """Trivial CompoundMetric subclass for unit testing"""

    @property
    def namespace(self) -> str:
        return "good"

    @classmethod
    def zero(cls, config: Optional[MetricConfig] = None) -> "GoodCM":
        return cls({})


def test_multimetric() -> None:
    metric = GoodMM(
        {
            "Metric1": {
                CardinalityMetric.get_namespace(): CardinalityMetric.zero(),
                ColumnCountsMetric.get_namespace(): ColumnCountsMetric.zero(),
                DistributionMetric.get_namespace(): DistributionMetric.zero(),
                IntsMetric.get_namespace(): IntsMetric.zero(),
                TypeCountersMetric.get_namespace(): TypeCountersMetric.zero(),
            },
            "Metric2": {
                CardinalityMetric.get_namespace(): CardinalityMetric.zero(),
                DistributionMetric.get_namespace(): DistributionMetric.zero(),
                FrequentItemsMetric.get_namespace(): FrequentItemsMetric.zero(),
                TypeCountersMetric.get_namespace(): TypeCountersMetric.zero(),
            },
        },
    )
    data = [10, 20, 30]
    arr = np.array(data)
    col = PreprocessedColumn.apply(arr)
    metric.columnar_update(col)

    assert metric.submetrics["Metric1"]["distribution"].kll.value.get_n() == 3
    assert metric.submetrics["Metric1"]["counts"].n.value == 3
    assert 0 < metric.submetrics["Metric1"]["cardinality"].hll.value.get_estimate() < 4
    assert metric.submetrics["Metric1"]["ints"].max.value == 30
    assert metric.submetrics["Metric1"]["types"].integral.value == 3
    assert metric.submetrics["Metric1"]["types"].string.value == 0

    assert 0 < metric.submetrics["Metric2"]["cardinality"].hll.value.get_estimate() < 4
    assert metric.submetrics["Metric2"]["distribution"].mean.value == arr.mean()
    assert metric.submetrics["Metric2"]["types"].integral.value == 3
    assert metric.submetrics["Metric2"]["types"].string.value == 0


def test_add_submetric() -> None:
    metric = GoodMM(
        {
            "metric1": {
                CardinalityMetric.get_namespace(): CardinalityMetric.zero(),
                ColumnCountsMetric.get_namespace(): ColumnCountsMetric.zero(),
                DistributionMetric.get_namespace(): DistributionMetric.zero(),
                IntsMetric.get_namespace(): IntsMetric.zero(),
                TypeCountersMetric.get_namespace(): TypeCountersMetric.zero(),
            },
        }
    )
    col = PreprocessedColumn.apply(np.array([1, 2, 3]))
    metric.columnar_update(col)
    metric.submetrics["metric2"] = {
        CardinalityMetric.get_namespace(): CardinalityMetric.zero(),
        DistributionMetric.get_namespace(): DistributionMetric.zero(),
        FrequentItemsMetric.get_namespace(): FrequentItemsMetric.zero(),
        TypeCountersMetric.get_namespace(): TypeCountersMetric.zero(),
    }
    metric.columnar_update(col)
    assert metric.submetrics["metric1"]["distribution"].kll.value.get_n() == 6
    assert metric.submetrics["metric2"]["distribution"].kll.value.get_n() == 3


def test_merge_symmetric_set_difference() -> None:
    metric1 = GoodMM(
        {
            "metric1": {
                DistributionMetric.get_namespace(): DistributionMetric.zero(),
            },
            "metric2": {
                ColumnCountsMetric.get_namespace(): ColumnCountsMetric.zero(),
                DistributionMetric.get_namespace(): DistributionMetric.zero(),
                TypeCountersMetric.get_namespace(): TypeCountersMetric.zero(),
            },
        },
    )
    metric2 = GoodMM(
        {
            "metric2": {
                ColumnCountsMetric.get_namespace(): ColumnCountsMetric.zero(),
                DistributionMetric.get_namespace(): DistributionMetric.zero(),
                FrequentItemsMetric.get_namespace(): FrequentItemsMetric.zero(),
            },
            "metric3": {
                ColumnCountsMetric.get_namespace(): ColumnCountsMetric.zero(),
            },
        },
    )
    col = PreprocessedColumn.apply(np.array([1, 2, 3]))
    metric1.columnar_update(col)
    metric2.columnar_update(col)
    merged = metric1 + metric2
    assert merged.submetrics["metric1"]["distribution"].kll.value.get_n() == 3
    assert merged.submetrics["metric2"]["counts"].n.value == 6
    assert merged.submetrics["metric2"]["distribution"].kll.value.get_n() == 6
    assert merged.submetrics["metric2"]["types"].integral.value == 3
    assert FrequentItemsMetric.get_namespace() in merged.submetrics["metric2"]
    assert merged.submetrics["metric3"]["counts"].n.value == 3


def test_merge_submetrics_disagree() -> None:
    metric1 = GoodMM(
        {
            "submetric": {
                DistributionMetric.get_namespace(): DistributionMetric.zero(),
            },
        },
    )
    metric2 = GoodMM(
        {
            "submetric": {
                IntsMetric.get_namespace(): IntsMetric.zero(),
            },
        },
    )
    col = PreprocessedColumn.apply(np.array([1, 2, 3]))
    metric1.columnar_update(col)
    metric2.columnar_update(col)
    merged = metric1 + metric2
    assert IntsMetric.get_namespace() in merged.submetrics["submetric"]
    assert DistributionMetric.get_namespace() in merged.submetrics["submetric"]


def test_colon_in_namespace_fails() -> None:
    with pytest.raises(ValueError):

        @custom_metric
        class BadMM1(MultiMetric):
            @property
            def namespace(self) -> str:
                return "bad:namespace"

            @classmethod
            def zero(cls, config: Optional[MetricConfig] = None) -> "BadMM1":
                return cls({})


def test_slash_in_namespace_fails() -> None:
    with pytest.raises(ValueError):

        @custom_metric
        class BadMM2(MultiMetric):
            @property
            def namespace(self) -> str:
                return "bad/namespace"

            @classmethod
            def zero(cls, config: Optional[MetricConfig] = None) -> "BadMM2":
                return cls({})


@pytest.mark.parametrize(
    "cls, metrics",
    [
        (GoodMM, {"bad:name": {DistributionMetric.get_namespace(): DistributionMetric.zero()}}),
        (GoodMM, {"bad/name": {DistributionMetric.get_namespace(): DistributionMetric.zero()}}),
    ],
)
def test_compound_metric_invalid_initialization(cls, metrics):
    with pytest.raises(ValueError):
        cls(metrics)


def test_compound_metric_serialization() -> None:
    metric = GoodMM(
        {
            "Metric1": {DistributionMetric.get_namespace(): DistributionMetric.zero()},
            "Metric2": {DistributionMetric.get_namespace(): DistributionMetric.zero()},
        },
    )
    data = [10, 20, 30]
    arr = np.array(data)
    col = PreprocessedColumn.apply(arr)
    metric.columnar_update(col)
    msg = metric.to_protobuf()
    deserialized = GoodMM.from_protobuf(msg)

    assert deserialized.namespace == metric.namespace
    assert deserialized.submetrics["Metric1"]["distribution"].kll.value.get_n() == 3
    assert deserialized.submetrics["Metric2"]["distribution"].mean.value == arr.mean()
    assert len(deserialized.submetrics) == 2


def test_compound_metric_summary() -> None:
    metric = GoodMM(
        {
            "Metric1": {DistributionMetric.get_namespace(): DistributionMetric.zero()},
            "Metric2": {DistributionMetric.get_namespace(): DistributionMetric.zero()},
        },
    )
    data = [10, 20, 30]
    arr = np.array(data)
    col = PreprocessedColumn.apply(arr)
    metric.columnar_update(col)
    summary = metric.to_summary_dict(None)

    assert "Metric1:distribution/mean" in summary
    assert "Metric1:distribution/stddev" in summary
    assert "Metric2:distribution/n" in summary
    assert "Metric2:distribution/median" in summary


def test_compound_metric_merge() -> None:
    metric1 = GoodMM(
        {
            "Metric1": {DistributionMetric.get_namespace(): DistributionMetric.zero()},
            "Metric2": {DistributionMetric.get_namespace(): DistributionMetric.zero()},
        },
    )
    data1 = [10, 20, 30]
    arr1 = np.array(data1)
    col = PreprocessedColumn.apply(arr1)
    metric1.columnar_update(col)
    d1 = DistributionMetric.zero(MetricConfig())
    d1.columnar_update(col)

    metric2 = GoodMM(
        {
            "Metric1": {DistributionMetric.get_namespace(): DistributionMetric.zero()},
            "Metric2": {DistributionMetric.get_namespace(): DistributionMetric.zero()},
        },
    )
    data2 = [40, 50, 60]
    arr2 = np.array(data2)
    col = PreprocessedColumn.apply(arr2)
    metric2.columnar_update(col)
    d2 = DistributionMetric.zero(MetricConfig())
    d2.columnar_update(col)

    merged = metric1 + metric2
    d_merged = d1 + d2

    assert merged.submetrics["Metric1"]["distribution"].kll.value.get_n() == d_merged.kll.value.get_n()
    assert merged.submetrics["Metric1"]["distribution"].mean.value == d_merged.mean.value
    assert merged.submetrics["Metric1"]["distribution"].stddev == d_merged.stddev
    assert merged.submetrics["Metric2"]["distribution"].kll.value.get_n() == d_merged.kll.value.get_n()
    assert merged.submetrics["Metric2"]["distribution"].mean.value == d_merged.mean.value
    assert merged.submetrics["Metric2"]["distribution"].stddev == d_merged.stddev
