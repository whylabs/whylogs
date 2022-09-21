import numpy as np
import pytest

from whylogs.core.metrics.compound_metric import CompoundMetric
from whylogs.core.metrics.metrics import (
    DistributionMetric,
    IntsMetric,
    MetricConfig,
    custom_metric,
)
from whylogs.core.preprocessing import PreprocessedColumn


@custom_metric
class GoodCM(CompoundMetric):
    """Trivial CompoundMetric subclass for unit testing"""

    @property
    def namespace(self) -> str:
        return "good"

    @classmethod
    def zero(cls, config: MetricConfig) -> "GoodCM":
        return cls({})


def test_compound_metric() -> None:
    metric = GoodCM(
        {
            "Metric1": DistributionMetric.zero(MetricConfig()),
            "Metric2": DistributionMetric.zero(MetricConfig()),
        },
    )
    data = [10, 20, 30]
    arr = np.array(data)
    col = PreprocessedColumn.apply(arr)
    metric.columnar_update(col)

    assert metric.submetrics["Metric1"].kll.value.get_n() == 3
    assert metric.submetrics["Metric2"].mean.value == arr.mean()


def test_add_submetric() -> None:
    metric = GoodCM({"metric1": DistributionMetric.zero(MetricConfig())})
    col = PreprocessedColumn.apply(np.array([1, 2, 3]))
    metric.columnar_update(col)
    metric.submetrics["metric2"] = DistributionMetric.zero(MetricConfig())
    metric.columnar_update(col)
    assert metric.submetrics["metric1"].kll.value.get_n() == 6
    assert metric.submetrics["metric2"].kll.value.get_n() == 3


def test_merge_symmetric_set_difference() -> None:
    metric1 = GoodCM(
        {
            "metric1": DistributionMetric.zero(MetricConfig()),
            "metric2": DistributionMetric.zero(MetricConfig()),
        },
    )
    metric2 = GoodCM(
        {
            "metric2": DistributionMetric.zero(MetricConfig()),
            "metric3": DistributionMetric.zero(MetricConfig()),
        },
    )
    col = PreprocessedColumn.apply(np.array([1, 2, 3]))
    metric1.columnar_update(col)
    metric2.columnar_update(col)
    merged = metric1 + metric2
    assert merged.submetrics["metric1"].kll.value.get_n() == 3
    assert merged.submetrics["metric2"].kll.value.get_n() == 6
    assert merged.submetrics["metric3"].kll.value.get_n() == 3


def test_merge_submetrics_disagree() -> None:
    metric1 = GoodCM(
        {
            "submetric": DistributionMetric.zero(MetricConfig()),
        },
    )
    metric2 = GoodCM(
        {
            "submetric": IntsMetric.zero(MetricConfig()),
        },
    )
    col = PreprocessedColumn.apply(np.array([1, 2, 3]))
    metric1.columnar_update(col)
    metric2.columnar_update(col)
    with pytest.raises(ValueError):
        metric1 + metric2


def test_colon_in_namespace_fails() -> None:
    with pytest.raises(ValueError):

        @custom_metric
        class BadCM1(CompoundMetric):
            @property
            def namespace(self) -> str:
                return "bad:namespace"

            @classmethod
            def zero(cls, config: MetricConfig) -> "BadCM1":
                return cls({})


def test_slash_in_namespace_fails() -> None:
    with pytest.raises(ValueError):

        @custom_metric
        class BadCM2(CompoundMetric):
            @property
            def namespace(self) -> str:
                return "bad/namespace"

            @classmethod
            def zero(cls, config: MetricConfig) -> "BadCM2":
                return cls({})


@pytest.mark.parametrize(
    "cls, metrics",
    [
        (GoodCM, {"bad:name": DistributionMetric.zero(MetricConfig())}),
        (GoodCM, {"bad/name": DistributionMetric.zero(MetricConfig())}),
    ],
)
def test_compound_metric_invalid_initialization(cls, metrics):
    with pytest.raises(ValueError):
        cls(metrics)


def test_compound_metric_serialization() -> None:
    metric = GoodCM(
        {
            "Metric1": DistributionMetric.zero(MetricConfig()),
            "Metric2": DistributionMetric.zero(MetricConfig()),
        },
    )
    data = [10, 20, 30]
    arr = np.array(data)
    col = PreprocessedColumn.apply(arr)
    metric.columnar_update(col)
    msg = metric.to_protobuf()
    deserialized = GoodCM.from_protobuf(msg)

    assert deserialized.namespace == metric.namespace
    assert deserialized.submetrics["Metric1"].kll.value.get_n() == 3
    assert deserialized.submetrics["Metric2"].mean.value == arr.mean()
    assert len(deserialized.submetrics) == 2


def test_compound_metric_summary() -> None:
    metric = GoodCM(
        {
            "Metric1": DistributionMetric.zero(MetricConfig()),
            "Metric2": DistributionMetric.zero(MetricConfig()),
        },
    )
    data = [10, 20, 30]
    arr = np.array(data)
    col = PreprocessedColumn.apply(arr)
    metric.columnar_update(col)
    summary = metric.to_summary_dict(None)

    assert "Metric1/mean" in summary
    assert "Metric1/stddev" in summary
    assert "Metric2/n" in summary
    assert "Metric2/median" in summary


def test_compound_metric_merge() -> None:
    metric1 = GoodCM(
        {
            "Metric1": DistributionMetric.zero(MetricConfig()),
            "Metric2": DistributionMetric.zero(MetricConfig()),
        },
    )
    data1 = [10, 20, 30]
    arr1 = np.array(data1)
    col = PreprocessedColumn.apply(arr1)
    metric1.columnar_update(col)
    d1 = DistributionMetric.zero(MetricConfig())
    d1.columnar_update(col)

    metric2 = GoodCM(
        {
            "Metric1": DistributionMetric.zero(MetricConfig()),
            "Metric2": DistributionMetric.zero(MetricConfig()),
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

    assert merged.submetrics["Metric1"].kll.value.get_n() == d_merged.kll.value.get_n()
    assert merged.submetrics["Metric1"].mean.value == d_merged.mean.value
    assert merged.submetrics["Metric1"].stddev == d_merged.stddev
    assert merged.submetrics["Metric2"].kll.value.get_n() == d_merged.kll.value.get_n()
    assert merged.submetrics["Metric2"].mean.value == d_merged.mean.value
    assert merged.submetrics["Metric2"].stddev == d_merged.stddev
