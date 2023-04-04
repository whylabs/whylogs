from whylogs.core.preprocessing import PreprocessedColumn
from whylogs.experimental.core.metrics.udf_metric import UdfMetric, UdfMetricConfig


def test_udf_metric() -> None:
    config = UdfMetricConfig(
        udfs={
            "fortytwo": lambda x: 42,
            "foo": lambda x: "bar",
        },
    )
    metric = UdfMetric.zero(config)
    metric.columnar_update(PreprocessedColumn.apply([0]))
    summary = metric.to_summary_dict()

    assert summary["fortytwo:counts/n"] == 1
    assert summary["fortytwo:types/integral"] == 1
    assert summary["fortytwo:types/string"] == 0
    assert summary["fortytwo:cardinality/est"] == 1
    assert summary["fortytwo:distribution/n"] == 1
    assert summary["fortytwo:distribution/mean"] == 42
    assert summary["fortytwo:ints/max"] == 42
    assert summary["fortytwo:ints/min"] == 42
    assert "fortytwo:frequent_items/frequent_strings" in summary

    assert summary["foo:counts/n"] == 1
    assert summary["foo:types/integral"] == 0
    assert summary["foo:types/string"] == 1
    assert summary["foo:cardinality/est"] == 1
    assert "foo:frequent_items/frequent_strings" in summary


def test_merge() -> None:
    config = UdfMetricConfig(
        udfs={
            "fortytwo": lambda x: 42,
            "foo": lambda x: "bar",
        },
    )
    metric1 = UdfMetric.zero(config)
    metric1.columnar_update(PreprocessedColumn.apply([0]))
    metric2 = UdfMetric.zero(config)
    metric2.columnar_update(PreprocessedColumn.apply([0]))
    metric = metric1.merge(metric2)
    assert isinstance(metric, UdfMetric)
    assert "fortytwo" in metric._udfs
    assert "fortytwo" in metric.submetrics
    assert "foo" in metric._udfs
    assert "foo" in metric.submetrics

    summary = metric.to_summary_dict()
    assert summary["fortytwo:counts/n"] == 2
    assert summary["fortytwo:types/integral"] == 2
    assert summary["fortytwo:types/string"] == 0
    assert summary["fortytwo:cardinality/est"] == 1
    assert summary["fortytwo:distribution/n"] == 2
    assert summary["fortytwo:distribution/mean"] == 42
    assert summary["fortytwo:ints/max"] == 42
    assert summary["fortytwo:ints/min"] == 42
    assert "fortytwo:frequent_items/frequent_strings" in summary

    assert summary["foo:counts/n"] == 2
    assert summary["foo:types/integral"] == 0
    assert summary["foo:types/string"] == 2
    assert summary["foo:cardinality/est"] == 1
    assert "foo:frequent_items/frequent_strings" in summary
