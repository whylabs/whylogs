import pandas as pd

import whylogs as why
from whylogs.core.datatypes import String
from whylogs.core.preprocessing import PreprocessedColumn
from whylogs.core.schema import DeclarativeSchema
from whylogs.experimental.core.metrics.udf_metric import (
    UdfMetric,
    UdfMetricConfig,
    generate_udf_metric_schema,
    register_metric_udf,
)


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


@register_metric_udf(col_name="col1")
def add5(x):
    return x + 5


@register_metric_udf("col1")
def tostr(x):
    return str(x)


@register_metric_udf("col2", submetric_name="square")
def frob(x):
    return x * x


@register_metric_udf(col_type=String)
def upper(x):
    return x.upper()


def test_decorator() -> None:
    schema = DeclarativeSchema(generate_udf_metric_schema())
    data = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6], "col3": [7, 8, 9], "col4": ["a", "b", "c"]})
    view = why.log(data, schema=schema).profile().view()
    col1_view = view.get_column("col1")
    col2_view = view.get_column("col2")
    col3_view = view.get_column("col3")
    col4_view = view.get_column("col4")
    assert "udf" in col1_view.get_metric_names()
    assert "udf" in col2_view.get_metric_names()
    assert "udf" not in col3_view.get_metric_names()
    assert "udf" in col4_view.get_metric_names()

    col1_summary = col1_view.to_summary_dict()
    assert "udf/add5:counts/n" in col1_summary
    assert "udf/add5:types/integral" in col1_summary
    assert "udf/add5:distribution/n" in col1_summary
    assert "udf/add5:ints/max" in col1_summary
    assert "udf/add5:cardinality/est" in col1_summary

    assert "udf/tostr:counts/n" in col1_summary
    assert "udf/tostr:types/integral" in col1_summary
    assert "udf/tostr:distribution/n" in col1_summary
    assert "udf/tostr:cardinality/est" in col1_summary
    assert "udf/tostr:frequent_items/frequent_strings" in col1_summary

    col2_summary = col2_view.to_summary_dict()
    assert "udf/square:counts/n" in col2_summary
    assert "udf/square:types/integral" in col2_summary
    assert "udf/square:distribution/n" in col2_summary
    assert "udf/square:cardinality/est" in col2_summary

    col4_summary = col4_view.to_summary_dict()
    print(col4_summary)
    assert "udf/upper:counts/n" in col4_summary
    assert "udf/upper:types/integral" in col4_summary
    assert "udf/upper:distribution/n" in col4_summary
    assert "udf/upper:cardinality/est" in col4_summary
    assert "udf/upper:frequent_items/frequent_strings" in col4_summary
