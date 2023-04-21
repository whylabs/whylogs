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


import pandas as pd
import whylogs as why
from whylogs.experimental.core.metrics.udf_metric import generate_udf_schema, register_metric_udf
from whylogs.core.resolvers import (
    DeclarativeResolver,
    STANDARD_RESOLVER,
)
from whylogs.core.schema import DeclarativeSchema


@register_metric_udf(col_name="col1")
def add5(x):
    return x + 5


#@register_metric_udf("col1")
def tostr(x):
    return str(x)


@register_metric_udf("col2", submetric_name="square")
def frob(x):
    return x * x


def print_sum(view):
    print()
    for col_name, col_view in view._columns.items():
        print(f"{col_name}/")
        for met_name, metric in view._columns[col_name]._metrics.items():
            print(f"  {met_name}/")
            summary = metric.to_summary_dict()
            for comp, val in summary.items():
                print(f"    {comp}: {val}")
            print()
        print()


def test_voodoo() -> None:
    schema = DeclarativeSchema(STANDARD_RESOLVER + generate_udf_schema())
    data = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6], "col3": [7, 8, 9]})
    view = why.log(data, schema=schema).profile().view()
    print_sum(view)
    assert False
