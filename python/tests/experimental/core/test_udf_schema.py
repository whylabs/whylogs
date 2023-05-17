import pandas as pd

import whylogs as why
from whylogs.core.datatypes import Fractional, Integral, String
from whylogs.core.metrics import StandardMetric
from whylogs.core.resolvers import STANDARD_RESOLVER, MetricSpec, ResolverSpec
from whylogs.experimental.core.udf_schema import (
    UdfSchema,
    UdfSpec,
    generate_udf_dataset_schema,
    register_dataset_udf,
)


def test_udf_row() -> None:
    schema = UdfSchema(
        STANDARD_RESOLVER,
        udf_specs=[UdfSpec(column_names=["col1"], udfs={"col2": lambda x: x["col1"], "col3": lambda x: x["col1"]})],
    )
    data = {"col1": 42}
    results = why.log(row=data, schema=schema).view()
    col1 = results.get_column("col1").to_summary_dict()
    col2 = results.get_column("col2").to_summary_dict()
    col3 = results.get_column("col3").to_summary_dict()
    assert col1 == col2 == col3


def test_udf_pandas() -> None:
    schema = UdfSchema(
        STANDARD_RESOLVER,
        udf_specs=[UdfSpec(column_names=["col1"], udfs={"col2": lambda x: x["col1"], "col3": lambda x: x["col1"]})],
    )
    data = pd.DataFrame({"col1": [42, 12, 7]})
    results = why.log(pandas=data, schema=schema).view()
    col1 = results.get_column("col1").to_summary_dict()
    col2 = results.get_column("col2").to_summary_dict()
    col3 = results.get_column("col3").to_summary_dict()
    assert col1 == col2 == col3


@register_dataset_udf(["col1"])
def add5(x):
    return x["col1"] + 5


def square(x):
    return x["col1"] * x["col1"]


def test_decorator_pandas() -> None:
    extra_spec = UdfSpec(["col1"], {"sqr": square})
    schema = generate_udf_dataset_schema([extra_spec], STANDARD_RESOLVER)
    data = pd.DataFrame({"col1": [42, 12, 7], "col2": ["a", "b", "c"]})
    results = why.log(pandas=data, schema=schema).view()
    col1_summary = results.get_column("col1").to_summary_dict()
    assert "distribution/n" in col1_summary
    add5_summary = results.get_column("add5").to_summary_dict()
    assert "distribution/n" in add5_summary
    sqr_summary = results.get_column("sqr").to_summary_dict()
    assert "distribution/n" in sqr_summary


def test_decorator_row() -> None:
    extra_spec = UdfSpec(["col1"], {"sqr": square})
    schema = generate_udf_dataset_schema([extra_spec], STANDARD_RESOLVER)
    results = why.log(row={"col1": 42, "col2": "a"}, schema=schema).view()
    col1_summary = results.get_column("col1").to_summary_dict()
    assert "distribution/n" in col1_summary
    add5_summary = results.get_column("add5").to_summary_dict()
    assert "distribution/n" in add5_summary
    sqr_summary = results.get_column("sqr").to_summary_dict()
    assert "distribution/n" in sqr_summary


@register_dataset_udf(["col1", "col2"], "product")
def times(x):
    return x["col1"] * x["col2"]


@register_dataset_udf(["col1", "col3"], metrics=[MetricSpec(StandardMetric.distribution.value)])
def ratio(x):
    return x["col1"] / x["col3"]


def test_multicolumn_udf_pandas() -> None:
    count_only = [
        ResolverSpec(
            column_type=Integral,
            metrics=[MetricSpec(StandardMetric.counts.value)],
        ),
        ResolverSpec(
            column_type=Fractional,
            metrics=[MetricSpec(StandardMetric.counts.value)],
        ),
        ResolverSpec(
            column_type=String,
            metrics=[MetricSpec(StandardMetric.counts.value)],
        ),
    ]

    extra_spec = UdfSpec(["col1"], {"sqr": square})
    schema = generate_udf_dataset_schema([extra_spec], count_only)
    data = pd.DataFrame({"col1": [42, 12, 7], "col2": [2, 3, 4], "col3": [2, 3, 4]})
    results = why.log(pandas=data, schema=schema).view()
    col1_summary = results.get_column("col1").to_summary_dict()
    assert "counts/n" in col1_summary
    col2_summary = results.get_column("col2").to_summary_dict()
    assert "counts/n" in col2_summary
    col3_summary = results.get_column("col3").to_summary_dict()
    assert "counts/n" in col3_summary
    add5_summary = results.get_column("add5").to_summary_dict()
    assert "counts/n" in add5_summary
    prod_summary = results.get_column("product").to_summary_dict()
    assert prod_summary["counts/n"] == 3
    sqr_summary = results.get_column("sqr").to_summary_dict()
    assert "counts/n" in sqr_summary
    div_summary = results.get_column("ratio").to_summary_dict()
    assert div_summary["distribution/n"] == 3
    assert len(results.get_column("ratio").get_metrics()) == 2  # Integral -> counts plus registered distribution


def test_multicolumn_udf_row() -> None:
    count_only = [
        ResolverSpec(
            column_type=Integral,
            metrics=[MetricSpec(StandardMetric.counts.value)],
        ),
        ResolverSpec(
            column_type=Fractional,
            metrics=[MetricSpec(StandardMetric.counts.value)],
        ),
        ResolverSpec(
            column_type=String,
            metrics=[MetricSpec(StandardMetric.counts.value)],
        ),
    ]

    extra_spec = UdfSpec(["col1"], {"sqr": square})
    schema = generate_udf_dataset_schema([extra_spec], count_only)
    data = {"col1": 42, "col2": 2, "col3": 2}
    results = why.log(row=data, schema=schema).view()
    col1_summary = results.get_column("col1").to_summary_dict()
    assert "counts/n" in col1_summary
    col2_summary = results.get_column("col2").to_summary_dict()
    assert "counts/n" in col2_summary
    col3_summary = results.get_column("col3").to_summary_dict()
    assert "counts/n" in col3_summary
    add5_summary = results.get_column("add5").to_summary_dict()
    assert "counts/n" in add5_summary
    prod_summary = results.get_column("product").to_summary_dict()
    assert prod_summary["counts/n"] == 1
    sqr_summary = results.get_column("sqr").to_summary_dict()
    assert "counts/n" in sqr_summary
    div_summary = results.get_column("ratio").to_summary_dict()
    assert div_summary["distribution/n"] == 1
    assert len(results.get_column("ratio").get_metrics()) == 2  # Integral -> counts plus registered distribution


n: int = 0


@register_dataset_udf(["oops"])
def exothermic(x):
    global n
    n += 1
    if n < 3:
        raise ValueError("kaboom")

    return x["oops"]


def test_udf_throws() -> None:
    schema = generate_udf_dataset_schema()
    df = pd.DataFrame({"oops": [1, 2, 3, 4], "ok": [5, 6, 7, 8]})
    results = why.log(pandas=df, schema=schema).view()
    assert "exothermic" not in results.get_columns()
    # oops_summary = results.get_column("exothermic").to_summary_dict()
    ok_summary = results.get_column("ok").to_summary_dict()
    assert ok_summary["counts/n"] == 4
