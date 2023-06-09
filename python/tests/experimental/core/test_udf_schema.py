import pandas as pd

import whylogs as why
from whylogs.core.dataset_profile import DatasetProfile
from whylogs.core.datatypes import Fractional, Integral, String
from whylogs.core.metrics import CardinalityMetric, DistributionMetric, StandardMetric
from whylogs.core.resolvers import STANDARD_RESOLVER, MetricSpec, ResolverSpec
from whylogs.core.segmentation_partition import segment_on_column
from whylogs.experimental.core.metrics.udf_metric import register_metric_udf
from whylogs.experimental.core.udf_schema import (
    UdfSchema,
    UdfSpec,
    register_dataset_udf,
    udf_schema,
)

#_reset_udfs()


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
    schema = udf_schema([extra_spec], STANDARD_RESOLVER)
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
    schema = udf_schema([extra_spec], STANDARD_RESOLVER)
    results = why.log(row={"col1": 42, "col2": "a"}, schema=schema).view()
    col1_summary = results.get_column("col1").to_summary_dict()
    assert "distribution/n" in col1_summary
    add5_summary = results.get_column("add5").to_summary_dict()
    assert "distribution/n" in add5_summary
    sqr_summary = results.get_column("sqr").to_summary_dict()
    assert "distribution/n" in sqr_summary


@register_dataset_udf(["col1"], "annihilate_me", anti_metrics=[CardinalityMetric, DistributionMetric])
def plus1(x):
    return x["col1"] + 1


def test_anti_resolver() -> None:
    schema = udf_schema()
    data = pd.DataFrame({"col1": [42, 12, 7], "col2": ["a", "b", "c"]})
    results = why.log(pandas=data, schema=schema).view()
    col1_summary = results.get_column("col1").to_summary_dict()
    assert "distribution/n" in col1_summary
    assert "cardinality/est" in col1_summary
    col2_summary = results.get_column("col2").to_summary_dict()
    assert "distribution/n" in col2_summary
    assert "cardinality/est" in col2_summary
    add5_summary = results.get_column("add5").to_summary_dict()
    assert "distribution/n" in add5_summary
    assert "cardinality/est" in add5_summary
    plus1_summary = results.get_column("annihilate_me").to_summary_dict()
    assert "ints/max" in plus1_summary
    assert "distribution/n" not in plus1_summary
    assert "cardinality/est" not in plus1_summary


@register_dataset_udf(["col1"], "colliding_name", namespace="pluto")
def a_function(x):
    return x["col1"]


@register_dataset_udf(["col1"], "colliding_name", namespace="neptune")
def another_function(x):
    return x["col1"]


def test_namespace() -> None:
    results = why.log(row={"col1": 42}, schema=udf_schema()).view()
    assert results.get_column("pluto.colliding_name") is not None
    assert results.get_column("neptune.colliding_name") is not None


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
    schema = udf_schema([extra_spec], count_only)
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
    # Integral -> counts plus registered distribution
    assert results.get_column("ratio").get_metric("counts") is not None
    assert results.get_column("ratio").get_metric("distribution") is not None


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
    schema = udf_schema([extra_spec], count_only)
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
    # Integral -> counts plus registered distribution
    assert results.get_column("ratio").get_metric("counts") is not None
    assert results.get_column("ratio").get_metric("distribution") is not None


n: int = 0


@register_dataset_udf(["oops"])
def exothermic(x):
    global n
    n += 1
    if n < 3:
        raise ValueError("kaboom")

    return x["oops"]


def test_udf_throws_pandas() -> None:
    global n
    n = 0
    schema = udf_schema()
    df = pd.DataFrame({"oops": [1, 2, 3, 4], "ok": [5, 6, 7, 8]})
    results = why.log(pandas=df, schema=schema).view()
    assert "exothermic" in results.get_columns()
    oops_summary = results.get_column("exothermic").to_summary_dict()
    assert oops_summary["counts/null"] > 0
    ok_summary = results.get_column("ok").to_summary_dict()
    assert ok_summary["counts/n"] == 4


def test_udf_throws_row() -> None:
    global n
    n = 0
    schema = udf_schema()
    data = {"oops": 1, "ok": 5}
    profile = why.log(row=data, schema=schema).profile()
    profile.track(row=data)
    profile.flush()
    ok_summary = profile.view().get_column("ok").to_summary_dict()
    assert ok_summary["counts/n"] == 2
    assert ok_summary["counts/null"] == 0
    oops_summary = profile.view().get_column("exothermic").to_summary_dict()
    assert oops_summary["counts/n"] == 2
    assert oops_summary["counts/null"] == 2
    profile.track(row=data)
    profile.track(row=data)
    profile.flush()
    oops_summary = profile.view().get_column("exothermic").to_summary_dict()
    assert oops_summary["counts/n"] == 4
    assert oops_summary["counts/null"] == 2
    ok_summary = profile.view().get_column("ok").to_summary_dict()
    assert ok_summary["counts/n"] == 4
    assert ok_summary["counts/null"] == 0


@register_metric_udf("foo")
def bar(x):
    return x


def test_udf_metric_resolving() -> None:
    schema = udf_schema()
    df = pd.DataFrame({"col1": [1, 2, 3], "foo": [1, 2, 3]})
    results = why.log(pandas=df, schema=schema).view()
    assert "add5" in results.get_columns()
    assert results.get_column("add5").to_summary_dict()["counts/n"] == 3
    assert results.get_column("col1").to_summary_dict()["counts/n"] == 3
    print(schema.resolvers._resolvers)
    foo_summary = results.get_column("foo").to_summary_dict()
    assert "udf/bar:counts/n" in foo_summary


def test_udf_segmentation_pandas() -> None:
    column_segments = segment_on_column("product")
    segmented_schema = udf_schema(segments=column_segments)
    data = pd.DataFrame({"col1": [42, 12, 7], "col2": [2, 3, 4], "col3": [2, 3, 4]})
    results = why.log(pandas=data, schema=segmented_schema)
    assert len(results.segments()) == 3


def test_udf_segmentation_row() -> None:
    column_segments = segment_on_column("product")
    segmented_schema = udf_schema(segments=column_segments)
    data = {"col1": 42, "col2": 2, "col3": 2}
    results = why.log(row=data, schema=segmented_schema)
    assert len(results.segments()) == 1


def test_udf_segmentation_obj() -> None:
    column_segments = segment_on_column("product")
    segmented_schema = udf_schema(segments=column_segments)
    data = {"col1": 42, "col2": 2, "col3": 2}
    results = why.log(data, schema=segmented_schema)
    assert len(results.segments()) == 1


def test_udf_track() -> None:
    schema = udf_schema()
    prof = DatasetProfile(schema)
    data = pd.DataFrame({"col1": [42, 12, 7], "col2": [2, 3, 4], "col3": [2, 3, 4]})
    prof.track(data)
    results = prof.view()
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
    div_summary = results.get_column("ratio").to_summary_dict()
    assert div_summary["distribution/n"] == 3


@register_dataset_udf(["schema.col1"], schema_name="bob")
def bob(x):
    return x["schema.col1"]


@register_metric_udf("schema.col1", schema_name="bob")
def rob(x):
    return x


@register_dataset_udf(["schema.col1"], "add5")
def fob(x):
    return x["schema.col1"] + 5


def test_schema_name() -> None:
    default_schema = udf_schema()
    data = pd.DataFrame({"schema.col1": [42, 12, 7]})
    default_view = why.log(data, schema=default_schema).view()
    assert "add5" in default_view.get_columns()
    assert "bob" not in default_view.get_columns()
    assert "udf" not in default_view.get_column("schema.col1").get_metric_names()

    bob_schema = udf_schema(schema_name="bob")
    data = pd.DataFrame({"schema.col1": [42, 12, 7]})
    bob_view = why.log(data, schema=bob_schema).view()
    assert "add5" not in bob_view.get_columns()
    assert "bob" in bob_view.get_columns()
    assert "udf" in bob_view.get_column("schema.col1").get_metric_names()
