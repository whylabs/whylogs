from typing import Any, Tuple

import pytest
import sys

import whylogs as why
from whylogs.core.dataset_profile import DatasetProfile
from whylogs.core.datatypes import Fractional, Integral, String
from whylogs.core.metrics import CardinalityMetric, DistributionMetric, StandardMetric
from whylogs.core.resolvers import STANDARD_RESOLVER, MetricSpec, ResolverSpec
from whylogs.core.segmentation_partition import segment_on_column
from whylogs.core.stubs import pl
from whylogs.experimental.core.metrics.udf_metric import register_metric_udf
from whylogs.experimental.core.udf_schema import (
    UdfSchema,
    UdfSpec,
    register_dataset_udf,
    register_multioutput_udf,
    register_type_udf,
    udf_schema,
)
from whylogs.experimental.core.validators import condition_validator


if sys.version_info < (3, 8):
    pytest.skip(allow_module_level=True, reason="Polars requires Python >= 3.8")


def test_udf_polars() -> None:
    schema = UdfSchema(
        STANDARD_RESOLVER,
        udf_specs=[UdfSpec(column_names=["col1"], udfs={"col2": lambda x: x[0], "col3": lambda x: x[0]})],
    )
    data = pl.DataFrame({"col1": [42, 12, 7]})
    results = why.log(data, schema=schema).view()
    col1 = results.get_column("col1").to_summary_dict()
    col2 = results.get_column("col2").to_summary_dict()
    col3 = results.get_column("col3").to_summary_dict()
    assert col1 == col2 == col3
    assert len(data.columns) == 1


@register_multioutput_udf(["xx1", "xx2"], schema_name="polars")
def f1(x) -> pl.DataFrame:
    return pl.DataFrame({"foo": x["xx1"], "bar": x["xx2"]})


@register_multioutput_udf(["xx1", "xx2"], prefix="blah", schema_name="polars")
def f2(x) -> pl.DataFrame:
    return pl.DataFrame({"foo": x["xx1"], "bar": x["xx2"]})


@register_multioutput_udf(["xx1", "xx2"], no_prefix=True, schema_name="polars")
def no_prefix_udf(x) -> pl.DataFrame:
    df = pl.DataFrame({"foo": x["xx1"], "bar": x["xx2"]})
    return df


def test_multioutput_udf_dataframe() -> None:
    schema = udf_schema(schema_name="polars")
    df = pl.DataFrame({"xx1": [42, 7], "xx2": [3.14, 2.72]})
    results = why.log(df, schema=schema).view()
    assert results.get_column("f1.foo") is not None
    assert results.get_column("f1.bar") is not None
    assert results.get_column("blah.foo") is not None
    assert results.get_column("blah.bar") is not None
    assert results.get_column("foo") is not None
    assert results.get_column("bar") is not None


def test_drop_columns() -> None:
    schema = udf_schema(drop_columns={"xx1", "xx2"}, schema_name="polars")
    df = pl.DataFrame({"xx1": [42, 7], "xx2": [3.14, 2.72]})
    results = why.log(df, schema=schema).view()
    assert results.get_column("xx1") is None
    assert results.get_column("xx2") is None
    # UDFs that needed the dropped columns as input still work
    assert results.get_column("f1.foo") is not None
    assert results.get_column("f1.bar") is not None
    assert results.get_column("blah.foo") is not None
    assert results.get_column("blah.bar") is not None
    assert results.get_column("foo") is not None
    assert results.get_column("bar") is not None


@register_dataset_udf(["col1"], schema_name="polars-unit-tests")
def add5(x) -> float:
    return x[0] + 5


def square(x: Tuple) -> float:
    return x[0] * x[0]


action_list = []


def do_something_important(validator_name, condition_name: str, value: Any, column_id=None):
    print("Validator: {}\n    Condition name {} failed for value {}".format(validator_name, condition_name, value))
    action_list.append(value)
    if column_id:
        # this list is just to verify that the action was called with the correct column id
        action_list.append(column_id)
    return


@condition_validator(
    ["col1", "add5"], condition_name="less_than_four", actions=[do_something_important], schema_name="polars"
)
def lt_4(x):
    return x < 4


def test_validator_udf_polars() -> None:
    global action_list
    data = pl.DataFrame({"col1": [1, 3, 7]})
    schema = udf_schema(schema_name="polars", include_default_schema=False)
    why.log(data, schema=schema).view()
    assert 7 in action_list


def test_validator_double_register_udf_polars() -> None:
    global action_list

    @condition_validator(
        ["col1", "add5"], condition_name="less_than_four", actions=[do_something_important], schema_name="polars"
    )
    def lt_4_2(x):
        return x < 4

    schema = udf_schema(schema_name="polars", include_default_schema=False)
    # registering the same validator twice should keep only the latest registration
    assert schema.validators["col1"][0].conditions["less_than_four"].__name__ == "lt_4_2"
    print(f"schema.validators['col1'] = {schema.validators['col1']}")
    assert len(schema.validators["col1"]) == 1


def test_decorator_polars() -> None:
    extra_spec = UdfSpec(["col1"], {"sqr": square})
    schema = udf_schema([extra_spec], STANDARD_RESOLVER, schema_name="polars-unit-tests")
    data = pl.DataFrame({"col1": [42, 12, 7], "col2": ["a", "b", "c"]})
    results = why.log(data, schema=schema).view()
    col1_summary = results.get_column("col1").to_summary_dict()
    assert "distribution/n" in col1_summary
    add5_summary = results.get_column("add5").to_summary_dict()
    assert "distribution/n" in add5_summary
    sqr_summary = results.get_column("sqr").to_summary_dict()
    assert "distribution/n" in sqr_summary


@register_dataset_udf(
    ["col1"], "annihilate_me", anti_metrics=[CardinalityMetric, DistributionMetric], schema_name="polars-unit-tests"
)
def plus1(x) -> float:
    return x[0] + 1


def test_anti_resolver() -> None:
    schema = udf_schema(schema_name="polars-unit-tests")
    data = pl.DataFrame({"col1": [42, 12, 7], "col2": ["a", "b", "c"]})
    results = why.log(data, schema=schema).view()
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


@register_dataset_udf(["col1"], "colliding_name", namespace="pluto", schema_name="polars-unit-tests")
def a_function(x):
    return x[0]


@register_dataset_udf(["col1"], "colliding_name", namespace="neptune", schema_name="polars-unit-tests")
def another_function(x):
    return x[0]


@register_dataset_udf(["col1", "col2"], "product", schema_name="polars-unit-tests")
def times(x: Tuple) -> float:
    return x[0] * x[1]


@register_dataset_udf(
    ["col1", "col3"], metrics=[MetricSpec(StandardMetric.distribution.value)], schema_name="polars-unit-tests"
)
def ratio(x: Tuple) -> float:
    return x[0] / x[1]


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
    schema = udf_schema([extra_spec], count_only, schema_name="polars-unit-tests")
    data = pl.DataFrame({"col1": [42, 12, 7], "col2": [2, 3, 4], "col3": [2, 3, 4]})
    results = why.log(data, schema=schema).view()
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


n: int = 0


@register_dataset_udf(["oops"], schema_name="polars-unit-tests")
def exothermic(x: pl.DataFrame) -> pl.Series:
    global n
    n += 1
    if n < 3:
        raise ValueError("kaboom")

    return x["oops"]


def test_udf_throws_polars() -> None:
    global n
    n = 0
    schema = udf_schema(schema_name="polars-unit-tests")
    df = pl.DataFrame({"oops": [1, 2, 3, 4], "ok": [5, 6, 7, 8]})
    results = why.log(df, schema=schema).view()
    assert "exothermic" in results.get_columns()
    oops_summary = results.get_column("exothermic").to_summary_dict()
    assert oops_summary["counts/nan"] > 0
    ok_summary = results.get_column("ok").to_summary_dict()
    assert ok_summary["counts/n"] == 4


@register_metric_udf("foo")
def bar(x: Any) -> Any:
    return x


def test_udf_metric_resolving() -> None:
    schema = udf_schema(schema_name="polars-unit-tests")
    df = pl.DataFrame({"col1": [1, 2, 3], "foo": [1, 2, 3]})
    results = why.log(df, schema=schema).view()
    assert "add5" in results.get_columns()
    assert results.get_column("add5").to_summary_dict()["counts/n"] == 3
    assert results.get_column("col1").to_summary_dict()["counts/n"] == 3
    foo_summary = results.get_column("foo").to_summary_dict()
    assert "udf/bar:counts/n" in foo_summary


def test_udf_segmentation_pandas() -> None:
    column_segments = segment_on_column("product")
    segmented_schema = udf_schema(segments=column_segments, schema_name="polars-unit-tests")
    data = pl.DataFrame({"col1": [42, 12, 7], "col2": [2, 3, 4], "col3": [2, 3, 4]})
    results = why.log(data, schema=segmented_schema)
    assert len(results.segments()) == 3


def test_udf_segmentation_obj() -> None:
    column_segments = segment_on_column("product")
    segmented_schema = udf_schema(segments=column_segments, schema_name="polars-unit-tests")
    data = {"col1": 42, "col2": 2, "col3": 2}
    results = why.log(data, schema=segmented_schema)
    assert len(results.segments()) == 1


def test_udf_track() -> None:
    schema = udf_schema(schema_name="polars-unit-tests")
    prof = DatasetProfile(schema)
    data = pl.DataFrame({"col1": [42, 12, 7], "col2": [2, 3, 4], "col3": [2, 3, 4]})
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


@register_dataset_udf(["schema.col1"], schema_name="polars-bob")
def bob(x: pl.DataFrame) -> pl.Series:
    return x["schema.col1"]


@register_metric_udf("schema.col1", schema_name="polars-bob")
def rob(x: Any) -> Any:
    return x


@register_dataset_udf(["schema.col1"], "add5", schema_name="polars")
def fob(x: pl.DataFrame) -> pl.Series:
    return x["schema.col1"] + 5


def test_direct_udfs() -> None:
    schema = udf_schema(schema_name=["polars", "polars-bob"])
    data = pl.DataFrame({"col1": [42, 12, 7]})
    more_data, _ = schema.apply_udfs(polars=data)
    udf_columns = set(more_data.columns)

    result = why.log(data, schema=schema).view()
    profile_columns = set(result.get_columns())
    assert udf_columns == profile_columns

    result = why.log(more_data, schema=schema).view()
    more_columns = set(result.get_columns())
    assert more_columns == profile_columns


@register_type_udf(Fractional, schema_name="polars-unit-tests")
def square_type(x: pl.Series) -> pl.Series:
    return x * x


def test_type_udf_dataframe() -> None:
    schema = udf_schema(schema_name="polars-unit-tests")
    data = pl.DataFrame({"col1": [3.14, 42.0]})
    results = why.log(data, schema=schema).view()
    assert "col1.square_type" in results.get_columns().keys()
    summary = results.get_column("col1.square_type").to_summary_dict()
    assert summary["counts/n"] == 2
    assert summary["types/fractional"] == 2


@register_type_udf(float, schema_name="polars-unit-tests")
def square_python_type(x: pl.Series) -> pl.Series:
    return x * x


def test_python_type_udf() -> None:
    schema = udf_schema(schema_name="polars-unit-tests")
    data = pl.DataFrame({"col1": [3.14, 42.0]})
    results = why.log(data, schema=schema).view()
    assert "col1.square_python_type" in results.get_columns().keys()
    summary = results.get_column("col1.square_python_type").to_summary_dict()
    assert summary["counts/n"] == 2
    assert summary["types/fractional"] == 2
