import pandas as pd

import whylogs as why
from whylogs.core.datatypes import String
from whylogs.core.resolvers import STANDARD_RESOLVER
from whylogs.experimental.core.udf_schema import (
    UdfSchema,
    UdfSpec,
    generate_udf_dataset_schema,
    register_dataset_udf,
)


def test_udf_row() -> None:
    schema = UdfSchema(
        STANDARD_RESOLVER,
        udf_specs=[UdfSpec(column_name="col1", udfs={"col2": lambda x: x, "col3": lambda x: x})],
    )
    data = {"col1": 42}
    results = why.log(row=data, schema=schema).view()
    col1 = results.get_column("col1").to_summary_dict()
    col2 = results.get_column("col1.col2").to_summary_dict()
    col3 = results.get_column("col1.col3").to_summary_dict()
    assert col1 == col2 == col3


def test_udf_pandas() -> None:
    schema = UdfSchema(
        STANDARD_RESOLVER,
        udf_specs=[UdfSpec(column_name="col1", udfs={"col2": lambda x: x, "col3": lambda x: x})],
    )
    data = pd.DataFrame({"col1": [42, 12, 7]})
    results = why.log(pandas=data, schema=schema).view()
    col1 = results.get_column("col1").to_summary_dict()
    col2 = results.get_column("col1.col2").to_summary_dict()
    col3 = results.get_column("col1.col3").to_summary_dict()
    assert col1 == col2 == col3


@register_dataset_udf("col1")
def add5(x):
    return x + 5


@register_dataset_udf(col_type=String, udf_name="upper case")
def upper(x):
    return x.upper()


def square(x):
    return x * x


def test_decorator_pandas() -> None:
    extra_spec = UdfSpec("col1", None, {"sqr": square})
    schema = generate_udf_dataset_schema([extra_spec], STANDARD_RESOLVER)
    data = pd.DataFrame({"col1": [42, 12, 7], "col2": ["a", "b", "c"]})
    results = why.log(pandas=data, schema=schema).view()
    col1_summary = results.get_column("col1").to_summary_dict()
    assert "distribution/n" in col1_summary
    add5_summary = results.get_column("col1.add5").to_summary_dict()
    assert "distribution/n" in add5_summary
    upper_summary = results.get_column("col2.upper case").to_summary_dict()
    assert "cardinality/est" in upper_summary
    sqr_summary = results.get_column("col1.sqr").to_summary_dict()
    assert "distribution/n" in sqr_summary


def test_decorator_row() -> None:
    extra_spec = UdfSpec("col1", None, {"sqr": square})
    schema = generate_udf_dataset_schema([extra_spec], STANDARD_RESOLVER)
    results = why.log(row={"col1": 42, "col2": "a"}, schema=schema).view()
    col1_summary = results.get_column("col1").to_summary_dict()
    assert "distribution/n" in col1_summary
    add5_summary = results.get_column("col1.add5").to_summary_dict()
    assert "distribution/n" in add5_summary
    upper_summary = results.get_column("col2.upper case").to_summary_dict()
    assert "cardinality/est" in upper_summary
    sqr_summary = results.get_column("col1.sqr").to_summary_dict()
    assert "distribution/n" in sqr_summary


@register_dataset_udf(["col1", "col2"], None, "product")
def times(x):
    return x["col1"] * x["col2"]


@register_dataset_udf(["col1", "col3"])
def ratio(x):
    return x["col1"] / x["col3"]


def test_multicolumn_udf_pandas() -> None:
    extra_spec = UdfSpec("col1", None, {"sqr": square})
    schema = generate_udf_dataset_schema([extra_spec], STANDARD_RESOLVER)
    data = pd.DataFrame({"col1": [42, 12, 7], "col2": [2, 3, 4], "col3": [2, 3, 4]})
    results = why.log(pandas=data, schema=schema).view()
    col1_summary = results.get_column("col1").to_summary_dict()
    assert "distribution/n" in col1_summary
    col2_summary = results.get_column("col2").to_summary_dict()
    assert "distribution/n" in col2_summary
    col3_summary = results.get_column("col3").to_summary_dict()
    assert "distribution/n" in col3_summary
    add5_summary = results.get_column("col1.add5").to_summary_dict()
    assert "distribution/n" in add5_summary
    prod_summary = results.get_column("product").to_summary_dict()
    assert prod_summary["distribution/n"] == 3
    sqr_summary = results.get_column("col1.sqr").to_summary_dict()
    assert "distribution/n" in sqr_summary
    div_summary = results.get_column("ratio").to_summary_dict()
    assert div_summary["distribution/n"] == 3


def test_multicolumn_udf_row() -> None:
    extra_spec = UdfSpec("col1", None, {"sqr": square})
    schema = generate_udf_dataset_schema([extra_spec], STANDARD_RESOLVER)
    data = {"col1": 42, "col2": 2, "col3": 2}
    results = why.log(row=data, schema=schema).view()
    col1_summary = results.get_column("col1").to_summary_dict()
    assert "distribution/n" in col1_summary
    col2_summary = results.get_column("col2").to_summary_dict()
    assert "distribution/n" in col2_summary
    col3_summary = results.get_column("col3").to_summary_dict()
    assert "distribution/n" in col3_summary
    add5_summary = results.get_column("col1.add5").to_summary_dict()
    assert "distribution/n" in add5_summary
    prod_summary = results.get_column("product").to_summary_dict()
    assert prod_summary["distribution/n"] == 1
    sqr_summary = results.get_column("col1.sqr").to_summary_dict()
    assert "distribution/n" in sqr_summary
    div_summary = results.get_column("ratio").to_summary_dict()
    assert div_summary["distribution/n"] == 1
