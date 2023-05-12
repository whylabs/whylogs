import pandas as pd

import whylogs as why
from whylogs.core.datatypes import String
from whylogs.core.resolvers import STANDARD_RESOLVER
from whylogs.experimental.core.udf_schema import (
    UdfSchema,
    UdfSpec,
    generate_udf_specs,
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
    schema = UdfSchema(STANDARD_RESOLVER, udf_specs=generate_udf_specs([extra_spec]))
    data = pd.DataFrame({"col1": [42, 12, 7], "col2": ["a", "b", "c"]})
    results = why.log(pandas=data, schema=schema).view()
    col1_summary = results.get_column("col1").to_summary_dict()
    assert "distribution/n" in col1_summary
    col3_summary = results.get_column("col1.add5").to_summary_dict()
    assert "distribution/n" in col3_summary
    col4_summary = results.get_column("col2.upper case").to_summary_dict()
    assert "cardinality/est" in col4_summary
    col5_summary = results.get_column("col1.sqr").to_summary_dict()
    assert "distribution/n" in col5_summary


def test_decorator_row() -> None:
    extra_spec = UdfSpec("col1", None, {"sqr": square})
    schema = UdfSchema(STANDARD_RESOLVER, udf_specs=generate_udf_specs([extra_spec]))
    results = why.log(row={"col1": 42, "col2": "a"}, schema=schema).view()
    col1_summary = results.get_column("col1").to_summary_dict()
    assert "distribution/n" in col1_summary
    col3_summary = results.get_column("col1.add5").to_summary_dict()
    assert "distribution/n" in col3_summary
    col4_summary = results.get_column("col2.upper case").to_summary_dict()
    assert "cardinality/est" in col4_summary
    col5_summary = results.get_column("col1.sqr").to_summary_dict()
    assert "distribution/n" in col5_summary
