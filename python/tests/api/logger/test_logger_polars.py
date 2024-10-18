import os
import tempfile
from typing import Any

import numpy as np
import pandas as pd
import polars as pl
import pytest

import whylogs as why
from whylogs.api.logger import write
from whylogs.api.logger.result_set import ResultSet, ResultSetReader
from whylogs.core import ColumnProfileView, MetricConfig
from whylogs.core.errors import LoggingError
from whylogs.core.metrics import StandardMetric
from whylogs.core.resolvers import Resolver
from whylogs.core.schema import DatasetSchema

'''
FLOAT_TYPES = [float, np.float16, np.float32, np.float64, np.floating, np.float_, np.longdouble]
INTEGER_TYPES = [int, np.intc, np.uintc, np.int_, np.uint, np.longlong, np.ulonglong]
DATETIME_TYPES = [np.datetime64, pd.Timestamp]
TIMEDELTA_TYPES = ["timedelta64[s]", "timedelta64[ms]"]
'''

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)


def test_basic_log_schema() -> None:
    d = {"col1": [1, 2]}
    df = pl.DataFrame(data=d)
    logger = why.logger()
    results = logger.log(df, schema=DatasetSchema())
    profile = results.profile()
    assert profile._columns["col1"]._schema.dtype == pl.Int64

def test_basic_log_schem_constructor() -> None:
    d = {"col1": [1, 2]}
    df = pl.DataFrame(data=d)
    logger = why.logger(schema=DatasetSchema())
    results = logger.log(df)
    profile = results.profile()
    assert profile._columns["col1"]._schema.dtype == pl.Int64


def test_basic_log() -> None:
    d = {"col1": [1, 2], "col2": [3.0, 4.0], "col3": ["a", "b"]}
    df = pl.DataFrame(data=d)

    results = why.log(df)

    profile = results.profile()

    assert profile._columns["col1"]._schema.dtype == pl.Int64
    assert profile._columns["col2"]._schema.dtype == pl.Float64
    assert profile._columns["col3"]._schema.dtype == pl.Utf8


def test_log_nothing_raises_error() -> None:
    with pytest.raises(LoggingError):
        why.log()


def test_basic_log_row() -> None:
    d = {"col1": [1, 2], "col2": [3.0, 4.0], "col3": ["a", "b"]}

    results = why.log(row=d)

    profile = results.profile()

    assert profile._columns["col1"]._schema.dtype == list
    assert profile._columns["col2"]._schema.dtype == list
    assert profile._columns["col3"]._schema.dtype == list


def test_basic_log_dict_of_lists() -> None:
    d = {"col1": [np.int64(1), np.int64(2)], "col2": [3.0, 4.0], "col3": ["a", "b"]}

    results = why.log(d)

    profile = results.profile()

    assert profile._columns["col1"]._schema.dtype == list
    assert profile._columns["col2"]._schema.dtype == list
    assert profile._columns["col3"]._schema.dtype == list


def test_basic_log_dictionary() -> None:
    d = {"a": 1.0, "b": 2.0}

    results = why.log(d)

    profile = results.profile()

    assert profile._columns["a"]._schema.dtype == float
    assert profile._columns["b"]._schema.dtype == float


def test_lending_club(lending_club_df: pl.DataFrame) -> None:
    res = why.log(lending_club_df)
    view = res.view()
    df = view.to_pandas()
    assert len(df) == 151


@pytest.mark.skip("type not supported yet")
def test_categorical_dtype() -> None:
    data = {"can_fly": [0, 1, 0, 0], "habitat": ["forest", "forest", "river", "river"]}

    df = pl.DataFrame(data)
    df["can_fly"] = df["can_fly"].astype("category")
    df["habitat"] = df["habitat"].astype("category")

    results = why.log(df)
    view = results.view()
    metrics = view.get_column("can_fly").get_metric("counts").to_summary_dict()
    assert metrics["n"] == 4


def test_roundtrip_resultset(tmp_path: Any) -> None:
    d = {"col1": [1, 2], "col2": [3.0, 4.0], "col3": ["a", "b"]}
    df = pl.DataFrame(data=d)

    results = why.log(df)
    status, path = results.writer("local", base_name="profile.bin").option(base_dir=tmp_path).write()
    assert status
    roundtrip_result_set = why.read(path)
    assert len(results.view().to_pandas()) == len(roundtrip_result_set.view().to_pandas())


def test_profile_write(tmp_path: Any) -> None:
    d = {"col1": [1, 2], "col2": [3.0, 4.0], "col3": ["a", "b"]}
    df = pl.DataFrame(data=d)
    results = why.log(df)
    profile = results.profile()
    write(profile, tmp_path, "test1_profile.bin")
    assert os.path.isfile(os.path.join(tmp_path, "test1_profile.bin"))
    path = os.path.join(tmp_path, "test2_profile.bin")
    write(profile, path)
    assert os.path.isfile(path)


'''
@pytest.mark.parametrize("data_type", [*INTEGER_TYPES, *FLOAT_TYPES, *TIMEDELTA_TYPES])
def test_different_integer_types(data_type) -> None:
    d = {"col1": [1, 3, 2, 5]}
    df = pl.DataFrame(d, dtype=data_type)
    results = why.log(df)
    view = results.view()

    assert isinstance(view._columns["col1"], ColumnProfileView)
    assert view._columns.get("col1")._failure_count == 0
    assert view._columns.get("col1")._success_count > 0

    view_pandas = view.to_pandas()
    assert len(view_pandas) == 1
    assert len(view_pandas.columns) > 0
'''


def test_counters_dataframe_vs_row() -> None:
    d = {"a": 1, "b": 2.0, "c": ["foo", "bar"]}
    df = pl.DataFrame(d)

    df_results = why.log(df)
    row_results = why.log(d)

    df_view = df_results.view()
    row_view = row_results.view()

    view_pandas = df_view.to_pandas()
    assert len(view_pandas) == 3
    assert len(view_pandas.columns) > 0

    view_row_pandas = row_view.to_pandas()
    assert len(view_row_pandas) == 3
    assert len(view_row_pandas.columns) > 0


@pytest.mark.parametrize(
    "input",
    [{"a": ["x", "y"]}, {"a": []}],  # non-numeric list -> object  # tensors require positive shape in every dimension
)
def test_object_count_dict(input) -> None:
    row_results = why.log(input)
    row_view = row_results.view()
    assert row_view._columns.get("a")._success_count == 2
    assert row_view._columns.get("a")._metrics.get("types").object.value == 1


@pytest.mark.parametrize(
    "input,stub_np,ints,reals,bools,strs,tensors,objs",
    [
        ({"a": 1}, False, 1, 0, 0, 0, 0, 0),
        ({"a": 1.0}, False, 0, 1, 0, 0, 0, 0),
        ({"a": True}, False, 0, 0, 1, 0, 0, 0),
        ({"a": "foo"}, False, 0, 0, 0, 1, 0, 0),
        ({"a": [1, 2]}, False, 0, 0, 0, 0, 1, 0),
        ({"a": [[1, 2], [3, 4]]}, False, 0, 0, 0, 0, 1, 0),
        ({"a": [[1, 2.5], [3.14, 4]]}, False, 0, 0, 0, 0, 1, 0),
        ({"a": [[1, 2], ["x", "y"]]}, False, 0, 0, 0, 0, 0, 1),
        ({"a": np.asarray([1, 2])}, False, 0, 0, 0, 0, 1, 0),
        ({"a": np.asarray([[1, 2], [3, 4]])}, False, 0, 0, 0, 0, 1, 0),
        ({"a": np.asarray([[1, 2.5], [3.14, 4]])}, False, 0, 0, 0, 0, 1, 0),
        ({"a": np.asarray([[1, 2], ["x", "y"]])}, False, 0, 0, 0, 0, 0, 1),
        ({"a": []}, False, 0, 0, 0, 0, 0, 1),
        ({"a": 1}, True, 1, 0, 0, 0, 0, 0),
        ({"a": 1.0}, True, 0, 1, 0, 0, 0, 0),
        ({"a": True}, True, 0, 0, 1, 0, 0, 0),
        ({"a": "foo"}, True, 0, 0, 0, 1, 0, 0),
        ({"a": [1, 2]}, True, 0, 0, 0, 0, 0, 1),
        ({"a": [[1, 2], [3, 4]]}, True, 0, 0, 0, 0, 0, 1),
        ({"a": [[1, 2.5], [3.14, 4]]}, True, 0, 0, 0, 0, 0, 1),
        ({"a": [[1, 2], ["x", "y"]]}, True, 0, 0, 0, 0, 0, 1),
        ({"a": np.asarray([1, 2])}, True, 0, 0, 0, 0, 0, 1),
        ({"a": np.asarray([[1, 2], [3, 4]])}, True, 0, 0, 0, 0, 0, 1),
        ({"a": np.asarray([[1, 2.5], [3.14, 4]])}, True, 0, 0, 0, 0, 0, 1),
        ({"a": np.asarray([[1, 2], ["x", "y"]])}, True, 0, 0, 0, 0, 0, 1),
        ({"a": []}, True, 0, 0, 0, 0, 0, 1),
    ],
)
def test_type_count_dict(input, stub_np, ints, reals, bools, strs, tensors, objs, monkeypatch) -> None:
    monkeypatch.setattr("whylogs.core.preprocessing.is_not_stub", lambda x: (not stub_np))
    row_results = why.log(input)
    row_view = row_results.view()
    assert row_view._columns.get("a")._metrics.get("types").integral.value == ints
    assert row_view._columns.get("a")._metrics.get("types").fractional.value == reals
    assert row_view._columns.get("a")._metrics.get("types").boolean.value == bools
    assert row_view._columns.get("a")._metrics.get("types").string.value == strs
    assert row_view._columns.get("a")._metrics.get("types").tensor.value == tensors
    assert row_view._columns.get("a")._metrics.get("types").object.value == objs


def test_bool_count():
    data = {
        "animal": ["cat", "hawk", "snake", "cat"],
        "fly": [False, True, False, False],
        "legs": [4, 2, 0, 4],
    }

    df = pl.DataFrame(data)

    results = why.log(polars=df)
    prof_view = results.profile().view()
    assert prof_view._columns.get("fly")._metrics.get("types").boolean.value == 4
    assert prof_view._columns.get("fly")._metrics.get("types").integral.value == 0


def test_unicode_range_enabled() -> None:
    strings = {
        "words": ["1", "12", "123", "1234a", "abc", "abc123", "I😍emoticons"],
    }  # TODO: follow and create ranges for common emoji like ❤️  /u+fe0f
    data = pl.DataFrame(strings)
    digit_counts = [1, 2, 3, 4, 0, 3, 0]
    latin_counts = [1, 2, 3, 5, 3, 6, 10]
    emoticon_counts = [0, 0, 0, 0, 0, 0, 1]
    configured_schema = DatasetSchema(default_configs=MetricConfig(track_unicode_ranges=True))
    prof_view = why.log(data, schema=configured_schema).view()
    assert "words" in prof_view.get_columns()
    column_profile = prof_view.get_column("words")
    assert "unicode_range" in column_profile.get_metric_names()
    metric = column_profile.get_metric("unicode_range")

    assert "digits" in metric.submetrics
    assert "basic-latin" in metric.submetrics
    assert "emoticon" in metric.submetrics

    assert metric.submetrics["digits"]["distribution"].mean.value == np.array(digit_counts).mean()
    assert metric.submetrics["emoticon"]["distribution"].mean.value == np.array(emoticon_counts).mean()
    assert metric.submetrics["basic-latin"]["distribution"].mean.value == np.array(latin_counts).mean()


def test_unicode_range_default_config_off() -> None:
    strings = {
        "words": ["1", "12", "123", "1234a", "abc", "abc123", "I😍emoticon"],
    }
    data = pl.DataFrame(strings)

    prof_view = why.log(data).view()
    assert "words" in prof_view.get_columns()
    column_profile = prof_view.get_column("words")
    assert "unicode_range" not in column_profile.get_metric_names()


def test_frequent_items() -> None:
    strings = {
        "words": ["1", "12", "123"],
    }
    data = pl.DataFrame(strings)

    prof_view = why.log(data).view()
    assert "words" in prof_view.get_columns()
    column_profile = prof_view.get_column("words")
    assert "frequent_items" in column_profile.get_metric_names()


def test_frequent_items_disabled() -> None:
    strings = {
        "words": ["1", "12", "123"],
    }
    data = pl.DataFrame(strings)
    configured_schema = DatasetSchema(default_configs=MetricConfig(fi_disabled=True))

    prof_view = why.log(data, schema=configured_schema).view()
    assert "words" in prof_view.get_columns()
    column_profile = prof_view.get_column("words")
    assert "frequent_items" not in column_profile.get_metric_names()


def test_custom_resolver() -> None:
    class CustomResolver(Resolver):
        """Resolver that keeps distribution metrics for Fractional and frequent items for Integral, and counters and types metrics for all data types."""

        def resolve(self, name: str, why_type, column_schema):
            metrics = []
            if name == "col1":
                metrics.append(StandardMetric.counts)
            result = {}
            for m in metrics:
                result[m.name] = m.zero(column_schema.cfg)
            return result

    d = {"col1": [3.0, 4.0, 5.0]}
    df = pl.DataFrame(data=d)
    prof_view = why.log(df, schema=DatasetSchema(resolvers=CustomResolver())).profile().view()

    assert prof_view.get_column("col1").get_metric("counts").n.value == 3
    assert not prof_view.get_column("col1").get_metric("distribution")


def test_result_set_reader(profile_view):
    with tempfile.NamedTemporaryFile() as tmp_file:
        success, path = profile_view.write(file=tmp_file)
        assert success
        tmp_file.flush()
        tmp_file.seek(0)
        reader = why.reader(name="local")
        results = reader.read(path=path)
        assert isinstance(reader, ResultSetReader)
        assert isinstance(results, ResultSet)
