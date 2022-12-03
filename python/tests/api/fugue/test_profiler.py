import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pytest

from whylogs.api.fugue import fugue_profile
from whylogs.core import DatasetSchema, Resolver
from whylogs.core.metrics import StandardMetric
from whylogs.core.view.dataset_profile_view import DatasetProfileView


@pytest.fixture
def _test_df():
    n = 100
    np.random.seed(0)
    return pd.DataFrame(
        {
            "1": np.random.choice([1, 2, 3], n),
            "a b": np.random.choice(["a", "b"], n),
            "_1": np.random.random(n),
            "d e": np.random.choice(["xy", "z"], n),
        }
    )


@pytest.fixture()
def _test_df2():
    return pd.DataFrame(
        [
            [0.0, 1.0, 2.0, 3.0],
            [0.1, 1.1, 2.1, 3.1],
            [0.2, 1.3, 2.3, 3.3],
        ],
        columns=["0", "1", "2", "3"],
    )


def _is_testable_version():
    # TODO: Due to https://github.com/fugue-project/triad/issues/91
    # this test doesn't work on very rare cases, will re-enable when
    # the issue is resolved
    return sys.version_info < (3, 10)


@pytest.mark.skipif(not _is_testable_version(), reason="not a testable python version")
def test_no_partition(_test_df):
    for engine in [None, "spark"]:
        t1 = datetime(2020, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2020, 1, 2, tzinfo=timezone.utc)
        v = fugue_profile(
            _test_df,
            dataset_timestamp=t1,
            creation_timestamp=t2,
            engine=engine,
            engine_conf={"fugue.spark.use_pandas_udf": True},
        )
        assert isinstance(v, DatasetProfileView)
        pf = v.to_pandas()

        assert len(pf) == 4
        assert pf["counts/n"].tolist() == [100, 100, 100, 100]
        assert v.dataset_timestamp == t1
        assert v.creation_timestamp == t2

        profile_cols = [_test_df.columns[1], _test_df.columns[2]]
        pf = fugue_profile(_test_df, profile_cols=profile_cols, engine=engine).to_pandas()
        assert len(pf) == 2
        assert pf["counts/n"].tolist() == [100, 100]
        assert set(pf.index) == set(profile_cols)


@pytest.mark.skipif(not _is_testable_version(), reason="not a testable python version")
def test_with_partition(_test_df):
    for engine in [None, "spark"]:
        part_keys = [_test_df.columns[0], _test_df.columns[1]]
        profile_cols = [_test_df.columns[2], _test_df.columns[3]]
        df = fugue_profile(
            _test_df,
            partition={"by": part_keys},
            profile_field="x",
            engine=engine,
            engine_conf={"fugue.spark.use_pandas_udf": True},
        )
        # get counts of each group
        df["ct"] = df.x.apply(lambda x: DatasetProfileView.deserialize(x).to_pandas()["counts/n"].iloc[0])
        # get counts of profiled cols of each group should always be 4
        df["pct"] = df.x.apply(lambda x: len(DatasetProfileView.deserialize(x).to_pandas()))
        df = df[part_keys + ["ct", "pct"]].set_index(part_keys)
        # compute group counts in a different way
        ct = _test_df.groupby(part_keys)[_test_df.columns[2]].count()
        df["ct_true"] = ct
        # assert counts match
        assert all(df.ct == df.ct_true)
        assert all(df.pct == 4)

        df = fugue_profile(
            _test_df, partition={"by": part_keys}, profile_cols=profile_cols, profile_field="x", engine=engine
        )
        df["pct"] = df.x.apply(lambda x: len(DatasetProfileView.deserialize(x).to_pandas()))
        assert all(df.pct == 2)


def test_collect_dataset_profile_view_with_schema(_test_df2):
    class TestResolver(Resolver):
        def resolve(self, name, why_type, column_schema):
            metric_map = {"0": [StandardMetric.counts], "1": [], "2": [], "3": []}
            return {metric.name: metric.zero(column_schema.cfg) for metric in metric_map[name]}

    schema = DatasetSchema(resolvers=TestResolver())
    profile_view = fugue_profile(_test_df2, schema=schema)

    assert isinstance(profile_view, DatasetProfileView)
    assert len(profile_view.get_columns()) > 0
    assert profile_view.get_column("0").get_metric_names() == ["counts"]
    assert profile_view.get_column("0").get_metric("counts").n.value == 3
    assert profile_view.get_column("1").get_metric_names() == []
    assert profile_view.get_column("2").get_metric_names() == []
    assert profile_view.get_column("3").get_metric_names() == []
