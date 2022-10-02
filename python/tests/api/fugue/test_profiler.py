import sys
from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from whylogs.api.fugue import fugue_profile
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


def _is_testable_version():
    # TODO: Due to https://github.com/fugue-project/triad/issues/91
    # this test doesn't work on very rare cases, will re-enable when
    # the issue is resolved
    return sys.version_info < (3, 10)


@pytest.mark.skipif(not _is_testable_version(), reason="not a testable python version")
def test_no_partition(_test_df):
    for engine in [None, "spark"]:
        t1 = datetime(2020, 1, 1)
        t2 = datetime(2020, 1, 2)
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
