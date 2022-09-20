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
        dict(
            a=np.random.choice([1, 2, 3], n),
            b=np.random.choice(["a", "b"], n),
            c=np.random.random(n),
            d=np.random.choice(["xy", "z"], n),
        )
    )


def test_no_partition(_test_df):
    for engine in [None, "spark"]:
        t1 = datetime(2020, 1, 1)
        t2 = datetime(2020, 1, 2)
        v = fugue_profile(_test_df, dataset_timestamp=t1, creation_timestamp=t2, engine=engine)
        assert isinstance(v, DatasetProfileView)
        pf = v.to_pandas()

        assert len(pf) == 4
        assert pf["counts/n"].tolist() == [100, 100, 100, 100]
        assert v.dataset_timestamp == t1
        assert v.creation_timestamp == t2

        pf = fugue_profile(_test_df, profile_cols=["b", "c"], engine=engine).to_pandas()
        assert len(pf) == 2
        assert pf["counts/n"].tolist() == [100, 100]
        assert set(pf.index) == set(["b", "c"])


def test_with_partition(_test_df):
    for engine in [None, "spark"]:
        df = fugue_profile(_test_df, partition={"by": ["a", "b"]}, profile_field="x", engine=engine)
        # get counts of each group
        df["ct"] = df.x.apply(lambda x: DatasetProfileView.deserialize(x).to_pandas()["counts/n"].iloc[0])
        # get counts of profiled cols of each group should always be 4
        df["pct"] = df.x.apply(lambda x: len(DatasetProfileView.deserialize(x).to_pandas()))
        df = df[["a", "b", "ct", "pct"]].set_index(["a", "b"])
        # compute group counts in a different way
        ct = _test_df.groupby(["a", "b"])["c"].count()
        df["ct_true"] = ct
        # assert counts match
        assert all(df.ct == df.ct_true)
        assert all(df.pct == 4)

        df = fugue_profile(
            _test_df, partition={"by": ["a", "b"]}, profile_cols=["b", "c"], profile_field="x", engine=engine
        )
        df["pct"] = df.x.apply(lambda x: len(DatasetProfileView.deserialize(x).to_pandas()))
        assert all(df.pct == 2)
