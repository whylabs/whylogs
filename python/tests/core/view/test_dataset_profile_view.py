import os.path

import pandas as pd

import whylogs as why
from whylogs.core import DatasetProfile, DatasetProfileView


def test_view_serde_roundtrip(tmp_path: str) -> None:
    d = {"col1": [1, 2], "col2": [3.0, 4.0], "col3": ["a", "b"]}
    df = pd.DataFrame(data=d)

    profile = DatasetProfile()
    profile.track(pandas=df)
    view = profile.view()
    output_file = os.path.join(tmp_path, "view.bin")
    view.write(output_file)
    res = DatasetProfileView.read(output_file)

    assert len(view.to_pandas()) == len(res.to_pandas())


def test_merge_nan_column(lending_club_df) -> None:
    series_1 = lending_club_df.head(500)
    series_2 = lending_club_df.tail(500)
    prof_view_1 = why.log(series_1).view()
    prof_view_2 = why.log(series_2).view()
    merged_profile_view = prof_view_1.merge(prof_view_2)

    mean = merged_profile_view.get_column("member_id").get_metric("distribution").mean.value
    stddev = merged_profile_view.get_column("member_id").get_metric("distribution").stddev
    null_count = merged_profile_view.get_column("member_id").get_metric("counts").null.value
    n_count = merged_profile_view.get_column("member_id").get_metric("counts").n.value
    assert mean == 0
    assert stddev == 0
    assert null_count == n_count


def test_order_columns() -> None:
    d = {"c0": [0], "c1": [1], "c2": [2], "c3": [3], "c4": [4], "c5": [5], "c6": [6], "c7": [7], "c8": [8], "c9": [9]}
    df = pd.DataFrame(d)
    profile_view = why.log(df).profile().view()
    data_keys = d.keys()
    profile_keys = profile_view.get_columns().keys()
    assert data_keys == profile_keys
