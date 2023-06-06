import os.path
import pickle
import time
from logging import getLogger

import pandas as pd

import whylogs as why
from whylogs.core import DatasetProfile, DatasetProfileView
from whylogs.core.model_performance_metrics.model_performance_metrics import (
    ModelPerformanceMetrics,
)
from whylogs.core.schema import DatasetSchema
from whylogs.core.segmentation_partition import segment_on_column
from whylogs.core.view.dataset_profile_view import _TAG_PREFIX

TEST_LOGGER = getLogger(__name__)


def _assert_profiles_are_equal(profile_a: DatasetProfileView, profile_b: DatasetProfileView) -> None:
    if profile_a is None and profile_b is None:
        return
    if profile_a is None or profile_b is None:
        assert profile_a == profile_b

    columns_in_a = profile_a.get_columns()
    columns_in_b = profile_b.get_columns()
    if not columns_in_b:
        assert columns_in_a == columns_in_b

    assert columns_in_a.keys() == columns_in_b.keys()

    for col_name in columns_in_a:
        assert col_name in columns_in_b
        assert (col_name, columns_in_a[col_name].to_protobuf()) == (col_name, columns_in_b[col_name].to_protobuf())

    assert profile_a.creation_timestamp == profile_b.creation_timestamp
    assert profile_a.dataset_timestamp == profile_b.dataset_timestamp


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


def test_rt_pick() -> None:
    d = {"col1": [1, 2], "col2": [3.0, 4.0], "col3": ["a", "b"]}
    df = pd.DataFrame(data=d)

    profile = DatasetProfile()
    profile.track(pandas=df)
    view = profile.view()
    rt: DatasetProfileView = pickle.loads(pickle.dumps(view))
    assert len(view.to_pandas()) == len(rt.to_pandas())


def test_view_serder_bytes() -> None:
    d = {"col1": [1, 2], "col2": [3.0, 4.0], "col3": ["a", "b"]}
    df = pd.DataFrame(data=d)

    profile = DatasetProfile()
    profile.track(pandas=df)
    view = profile.view()
    data = view.serialize()
    assert len(data) > 0
    rt = DatasetProfileView.deserialize(data)
    assert len(view.to_pandas()) == len(rt.to_pandas())


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


def test_sort_columns_sending_to_pandas() -> None:
    data = {
        "animal": ["cat", "hawk", "snake", "cat"],
        "legs": [4, 2, 0, 4],
        "weight": [4.3, 1.8, 1.3, 4.1],
    }

    df = pd.DataFrame(data)
    profile_view = why.log(df).profile().view()
    view1 = profile_view.to_pandas().columns

    merged_view = profile_view.merge(profile_view)
    view2 = merged_view.to_pandas().columns
    assert (view1 == view2).all()


def test_different_ordered() -> None:
    data1 = {
        "animal": ["cat", "hawk", "snake", "cat"],
        "legs": [4, 2, 0, 4],
        "weight": [4.3, 1.8, 1.3, 4.1],
    }
    data2 = {
        "legs": [4, 2, 0, 4],
        "animal": ["cat", "hawk", "snake", "cat"],
        "weight": [4.3, 1.8, 1.3, 4.1],
    }

    df1 = pd.DataFrame(data1)
    profile_view1 = why.log(df1).profile().view()
    view1 = profile_view1.to_pandas().columns

    df2 = pd.DataFrame(data2)
    profile_view2 = why.log(df2).profile().view()

    merged_view = profile_view1.merge(profile_view2)
    view2 = merged_view.to_pandas().columns
    assert (view1 == view2).all()


def test_to_pandas_empty() -> None:
    profile = DatasetProfile().view()
    pdf: pd.DataFrame = profile.to_pandas()
    assert pdf is not None
    assert pdf.empty


def test_default_datasetprofile_timestamps() -> None:
    stamp0 = time.time()
    view = DatasetProfile().view()
    stamp1 = time.time()
    assert view is not None
    assert view.creation_timestamp is not None
    assert view.dataset_timestamp is not None
    assert stamp0 <= view.creation_timestamp.timestamp() <= stamp1
    assert stamp0 <= view.dataset_timestamp.timestamp() <= stamp1


def test_merge_takes_oldest_timestamps() -> None:
    view1 = DatasetProfile().view()
    view2 = DatasetProfile().view()
    assert view1.dataset_timestamp < view2.dataset_timestamp
    assert view1.creation_timestamp < view2.creation_timestamp
    merged_view = view2.merge(view1)
    assert merged_view.creation_timestamp == view1.creation_timestamp
    assert merged_view.dataset_timestamp == view1.dataset_timestamp


def test_datasetprofile_zero_timestamps() -> None:
    view = DatasetProfileView.zero()
    stamp = time.time()
    assert view is not None
    assert view.creation_timestamp is None
    assert view.dataset_timestamp is None
    view.dataset_timestamp = stamp
    assert view.dataset_timestamp is not None
    assert view.dataset_timestamp == stamp


def test_datasetprofile_model_performance_metrics() -> None:
    view = DatasetProfileView.zero()
    assert view.model_performance_metrics is None
    perf_metric = ModelPerformanceMetrics()
    view.model_performance_metrics = perf_metric
    assert view.model_performance_metrics is perf_metric


def test_zero_and_merging() -> None:
    view_zero = DatasetProfileView.zero()
    data1 = {
        "animal": ["cat", "hawk", "snake", "cat"],
        "legs": [4, 2, 0, 4],
        "weight": [4.3, 1.8, 1.3, 4.1],
    }

    data2 = {
        "animal": ["dog", "horse"],
        "legs": [4, 4],
        "weight": [35.6, 670],
    }

    view1 = why.log(pd.DataFrame(data1)).view()
    view2 = why.log(pd.DataFrame(data2)).view()

    merged_zero_1 = view_zero.merge(view1)
    merged_zero_2 = view_zero.merge(view2)
    merged_zero_1_2 = merged_zero_1.merge(merged_zero_2)
    merged1_2 = view1.merge(view2)

    # manually check one of the metric component values
    max1 = merged_zero_1_2.get_column("weight").get_metric("distribution").max
    max2 = merged1_2.get_column("weight").get_metric("distribution").max
    assert max1 == max2

    _assert_profiles_are_equal(view1, merged_zero_1)
    _assert_profiles_are_equal(view2, merged_zero_2)
    _assert_profiles_are_equal(merged1_2, merged_zero_1_2)


def test_segmented_round_trip_metadata(tmp_path: str) -> None:
    df = pd.DataFrame(
        {
            "animal": ["cat", "hawk", "snake", "cat"],
            "legs": [4, 2, 0, 4],
            "weight": [4.3, 1.8, 1.3, 4.1],
            "A": [True, True, True, True],
        }
    )
    output_file = os.path.join(tmp_path, "segmented_view.bin")
    segment_column = "A"
    results = why.log(df, schema=DatasetSchema(segments=segment_on_column(segment_column)))
    status = results.writer("local").write(dest=output_file)
    TEST_LOGGER.info(f"serialized segmented profile to {output_file} has status: {status}")

    view = DatasetProfileView.read(output_file)
    TEST_LOGGER.info(f"round trip serialized and deserialized segmented profile has metadata: {view._metadata}")
    assert view._metadata is not None
    segment_tag_metadata_key = _TAG_PREFIX + segment_column
    assert segment_tag_metadata_key in view._metadata
    assert view._metadata[segment_tag_metadata_key] == "True"
    assert "segp_col" in view._metadata
    assert view._metadata["segp_col"] == segment_column
    assert "segp_name" in view._metadata
    assert view._metadata["segp_name"] == segment_column
    assert "segp_id" in view._metadata
