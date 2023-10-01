import os
import pickle
import tempfile
from glob import glob
from logging import getLogger
from typing import Any

import numpy as np
import pandas as pd
import pytest

import whylogs as why
from whylogs.api.logger.result_set import (
    ProfileResultSet,
    SegmentedResultSet,
    ViewResultSet,
)
from whylogs.core.metrics.metrics import CardinalityMetric, DistributionMetric
from whylogs.core.schema import DatasetSchema
from whylogs.core.segment import Segment
from whylogs.core.segmentation_partition import (
    ColumnMapperFunction,
    SegmentationPartition,
    SegmentFilter,
    segment_on_column,
)
from whylogs.core.view.dataset_profile_view import DatasetProfileView
from whylogs.migration.converters import read_v0_to_view

TEST_LOGGER = getLogger(__name__)


def test_single_row_segment() -> None:
    segment_column = "col3"
    number_of_segments = 1

    test_segments = segment_on_column("col3")
    results: SegmentedResultSet = why.log(
        {"col1": 1, "col2": 1.1, "col3": "x0"}, schema=DatasetSchema(segments=test_segments)
    )
    assert results.count == number_of_segments
    partitions = results.partitions
    assert len(partitions) == 1
    partition = partitions[0]
    segments = results.segments_in_partition(partition)
    assert len(segments) == number_of_segments

    first_segment = next(iter(segments))
    assert first_segment.key == ("x0",)
    first_segment_profile = results.profile(first_segment)
    assert first_segment_profile is not None
    assert first_segment_profile._columns["col1"]._schema.dtype == int
    assert first_segment_profile._columns["col2"]._schema.dtype == float
    assert first_segment_profile._columns["col3"]._schema.dtype == str
    segment_cardinality: CardinalityMetric = (
        first_segment_profile.view().get_column(segment_column).get_metric("cardinality")
    )
    cardinality = segment_cardinality.estimate
    assert cardinality is not None
    assert cardinality == 1.0


def test_single_column_segment() -> None:
    input_rows = 100
    segment_column = "col3"
    number_of_segments = 5
    d = {
        "col1": [i for i in range(input_rows)],
        "col2": [i * i * 1.1 for i in range(input_rows)],
        segment_column: [f"x{str(i%number_of_segments)}" for i in range(input_rows)],
    }

    df = pd.DataFrame(data=d)
    test_segments = segment_on_column("col3")
    results: SegmentedResultSet = why.log(df, schema=DatasetSchema(segments=test_segments))
    assert results.count == number_of_segments
    partitions = results.partitions
    assert len(partitions) == 1
    partition = partitions[0]
    segments = results.segments_in_partition(partition)
    assert len(segments) == number_of_segments

    first_segment = next(iter(segments))
    first_segment_profile = results.profile(first_segment)
    assert first_segment_profile is not None
    assert first_segment_profile._columns["col1"]._schema.dtype == np.int64
    assert first_segment_profile._columns["col2"]._schema.dtype == np.float64
    assert first_segment_profile._columns["col3"]._schema.dtype.name == "object"
    segment_cardinality: CardinalityMetric = (
        first_segment_profile.view().get_column(segment_column).get_metric("cardinality")
    )
    cardinality = segment_cardinality.estimate
    assert cardinality is not None
    assert cardinality == 1.0


def test_single_column_segment_with_trace_id() -> None:
    input_rows = 100
    segment_column = "col3"
    number_of_segments = 5
    trace_id = "123-456"
    d = {
        "col1": [i for i in range(input_rows)],
        "col2": [i * i * 1.1 for i in range(input_rows)],
        segment_column: [f"x{str(i%number_of_segments)}" for i in range(input_rows)],
    }

    df = pd.DataFrame(data=d)
    test_segments = segment_on_column("col3")
    results: SegmentedResultSet = why.log(df, schema=DatasetSchema(segments=test_segments), trace_id=trace_id)
    assert results.count == number_of_segments
    partitions = results.partitions
    assert len(partitions) == 1
    partition = partitions[0]
    segments = results.segments_in_partition(partition)
    assert len(segments) == number_of_segments

    first_segment = next(iter(segments))
    first_segment_profile = results.profile(first_segment)
    assert first_segment_profile is not None
    assert first_segment_profile._columns["col1"]._schema.dtype == np.int64
    assert first_segment_profile._columns["col2"]._schema.dtype == np.float64
    assert first_segment_profile._columns["col3"]._schema.dtype.name == "object"
    segment_cardinality: CardinalityMetric = (
        first_segment_profile.view().get_column(segment_column).get_metric("cardinality")
    )
    cardinality = segment_cardinality.estimate
    assert cardinality is not None
    # cardinality is an estimate, and because this is the segment column, it should
    #  by definition contain only one unique value per segment.
    assert cardinality == 1.0
    assert results.metadata is not None
    assert results.metadata["whylabs.traceId"] == trace_id


def test_single_integer_column_segment() -> None:
    input_rows = 100
    segment_column = "col3"
    number_of_segments = 5
    d = {
        "col1": [i for i in range(input_rows)],
        "col2": [i * i * 1.1 for i in range(input_rows)],
        segment_column: [(i % number_of_segments) for i in range(input_rows)],
    }

    df = pd.DataFrame(data=d)
    test_segments = segment_on_column("col3")
    results: SegmentedResultSet = why.log(df, schema=DatasetSchema(segments=test_segments))
    assert results.count == number_of_segments
    partitions = results.partitions
    assert len(partitions) == 1
    partition = partitions[0]
    segments = results.segments_in_partition(partition)
    assert len(segments) == number_of_segments

    first_segment = next(iter(segments))
    first_segment_profile = results.profile(first_segment)
    assert first_segment_profile is not None
    assert first_segment_profile._columns["col1"]._schema.dtype == np.int64
    assert first_segment_profile._columns["col2"]._schema.dtype == np.float64
    assert first_segment_profile._columns["col3"]._schema.dtype == np.int64
    segment_cardinality: CardinalityMetric = (
        first_segment_profile.view().get_column(segment_column).get_metric("cardinality")
    )
    cardinality = segment_cardinality.estimate
    assert cardinality is not None
    assert cardinality == 1.0


def test_filtered_single_column_segment() -> None:
    input_rows = 100
    segment_column = "col3"
    number_of_segments = 5
    d = {
        "col1": [i for i in range(input_rows)],
        "col2": [i * i * 1.1 for i in range(input_rows)],
        segment_column: [f"x{str(i%number_of_segments)}" for i in range(input_rows)],
    }

    df = pd.DataFrame(data=d)
    test_segments = segment_on_column(segment_column)
    test_segments[segment_column].filter = SegmentFilter(filter_function=lambda df: df.col1 > 49)

    results: SegmentedResultSet = why.log(df, schema=DatasetSchema(segments=test_segments))
    assert results.count == number_of_segments
    partitions = results.partitions
    assert len(partitions) == 1
    partition = partitions[0]
    segments = results.segments_in_partition(partition)
    assert len(segments) == number_of_segments

    first_segment: Segment = next(iter(segments))
    first_segment_profile = results.profile(first_segment)
    assert first_segment.key == ("x0",)
    assert first_segment_profile is not None
    assert first_segment_profile._columns["col1"]._schema.dtype == np.int64
    assert first_segment_profile._columns["col2"]._schema.dtype == np.float64
    assert first_segment_profile._columns[segment_column]._schema.dtype.name == "object"
    segment_distribution: DistributionMetric = (
        first_segment_profile.view().get_column("col1").get_metric("distribution")
    )
    assert segment_distribution is not None
    count = segment_distribution.n
    assert count is not None
    assert count == 10


@pytest.mark.parametrize("v0", [True, False])
def test_segment_write_roundtrip_versions(tmp_path: Any, v0) -> None:
    input_rows = 10
    segment_column = "col3"
    number_of_segments = 2
    trace_id = "123-456"
    values_per_segment = input_rows / number_of_segments
    d = {
        "col1": [i for i in range(input_rows)],
        "col2": [i * i * 1.1 for i in range(input_rows)],
        segment_column: [f"x{str(i%number_of_segments)}" for i in range(input_rows)],
    }

    df = pd.DataFrame(data=d)
    test_segments = segment_on_column(segment_column)

    results: SegmentedResultSet = why.log(df, trace_id=trace_id, schema=DatasetSchema(segments=test_segments))
    assert results.count == number_of_segments
    partitions = results.partitions
    assert len(partitions) == 1
    partition = partitions[0]
    segments = results.segments_in_partition(partition)
    assert len(segments) == number_of_segments

    first_segment: Segment = next(iter(segments))
    first_segment_profile = results.profile(first_segment)
    assert first_segment.key == ("x0",)
    assert first_segment_profile is not None
    assert first_segment_profile._columns["col1"]._schema.dtype == np.int64
    assert first_segment_profile._columns["col2"]._schema.dtype == np.float64
    assert first_segment_profile._columns[segment_column]._schema.dtype.name == "object"
    segment_distribution: DistributionMetric = (
        first_segment_profile.view().get_column("col1").get_metric("distribution")
    )
    count = segment_distribution.n
    assert count is not None
    assert count == values_per_segment

    results.writer().option(base_dir=tmp_path).write(use_v0=v0)
    paths = glob(os.path.join(tmp_path) + "/*x0.bin")
    assert len(paths) == 1
    roundtrip_profiles = []
    for file_path in paths:
        if v0:
            roundtrip_profiles.append(read_v0_to_view(path=file_path))
        else:
            roundtrip_profiles.append(why.read(path=file_path).view())
    assert len(roundtrip_profiles) == 1
    post_deserialization_first_view = roundtrip_profiles[0]
    assert post_deserialization_first_view is not None
    assert isinstance(post_deserialization_first_view, DatasetProfileView)

    # check that trace_id is preserved round trip in metadata
    assert post_deserialization_first_view.metadata
    assert "whylabs.traceId" in post_deserialization_first_view.metadata
    assert trace_id == post_deserialization_first_view.metadata["whylabs.traceId"]
    pre_serialization_first_view = first_segment_profile.view()
    pre_columns = pre_serialization_first_view.get_columns()
    post_columns = post_deserialization_first_view.get_columns()

    # check that the distribution looks similar for each column profile
    for column_name in pre_columns:
        initial_column_profile = pre_columns[column_name]
        target_column_profile = post_columns[column_name]
        assert initial_column_profile is not None
        assert target_column_profile is not None
        assert target_column_profile.get_metric("distribution").n == initial_column_profile.get_metric("distribution").n
        assert (
            target_column_profile.get_metric("distribution").avg
            == initial_column_profile.get_metric("distribution").avg
        )


def test_multi_column_segment() -> None:
    input_rows = 100
    d = {
        "col1": [i for i in range(input_rows)],
        "col2": [i * i * 1.1 for i in range(input_rows)],
        "col3": [f"x{str(i%5)}" for i in range(input_rows)],
    }

    df = pd.DataFrame(data=d)
    segmentation_partition = SegmentationPartition(
        name="col1,col3", mapper=ColumnMapperFunction(col_names=["col1", "col3"])
    )
    test_segments = {segmentation_partition.name: segmentation_partition}
    results: SegmentedResultSet = why.log(df, schema=DatasetSchema(segments=test_segments))
    segments = results.segments()
    last_segment = segments[-1]

    # Note this segment is not useful as there is only one datapoint per segment, we have 100 rows and
    # 100 segments. The segment value is a tuple of strings identifying this segment.
    assert last_segment.key == ("99", "x4")

    last_segment_profile = results.profile(last_segment)

    assert last_segment_profile._columns["col1"]._schema.dtype == np.int64
    assert last_segment_profile._columns["col2"]._schema.dtype == np.float64
    assert last_segment_profile._columns["col3"]._schema.dtype.name == "object"

    segment_distribution: DistributionMetric = last_segment_profile.view().get_column("col1").get_metric("distribution")
    count = segment_distribution.n
    assert count is not None
    assert count == 1


def test_multi_column_segment_serialization_roundtrip_v0(tmp_path: Any) -> None:
    input_rows = 35
    d = {
        "A": [i % 7 for i in range(input_rows)],
        "B": [f"x{str(i%5)}" for i in range(input_rows)],
    }

    df = pd.DataFrame(data=d)
    segmentation_partition = SegmentationPartition(name="A,B", mapper=ColumnMapperFunction(col_names=["A", "B"]))
    test_segments = {segmentation_partition.name: segmentation_partition}
    results: SegmentedResultSet = why.log(df, schema=DatasetSchema(segments=test_segments))
    results.writer().option(base_dir=tmp_path).write(use_v0=True)

    paths = glob(os.path.join(tmp_path) + "/*.bin")
    assert len(paths) == input_rows
    roundtrip_profiles = []
    for file_path in paths:
        roundtrip_profiles.append(read_v0_to_view(file_path))
    assert len(roundtrip_profiles) == input_rows
    TEST_LOGGER.info(roundtrip_profiles)
    TEST_LOGGER.info(roundtrip_profiles[15])

    post_deserialization_view = roundtrip_profiles[15]
    assert post_deserialization_view is not None
    assert isinstance(post_deserialization_view, DatasetProfileView)

    post_columns = post_deserialization_view.get_columns()
    assert "A" in post_columns.keys()
    assert "B" in post_columns.keys()


def test_merge_view() -> None:
    df = pd.DataFrame(data={"col1": [1, 2]})
    logger = why.logger()
    results = logger.log(df)
    merged_results = results.merge(ViewResultSet.zero())
    view = merged_results.view()
    assert view._columns["col1"]._metrics["types"].integral.value == 2


def test_merge_two_result_sets() -> None:
    df1 = pd.DataFrame(data={"col1": [1, 2]})
    df2 = pd.DataFrame(data={"col1": [3, 4]})
    logger = why.logger()
    results1 = logger.log(df1)
    results2 = logger.log(df2)
    merged_results = results1.merge(results2)
    view = merged_results.view()
    assert view._columns["col1"]._metrics["types"].integral.value == 4
    assert view._columns["col1"]._metrics["distribution"].min == 1
    assert view._columns["col1"]._metrics["distribution"].max == 4


def test_merge_result_set_zero() -> None:
    df = pd.DataFrame(data={"col1": [1, 2]})
    logger = why.logger()
    results = logger.log(df)
    merged_results = results.merge(ProfileResultSet.zero())
    view = merged_results.view()
    assert view._columns["col1"]._metrics["types"].integral.value == 2


def test_pickle_load_merge_profile_view() -> None:
    df = pd.DataFrame(data={"col1": [1, 2]})
    logger = why.logger()
    results = logger.log(df)
    view2 = logger.log({"col1": 3}).view()
    pickle_loaded_view = None
    with tempfile.NamedTemporaryFile() as tmp_file:
        pickle.dump(results.view(), tmp_file)
        tmp_file.flush()
        tmp_file.seek(0)
        pickle_loaded_view = pickle.load(tmp_file)

    assert pickle_loaded_view is not None
    assert isinstance(pickle_loaded_view, DatasetProfileView)

    merged_view = view2.merge(pickle_loaded_view)
    assert merged_view._columns["col1"]._metrics["types"].integral.value == 3


def test_segment_merge_different_columns() -> None:
    input_rows = 35
    d = {
        "A": [i % 7 for i in range(input_rows)],
        "B": [f"x{str(i%5)}" for i in range(input_rows)],
    }
    input_rows2 = 27
    d2 = {
        "A": [i % 4 for i in range(input_rows2)],
        "B": [f"x{str(i%7)}" for i in range(input_rows2)],
        "C": [bool(i % 2) for i in range(input_rows2)],
    }

    df = pd.DataFrame(data=d)
    df2 = pd.DataFrame(data=d2)
    segmentation_partition = SegmentationPartition(name="A,B", mapper=ColumnMapperFunction(col_names=["A", "B"]))
    test_segments = {segmentation_partition.name: segmentation_partition}
    segmented_schema = DatasetSchema(segments=test_segments)
    results: SegmentedResultSet = why.log(df, schema=segmented_schema)
    results2: SegmentedResultSet = why.log(df2, schema=segmented_schema)
    merged_results = results.merge(results2)

    assert merged_results.count == 42
    for segment in merged_results.segments():
        segmented_view = merged_results.view(segment=segment)
        if len(segmented_view._columns) == 3:
            assert segmented_view._columns["C"] is not None
            assert segmented_view._columns["C"]._metrics["types"].boolean.value > 0
        else:
            # some segments haven't seen column 'C' and so only have two columns
            assert len(segmented_view._columns) == 2
            assert segmented_view._columns["A"] is not None
            assert segmented_view._columns["B"] is not None
        assert segmented_view._columns["A"]._metrics["cardinality"].estimate == pytest.approx(1.0)
