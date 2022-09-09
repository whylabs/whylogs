import os
from glob import glob
from typing import Any

import numpy as np
import pandas as pd

import whylogs as why
from whylogs.api.logger.result_set import SegmentedResultSet
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


def test_segment_write(tmp_path: Any) -> None:
    input_rows = 10
    segment_column = "col3"
    number_of_segments = 2
    values_per_segment = input_rows / number_of_segments
    d = {
        "col1": [i for i in range(input_rows)],
        "col2": [i * i * 1.1 for i in range(input_rows)],
        segment_column: [f"x{str(i%number_of_segments)}" for i in range(input_rows)],
    }

    df = pd.DataFrame(data=d)
    test_segments = segment_on_column(segment_column)

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
    count = segment_distribution.n
    assert count is not None
    assert count == values_per_segment

    results.writer().option(base_dir=tmp_path).write()
    paths = glob(os.path.join(tmp_path) + "/*x0.bin")
    assert len(paths) == 1
    roundtrip_profiles = []
    for file_path in paths:
        roundtrip_profiles.append(read_v0_to_view(file_path))
    assert len(roundtrip_profiles) == 1
    post_deserialization_first_view = roundtrip_profiles[0]
    assert post_deserialization_first_view is not None
    assert isinstance(post_deserialization_first_view, DatasetProfileView)
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
