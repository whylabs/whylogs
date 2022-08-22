from logging import NullHandler

import numpy as np
import pandas as pd

import whylogs as why
from whylogs.api.logger.result_set import SegmentedResultSet
from whylogs.core.metrics.metrics import CardinalityMetric, DistributionMetric
from whylogs.core.schema import DatasetSchema
from whylogs.core.segmentation_partition import (
    ColumnMapperFunction,
    SegmentationPartition,
    segment_on_column,
)


def test_single_row_segment() -> None:
    segment_column = "col3"
    number_of_segments = 1

    segmentation_partition = segment_on_column("col3")
    test_segments = {segmentation_partition.name: segmentation_partition}
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
    segmentation_partition = segment_on_column("col3")
    test_segments = {segmentation_partition.name: segmentation_partition}
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
    segments = results.segments
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
