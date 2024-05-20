import os
from datetime import datetime, timedelta, timezone
from logging import getLogger

import pandas as pd

import whylogs as why
from whylogs.api.logger.result_set import (
    ResultSet,
    ResultSetWriter,
    SegmentedResultSet,
    ViewResultSet,
)
from whylogs.api.writer.local import LocalWriter
from whylogs.core.schema import DatasetSchema
from whylogs.core.segmentation_partition import segment_on_column
from whylogs.core.view.dataset_profile_view import DatasetProfileView

TEST_LOGGER = getLogger(__name__)


def test_result_set_metadata_on_writables():
    segment_column = "col1"
    df = pd.DataFrame(data={segment_column: [1, 2]})
    results: SegmentedResultSet = why.log(df, schema=DatasetSchema(segments=segment_on_column(segment_column)))

    assert results.metadata is not None
    assert "whylabs.traceId" in results.metadata
    trace_id = results.metadata["whylabs.traceId"]
    custom_metadata_value = "Some custom metadata string"
    custom_metadata_key = "foo"
    results.metadata[custom_metadata_key] = custom_metadata_value

    for profile in results.get_writables():
        assert "whylabs.traceId" in profile.metadata
        assert profile.metadata["whylabs.traceId"] == trace_id
        assert custom_metadata_key in profile.metadata
        assert profile.metadata[custom_metadata_key] == custom_metadata_value


def test_segmented_result_set_timestamp():
    segment_column = "col1"
    df = pd.DataFrame(data={segment_column: [1, 2]})
    results: SegmentedResultSet = why.log(df, schema=DatasetSchema(segments=segment_on_column(segment_column)))
    timestamp = datetime.now(tz=timezone.utc) - timedelta(days=1)
    results.set_dataset_timestamp(timestamp)
    segments = results.segments()
    for segment in segments:
        assert results.view(segment).dataset_timestamp == timestamp

    # convert segment DatasetProfile to DatasetProfileView to test that case
    first_partition = results.partitions[0]
    segments = results.segments_in_partition(first_partition)
    for segment_key in segments:
        profile = segments[segment_key]
        segments[segment_key] = profile.view()

timestamp = datetime.now(tz=timezone.utc) - timedelta(days=2)
    results.set_dataset_timestamp(timestamp)
    segments = results.segments()
    for segment in segments:
        assert results.view(segment).dataset_timestamp == timestamp


def test_view_result_set_timestamp():
    results = ViewResultSet(DatasetProfileView(columns=dict(), dataset_timestamp=None, creation_timestamp=None))
    timestamp = datetime.now(tz=timezone.utc) - timedelta(days=1)
    results.set_dataset_timestamp(timestamp)
    assert results.view().dataset_timestamp == timestamp


def test_result_set_writer(tmp_path: str):
    df = pd.DataFrame(data={"col1": [1, 2]})
    results: ResultSet = why.log(df)
    local_writer = LocalWriter(base_dir=tmp_path, base_name="result_set.bin")
    writer = ResultSetWriter(results, local_writer)
    writer.write()
    expected_path = os.path.join(tmp_path, "result_set.bin")
    assert os.path.isfile(expected_path)
