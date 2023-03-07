import logging
from datetime import datetime, timedelta
from typing import Any, List, Optional, Tuple

import numpy as np
import pytest
from dateutil import tz

from whylogs.core.resolvers import MetricSpec, ResolverSpec
from whylogs.core.schema import DatasetSchema, DeclarativeSchema
from whylogs.experimental.extras.embedding_metric import (
    DistanceFunction,
    EmbeddingConfig,
    EmbeddingMetric,
)
from whylogs.experimental.preprocess.embeddings.selectors import PCAKMeansSelector

try:
    from whylogs.api.logger.experimental.multi_dataset_logger.multi_dataset_rolling_logger import (
        MultiDatasetRollingLogger,
    )
except Exception as e:
    if str(e) == "'type' object is not subscriptable":
        pytest.skip("Skipping module because of a pytest bug on older python versions.", allow_module_level=True)

from whylogs.api.logger.experimental.multi_dataset_logger.time_util import (
    TimeGranularity,
)
from whylogs.api.writer import Writer
from whylogs.api.writer.writer import Writable
from whylogs.core.segmentation_partition import (
    ColumnMapperFunction,
    SegmentationPartition,
)

logging.basicConfig()


class BadWriter(Writer):
    write_count = 0

    def write(self, file: Writable, dest: Optional[str] = None, **kwargs: Any) -> Tuple[bool, str]:
        self.write_count += 1
        raise Exception("I'm bad")
        return False, ""

    def option(self, **kwargs: Any) -> "BadWriter":
        return self


class TestWriter(Writer):
    write_count = 0

    def write(self, file: Writable, dest: Optional[str] = None, **kwargs: Any) -> Tuple[bool, str]:
        self.write_count += 1
        return True, ""

    def option(self, **kwargs: Any) -> "TestWriter":
        return self


def test_happy_path() -> None:
    writer = TestWriter()
    logger = MultiDatasetRollingLogger(writers=[writer])

    ts = 1677207714000
    row = {"col1": 1, "col2": "b"}
    logger.log(row, timestamp_ms=ts)
    logger.log(row, timestamp_ms=ts)
    logger.log(row, timestamp_ms=ts)

    result = logger._status()
    assert result.dataset_profiles == 1
    assert result.dataset_timestamps == 1
    assert result.segment_caches == 0
    logger.close()
    assert writer.write_count == 1


def test_no_segments_schema() -> None:
    writer = TestWriter()

    # Make a fancy embedding schema with no segmentation
    fake_embeddings: List[List[float]] = [[1.0 for i in range(0, 50)] for i in range(0, 30)]
    references, _ = PCAKMeansSelector(n_clusters=8, n_components=20).calculate_references_unsupervised(
        np.asarray(fake_embeddings)
    )
    config = EmbeddingConfig(
        references=references,
        labels=None,
        distance_fn=DistanceFunction.euclidean,
    )

    schema = DeclarativeSchema([ResolverSpec(column_name="embeddings", metrics=[MetricSpec(EmbeddingMetric, config)])])
    logger = MultiDatasetRollingLogger(writers=[writer], schema=schema)

    ts = 1677207714000
    row = {"col1": 1, "col2": "b"}
    logger.log(row, timestamp_ms=ts)
    logger.log(row, timestamp_ms=ts)
    logger.log(row, timestamp_ms=ts)

    result = logger._status()
    assert result.dataset_profiles == 1
    assert result.dataset_timestamps == 1
    assert result.segment_caches == 0
    logger.close()
    assert writer.write_count == 1


def test_track_errors_throw() -> None:
    writer = TestWriter()
    logger = MultiDatasetRollingLogger(writers=[writer])

    ts = 1677207714000
    wrong_format: Any = 2  # Invalid data format

    with pytest.raises(Exception):
        logger.log(wrong_format, timestamp_ms=ts, sync=True)
    logger.close()


def test_log_multiple_rows() -> None:
    writer = TestWriter()
    logger = MultiDatasetRollingLogger(writers=[writer])

    ts = 1677207714000
    row = {"col1": 1, "col2": "b"}
    logger.log([row, row, row, row], timestamp_ms=ts)

    result = logger._status()

    assert result.dataset_profiles == 1
    assert result.dataset_timestamps == 1
    assert result.segment_caches == 0
    logger.close()
    assert writer.write_count == 1


def test_logging_after_close_throws() -> None:
    writer = TestWriter()
    logger = MultiDatasetRollingLogger(writers=[writer])

    ts = 1677207714000
    row = {"col1": 1, "col2": "b"}
    logger.log([row, row, row, row], timestamp_ms=ts)

    result = logger._status()

    assert result.dataset_profiles == 1
    assert result.dataset_timestamps == 1
    assert result.segment_caches == 0

    logger.close()
    assert writer.write_count == 1

    with pytest.raises(Exception):
        logger.log([row, row, row, row], timestamp_ms=ts)


def test_multiple_loggers_work() -> None:
    """
    This test should finish without hanging forever.
    """
    logger = MultiDatasetRollingLogger()
    logger.close()

    logger = MultiDatasetRollingLogger()
    row = {"col1": 1, "col2": "b"}
    logger.log(row)

    logger._status()
    logger.close()


def test_flushing_doesnt_break_everything() -> None:
    writer = TestWriter()
    logger = MultiDatasetRollingLogger(writers=[writer])

    ts = 1677207714000
    row = {"col1": 1, "col2": "b"}

    logger.log(row, timestamp_ms=ts)
    logger.flush()

    logger.log(row, timestamp_ms=ts)
    logger.flush()

    logger.log(row, timestamp_ms=ts)
    logger.flush()

    result = logger._status()

    assert result.dataset_profiles == 0
    assert result.dataset_timestamps == 0
    assert result.segment_caches == 0
    logger.close()
    assert writer.write_count == 3


def test_bad_writer() -> None:
    row = {"col1": 1, "col2": "b"}
    writer = BadWriter()
    logger = MultiDatasetRollingLogger(writers=[writer], aggregate_by=TimeGranularity.Hour)

    # Create profiles for three different time periods
    date = datetime.fromtimestamp(1677207714, tz=tz.tzutc())
    one_hour = timedelta(hours=1)
    logger.log(row, int((date - one_hour).timestamp() * 1000))
    logger.log(row, int((date - one_hour - one_hour).timestamp() * 1000))
    logger.log(row, int((date - one_hour - one_hour - one_hour).timestamp() * 1000))

    # Confirm we have three profiles
    result = logger._status()
    assert result.dataset_profiles == 3
    assert result.dataset_timestamps == 3
    assert result.segment_caches == 0
    assert result.pending_writables == 0
    assert result.writers == 1
    assert writer.write_count == 0

    # Flush and trigger doomed writes. The profiles should still be pending after the flush
    logger.flush()
    result2 = logger._status()
    assert result2.dataset_profiles == 0
    assert result2.dataset_timestamps == 0
    assert result2.segment_caches == 0
    assert result2.pending_writables == 3
    assert result2.writers == 1
    assert writer.write_count == 3

    # And confirm that they are retried in further flushes
    logger.close()
    assert writer.write_count == 9


def test_segments_works() -> None:
    col1_3_partition = SegmentationPartition(
        name="col1,col3",
        mapper=ColumnMapperFunction(col_names=["col1", "col3"]),
    )
    multi_column_segments = {col1_3_partition.name: col1_3_partition}
    schema = DatasetSchema(segments=multi_column_segments)

    ts = 1677207714000
    row = {"col1": 1, "col2": "b", "col3": 0.1}
    writer = BadWriter()
    logger = MultiDatasetRollingLogger(writers=[writer], aggregate_by=TimeGranularity.Hour, schema=schema)

    logger.log(row, timestamp_ms=ts, sync=True)

    result = logger._status()
    assert result.dataset_profiles == 0
    assert result.dataset_timestamps == 1
    assert result.segment_caches == 1
    assert result.pending_writables == 0
    assert result.writers == 1
    assert writer.write_count == 0

    logger.flush()
    result2 = logger._status()

    assert result2.dataset_profiles == 0
    assert result2.dataset_timestamps == 0
    assert result2.segment_caches == 0
    assert result2.pending_writables == 1
    assert result2.writers == 1
    assert writer.write_count == 1
    logger.close()


def test_multiple_segments_works() -> None:
    col1_3_partition = SegmentationPartition(
        name="col1",
        mapper=ColumnMapperFunction(col_names=["col1"]),
    )
    multi_column_segments = {col1_3_partition.name: col1_3_partition}
    schema = DatasetSchema(segments=multi_column_segments)

    ts = 1677207714000
    # Create data with three segments/three unique col1 values
    data = [
        {"col1": 1, "col2": "b", "col3": 0.1},
        {"col1": 2, "col2": "b", "col3": 0.2},
        {"col1": 3, "col2": "b", "col3": 0.2},
    ]
    writer = BadWriter()
    logger = MultiDatasetRollingLogger(writers=[writer], aggregate_by=TimeGranularity.Hour, schema=schema)

    logger.log(data, timestamp_ms=ts, sync=True)

    result = logger._status()
    assert result.dataset_profiles == 0
    assert result.dataset_timestamps == 1
    assert result.segment_caches == 1
    assert result.pending_writables == 0
    assert result.writers == 1
    assert writer.write_count == 0

    logger.flush()
    result2 = logger._status()

    assert result2.dataset_profiles == 0
    assert result2.dataset_timestamps == 0
    assert result2.segment_caches == 0
    assert result2.pending_writables == 3
    assert result2.writers == 1
    assert writer.write_count == 3
    logger.close()
