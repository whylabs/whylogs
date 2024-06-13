import datetime
import logging
import multiprocessing as mp
import os
import time
from pprint import pformat
from typing import Any, Dict, Generator, List, Optional, Tuple, Type, Union, cast

import pandas as pd
import pytest
from dateutil import tz

from whylogs.api.logger.experimental.logger.actor.data_logger import DataLogger
from whylogs.api.logger.experimental.logger.actor.process_actor import QueueType
from whylogs.api.logger.experimental.logger.actor.process_rolling_logger import (
    ProcessRollingLogger,
    WriterFactory,
)
from whylogs.api.logger.experimental.logger.actor.process_rolling_logger_messages import (
    ProcessLoggerStatus,
)
from whylogs.api.logger.experimental.logger.actor.thread_rolling_logger import (
    LoggerStatus,
    ThreadRollingLogger,
)
from whylogs.api.logger.experimental.logger.actor.time_util import TimeGranularity
from whylogs.api.logger.segment_processing import SegmentationPartition
from whylogs.api.whylabs.session.session_manager import SessionManager, init
from whylogs.api.writer.writer import _Writable, Writer
from whylogs.core.dataset_profile import DatasetProfile
from whylogs.core.resolvers import STANDARD_RESOLVER
from whylogs.core.segmentation_partition import ColumnMapperFunction
from whylogs.core.view.dataset_profile_view import DatasetProfileView
from whylogs.experimental.core.udf_schema import UdfSchema, UdfSpec

logging.basicConfig(level=logging.DEBUG)

dataset_id = "model-1"


class FakeWriter(Writer):
    def __init__(self) -> None:
        super().__init__()
        self._write_calls = mp.Value("i", 0)
        self._writable_files: mp.Queue = mp.Queue()
        self._files: List[str] = []

    @property
    def write_calls(self) -> int:
        with self._write_calls.get_lock():
            return self._write_calls.value  # type: ignore

    @property
    def last_writables(self) -> List[_Writable]:
        # files: List[str] = []
        while not self._writable_files.empty():
            self._files.append(self._writable_files.get())

        return [DatasetProfileView.read(file) for file in self._files]

    def write(
        self,
        file: _Writable,
        dest: Optional[str] = None,
        **kwargs: Any,
    ) -> Tuple[bool, Union[str, List[Tuple[bool, str]]]]:
        with self._write_calls.get_lock():
            self._write_calls.value += 1  # type: ignore
            num = self._write_calls.value  # type: ignore

        file_path = f"/tmp/profile_{num}.bin"
        file._write(path="/tmp", filename=f"profile_{num}.bin")
        self._writable_files.put(file_path)

        return True, ""

    def option(self: Any, **kwargs: Any) -> Any:
        pass


class FasterFifoRollingLogger(ProcessRollingLogger):
    def __init__(
        self,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            **kwargs,
            queue_type=QueueType.FASTER_FIFO,
        )


class MPRollingLogger(ProcessRollingLogger):
    def __init__(
        self,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            **kwargs,
            queue_type=QueueType.MP,
        )


@pytest.fixture(scope="module", autouse=True)
def init_tests() -> Generator[None, None, None]:
    SessionManager.reset()  # Ensure this test is in a good state from previous moduels
    init(default_dataset_id=dataset_id)
    yield
    SessionManager.reset()  # Leave it in a good state for future modules


params = [MPRollingLogger, ThreadRollingLogger]
if os.name != "nt":
    params.append(FasterFifoRollingLogger)


@pytest.fixture(scope="function", params=params)
def actor(
    request: pytest.FixtureRequest,
) -> Generator[Tuple[Union[ProcessRollingLogger, ThreadRollingLogger], FakeWriter], None, None]:
    Act: Union[Type[ProcessRollingLogger], Type[ThreadRollingLogger]] = request.param
    writer = FakeWriter()

    class MyWriterFactory(WriterFactory):
        def create_writers(self, dataset_id: str) -> List[Writer]:
            # This is hard coded to create whylabs writers. We don't want to do any uploading
            # during unit tests though.
            return [writer]

    if issubclass(Act, ProcessRollingLogger):
        actor = Act(
            write_schedule=None,
            aggregate_by=TimeGranularity.Day,
            sync_enabled=True,
            writer_factory=MyWriterFactory(),
        )  # type: ignore
        actor.start()
    else:
        actor = Act(write_schedule=None, writers=[writer], aggregate_by=TimeGranularity.Day)  # type: ignore

    yield actor, writer

    try:
        actor.close()
    except Exception:
        # Probably closed as part of the test
        pass


def test_actor_happy_path(actor: Tuple[DataLogger, FakeWriter]) -> None:
    logger, writer = actor

    ms = 1689881671000

    logger.log(data={"a": 1}, sync=True, timestamp_ms=ms)
    logger.log(data={"b": 2}, sync=True, timestamp_ms=ms)
    logger.log(data={"c": 3}, sync=True, timestamp_ms=ms)
    status = logger.status()
    logger.close()

    dt = datetime.datetime.fromtimestamp(ms / 1000.0, tz=tz.tzutc())
    profile = DatasetProfile(dataset_timestamp=dt)
    profile.track({"a": 1})
    profile.track({"b": 2})
    profile.track({"c": 3})

    assert_status_single(
        status,
        LoggerStatus(
            dataset_profiles=1,
            dataset_timestamps=1,
            pending_writables=0,
            segment_caches=0,
            writers=1,
            views=[profile.view().serialize()],
            pending_views=[],
        ),
        dataset_id,
    )

    assert writer.write_calls == 1

    profile = cast(DatasetProfileView, writer.last_writables[0])
    assert_profile(profile, ["a", "b", "c"])


def test_segment_computed_column() -> None:
    # This test ensures that the thread rolling logger actually runs UDFs when working with segments.
    # This has to be done manually in the logger layer.
    ORIGINAL_COLUMN = "original_column"
    GENERATED_SEGMENT_COLUMN = "generated_column"
    segment_def = SegmentationPartition(
        name=GENERATED_SEGMENT_COLUMN,
        mapper=ColumnMapperFunction(col_names=[GENERATED_SEGMENT_COLUMN]),
    )

    def _convert_score_to_int(score: float) -> int:
        """
        round the score to 0 or 1
        """
        return int(round(score))

    def converter_udf(text: Union[pd.DataFrame, Dict[str, List[Any]]]) -> Any:
        # print(f">>> Refusal boolean UDF {text}")
        if isinstance(text, pd.DataFrame):
            return text[ORIGINAL_COLUMN].apply(_convert_score_to_int)  # type: ignore
        else:
            return [_convert_score_to_int(score) for score in text[ORIGINAL_COLUMN]]

    segmented_schema = UdfSchema(
        segments={segment_def.name: segment_def},
        resolvers=STANDARD_RESOLVER,
        udf_specs=[
            UdfSpec(
                column_names=[ORIGINAL_COLUMN],
                udfs={GENERATED_SEGMENT_COLUMN: converter_udf},
            )
        ],
    )
    writer = FakeWriter()
    logger = ThreadRollingLogger(
        write_schedule=None,
        writers=[writer],
        aggregate_by=TimeGranularity.Day,
        schema=segmented_schema,
    )

    ms = 1689881671000

    logger.log(data={"a": 1, ORIGINAL_COLUMN: 0.1}, sync=True, timestamp_ms=ms)
    logger.log(data={"b": 2, ORIGINAL_COLUMN: 0.2}, sync=True, timestamp_ms=ms)
    logger.log(data={"c": 3, ORIGINAL_COLUMN: 0.3}, sync=True, timestamp_ms=ms)

    logger.log(data={"a": 1, ORIGINAL_COLUMN: 0.6}, sync=True, timestamp_ms=ms)
    logger.log(data={"b": 2, ORIGINAL_COLUMN: 0.7}, sync=True, timestamp_ms=ms)
    logger.log(data={"c": 3, ORIGINAL_COLUMN: 0.9}, sync=True, timestamp_ms=ms)
    status = logger.status()
    logger.close()

    dt = datetime.datetime.fromtimestamp(ms / 1000.0, tz=tz.tzutc())
    profileA = DatasetProfile(dataset_timestamp=dt)
    profileA.track({"a": 1, ORIGINAL_COLUMN: 0.1, GENERATED_SEGMENT_COLUMN: 0})  # type: ignore
    profileA.track({"b": 2, ORIGINAL_COLUMN: 0.2, GENERATED_SEGMENT_COLUMN: 0})  # type: ignore
    profileA.track({"c": 3, ORIGINAL_COLUMN: 0.3, GENERATED_SEGMENT_COLUMN: 0})  # type: ignore

    profileB = DatasetProfile(dataset_timestamp=dt)
    profileB.track({"a": 1, ORIGINAL_COLUMN: 0.6, GENERATED_SEGMENT_COLUMN: 1})  # type: ignore
    profileB.track({"b": 2, ORIGINAL_COLUMN: 0.7, GENERATED_SEGMENT_COLUMN: 1})  # type: ignore
    profileB.track({"c": 3, ORIGINAL_COLUMN: 0.9, GENERATED_SEGMENT_COLUMN: 1})  # type: ignore

    expected = LoggerStatus(
        dataset_profiles=0,
        dataset_timestamps=1,
        pending_writables=0,
        segment_caches=1,
        writers=1,
        views=[profileA.view().serialize(), profileB.view().serialize()],
        pending_views=[],
    )

    assert_status_single(status, expected, dataset_id)

    # One call for each segment
    assert writer.write_calls == 2

    profile = cast(DatasetProfileView, writer.last_writables[0])
    assert_profile(profile, ["a", "b", "c", GENERATED_SEGMENT_COLUMN, ORIGINAL_COLUMN])


def test_segment_computed_column_list_input() -> None:
    # This test ensures that the thread rolling logger actually runs UDFs when working with segments.
    # This has to be done manually in the logger layer.
    ORIGINAL_COLUMN = "original_column"
    GENERATED_SEGMENT_COLUMN = "generated_column"
    segment_def = SegmentationPartition(
        name=GENERATED_SEGMENT_COLUMN,
        mapper=ColumnMapperFunction(col_names=[GENERATED_SEGMENT_COLUMN]),
    )

    def _convert_score_to_int(score: float) -> int:
        """
        round the score to 0 or 1
        """
        return int(round(score))

    def converter_udf(text: Union[pd.DataFrame, Dict[str, List[Any]]]) -> Any:
        # print(f">>> Refusal boolean UDF {text}")
        if isinstance(text, pd.DataFrame):
            return text[ORIGINAL_COLUMN].apply(_convert_score_to_int)  # type: ignore
        else:
            return [_convert_score_to_int(score) for score in text[ORIGINAL_COLUMN]]

    segmented_schema = UdfSchema(
        segments={segment_def.name: segment_def},
        resolvers=STANDARD_RESOLVER,
        udf_specs=[
            UdfSpec(
                column_names=[ORIGINAL_COLUMN],
                udfs={GENERATED_SEGMENT_COLUMN: converter_udf},
            )
        ],
    )
    writer = FakeWriter()
    logger = ThreadRollingLogger(
        write_schedule=None,
        writers=[writer],
        aggregate_by=TimeGranularity.Day,
        schema=segmented_schema,
    )

    ms = 1689881671000

    data = [
        {"a": 1, ORIGINAL_COLUMN: 0.1},
        {"b": 2, ORIGINAL_COLUMN: 0.2},
        {"c": 3, ORIGINAL_COLUMN: 0.3},
        {"a": 1, ORIGINAL_COLUMN: 0.6},
        {"b": 2, ORIGINAL_COLUMN: 0.7},
        {"c": 3, ORIGINAL_COLUMN: 0.9},
    ]

    logger.log(data=data, sync=True, timestamp_ms=ms)

    status = logger.status()
    logger.close()

    dt = datetime.datetime.fromtimestamp(ms / 1000.0, tz=tz.tzutc())
    profileA = DatasetProfile(dataset_timestamp=dt)
    profileA.track({"a": 1, ORIGINAL_COLUMN: 0.1, GENERATED_SEGMENT_COLUMN: 0})  # type: ignore
    profileA.track({"b": 2, ORIGINAL_COLUMN: 0.2, GENERATED_SEGMENT_COLUMN: 0})  # type: ignore
    profileA.track({"c": 3, ORIGINAL_COLUMN: 0.3, GENERATED_SEGMENT_COLUMN: 0})  # type: ignore

    profileB = DatasetProfile(dataset_timestamp=dt)
    profileB.track({"a": 1, ORIGINAL_COLUMN: 0.6, GENERATED_SEGMENT_COLUMN: 1})  # type: ignore
    profileB.track({"b": 2, ORIGINAL_COLUMN: 0.7, GENERATED_SEGMENT_COLUMN: 1})  # type: ignore
    profileB.track({"c": 3, ORIGINAL_COLUMN: 0.9, GENERATED_SEGMENT_COLUMN: 1})  # type: ignore

    expected = LoggerStatus(
        dataset_profiles=0,
        dataset_timestamps=1,
        pending_writables=0,
        segment_caches=1,
        writers=1,
        views=[profileA.view().serialize(), profileB.view().serialize()],
        pending_views=[],
    )

    assert_status_single(status, expected, dataset_id)

    # One call for each segment
    assert writer.write_calls == 2

    profile = cast(DatasetProfileView, writer.last_writables[0])
    assert_profile(profile, ["a", "b", "c", GENERATED_SEGMENT_COLUMN, ORIGINAL_COLUMN])


def test_actor_null_timestamp(actor: Tuple[DataLogger, FakeWriter]) -> None:
    logger, writer = actor

    logger.log(data={"a": 1}, sync=True)
    status = logger.status()
    logger.close()

    profile = DatasetProfile()
    profile.track({"a": 1})

    assert_status_single(
        status,
        LoggerStatus(
            dataset_profiles=1,
            dataset_timestamps=1,
            pending_writables=0,
            segment_caches=0,
            writers=1,
            views=[profile.view().serialize()],
            pending_views=[],
        ),
        dataset_id,
    )

    assert writer.write_calls == 1

    profile = cast(DatasetProfileView, writer.last_writables[0])
    assert_profile(profile, ["a"])


def test_flush(actor: Tuple[DataLogger, FakeWriter]) -> None:
    logger, writer = actor
    ms = 1689881671000

    # First flush content
    logger.log(data={"a": 1}, sync=True, timestamp_ms=ms)
    logger.flush()
    # Second flush content, triggered by close()
    logger.log(data={"b": 2}, sync=True, timestamp_ms=ms)
    logger.log(data={"c": 3}, sync=True, timestamp_ms=ms)

    assert writer.write_calls == 1
    assert_profile(cast(DatasetProfileView, writer.last_writables[0]), ["a"])

    logger.close()

    assert writer.write_calls == 2
    assert_profile(cast(DatasetProfileView, writer.last_writables[0]), ["a"])
    assert_profile(cast(DatasetProfileView, writer.last_writables[1]), ["b", "c"])


@pytest.mark.parametrize("Act", [ProcessRollingLogger, ThreadRollingLogger])
def test_multiple_writers(
    Act: Union[Type[ProcessRollingLogger], Type[ThreadRollingLogger]],
) -> None:
    writer1 = FakeWriter()
    writer2 = FakeWriter()

    class DoubleWriterFactory(WriterFactory):
        def create_writers(self, dataset_id: str) -> List[Writer]:
            return [writer1, writer2]

    if issubclass(Act, ProcessRollingLogger):
        actor = Act(
            write_schedule=None,
            aggregate_by=TimeGranularity.Day,
            sync_enabled=True,
            writer_factory=DoubleWriterFactory(),
        )  # type: ignore
        actor.start()
    else:
        actor = Act(
            write_schedule=None,
            writers=[writer1, writer2],
            aggregate_by=TimeGranularity.Day,
        )  # type: ignore

    ms = 1689881671000

    actor.log(data={"a": 1}, sync=True, timestamp_ms=ms)
    actor.log(data={"b": 2}, sync=True, timestamp_ms=ms)
    actor.log(data={"c": 3}, sync=True, timestamp_ms=ms)
    status = actor.status()
    actor.close()

    profile = DatasetProfile()
    profile.track({"a": 1})
    profile.track({"b": 2})
    profile.track({"c": 3})

    assert_status_single(
        status,
        LoggerStatus(
            dataset_profiles=1,
            dataset_timestamps=1,
            pending_writables=0,
            segment_caches=0,
            writers=2,
            views=[profile.view().serialize()],
            pending_views=[],
        ),
        dataset_id,
    )

    assert writer1.write_calls == 1
    assert writer2.write_calls == 1

    assert_profile(cast(DatasetProfileView, writer1.last_writables[0]), ["a", "b", "c"])
    assert_profile(cast(DatasetProfileView, writer2.last_writables[0]), ["a", "b", "c"])


@pytest.mark.parametrize("Act", [ProcessRollingLogger, ThreadRollingLogger])
def test_track_errors_throw(
    Act: Union[Type[ProcessRollingLogger], Type[ThreadRollingLogger]],
) -> None:
    i = mp.Value("i", 0)

    def throw_error() -> int:
        nonlocal i
        # Throw on the second call. The process logger needs to call this once without throwing, it will throw
        # inside of the thread logger that the process logger manages.
        with i.get_lock():
            if i.value == 1:  # type: ignore
                raise Exception("Error getting time")
            i.value += 1  # type: ignore
            return 0

    writer1 = FakeWriter()

    class MyWriterFactory(WriterFactory):
        def create_writers(self, dataset_id: str) -> List[Writer]:
            return [writer1]

    if issubclass(Act, ProcessRollingLogger):
        actor = Act(
            write_schedule=None,
            aggregate_by=TimeGranularity.Day,
            sync_enabled=True,
            current_time_fn=throw_error,
            writer_factory=MyWriterFactory(),
        )  # type: ignore
        actor.start()
    else:
        actor = Act(
            write_schedule=None,
            writers=[writer1],
            aggregate_by=TimeGranularity.Day,
            current_time_fn=throw_error,
        )  # type: ignore
        with i.get_lock():
            # bump this up to 1 so it throws right away since we're going direclty to
            # the thread logger in this case.
            i.value = 1  # type: ignore

    # ts = 1677207714000
    # wrong_format: Any = 2  # Invalid data format

    # In the thread logger this just throws right away synchronously, but in the process logger
    try:
        with pytest.raises(Exception, match="Error getting time"):
            actor.log(data={"a": 1}, sync=True)

        # This one will work normally without throwing
        with i.get_lock():
            i.value = 2  # type: ignore
        actor.log(data={"a": 1}, sync=True)
        actor.log(data={"b": 1}, sync=True)
        actor.log(data={"c": 1}, sync=True)
    finally:
        actor.close()

    assert writer1.write_calls == 1
    assert_profile(cast(DatasetProfileView, writer1.last_writables[0]), ["a", "b", "c"])


def test_closing_works(actor: Tuple[DataLogger, FakeWriter]) -> None:
    logger, writer = actor
    ms = 1689881671000
    ms1 = add_days(ms, 1)
    ms2 = add_days(ms, 2)

    logger.log(data={"a": 1}, sync=True, timestamp_ms=ms)
    logger.log(data={"b": 2}, sync=True, timestamp_ms=ms1)
    logger.log(data={"c": 3}, sync=True, timestamp_ms=ms2)
    status = logger.status()
    logger.close()

    profile = DatasetProfile(dataset_timestamp=datetime.datetime.fromtimestamp(ms / 1000.0, tz=tz.tzutc()))
    profile.track({"a": 1})

    profile1 = DatasetProfile(dataset_timestamp=datetime.datetime.fromtimestamp(ms1 / 1000.0, tz=tz.tzutc()))
    profile1.track({"b": 2})

    profile2 = DatasetProfile(dataset_timestamp=datetime.datetime.fromtimestamp(ms2 / 1000.0, tz=tz.tzutc()))
    profile2.track({"c": 3})

    assert_status_single(
        status,
        LoggerStatus(
            dataset_profiles=3,
            dataset_timestamps=3,
            pending_writables=0,
            segment_caches=0,
            writers=1,
            views=[
                profile.view().serialize(),
                profile1.view().serialize(),
                profile2.view().serialize(),
            ],
            pending_views=[],
        ),
        dataset_id,
    )

    assert writer.write_calls == 3
    assert len(writer.last_writables) == 3

    assert_profile(cast(DatasetProfileView, writer.last_writables[0]), ["a"])
    assert_profile(cast(DatasetProfileView, writer.last_writables[1]), ["b"])
    assert_profile(cast(DatasetProfileView, writer.last_writables[2]), ["c"])

    # Further calls after close should throw
    if isinstance(logger, ProcessRollingLogger):
        match = "Logger process is no longer alive. It may have been killed."
    else:
        match = "Actor is closed, can't send message."
    with pytest.raises(Exception, match=match):
        logger.log(data={"a": 1}, sync=True, timestamp_ms=ms)

    # These shouldn't change
    assert writer.write_calls == 3
    assert len(writer.last_writables) == 3


def test_process_throws_after_killed(actor: Tuple[DataLogger, FakeWriter]) -> None:
    """
    Test that the logger throws after the process is killed on the caller side. This
    version of the test asserts against the sync=True behavior. First the process is force
    killed and then immediately after the logger is used, which means the is_alive check
    won't have time to start returning false. This tests the case where something kills the process
    while something else is trying to use it.
    """
    logger, _ = actor
    if isinstance(logger, ProcessRollingLogger):
        logger = cast(ProcessRollingLogger, logger)  # type: ignore
        ms = 1689881671000

        # kill it
        os.kill(logger.pid, 9)  # type: ignore

        # Further sync calls close should throw
        with pytest.raises(Exception):
            logger.log(data={"a": 1}, sync=True, timestamp_ms=ms)

        with pytest.raises(Exception, match="Process isn't active. It might have been killed."):
            logger.close()


def test_process_throws_after_killed_delay(
    actor: Tuple[DataLogger, FakeWriter],
) -> None:
    """
    Very similar to test_process_throws_after_killed but there is a delay after the process is killed
    before logging so the log() call will throw before doing any actual work with a clear error message.
    """
    logger, _ = actor
    if isinstance(logger, ProcessRollingLogger):
        logger = cast(ProcessRollingLogger, logger)  # type: ignore
        ms = 1689881671000

        # kill it
        os.kill(logger.pid, 9)  # type: ignore
        time.sleep(2)  # should be enough

        # Further sync calls close should throw
        if isinstance(logger, ProcessRollingLogger):
            match = "Logger process is no longer alive. It may have been killed."
        else:
            match = "Actor is closed, can't send message."
        with pytest.raises(Exception, match=match):
            # Throws even when it isn't sync
            logger.log(data={"a": 1}, timestamp_ms=ms)

        with pytest.raises(Exception, match="Process isn't active. It might have been killed."):
            logger.close()


def test_actor_multiple_days(actor: Tuple[DataLogger, FakeWriter]) -> None:
    logger, writer = actor
    ms = 1689881671000
    ms1 = add_days(ms, 1)
    ms2 = add_days(ms, 2)

    logger.log(data={"a": 1}, sync=True, timestamp_ms=ms)
    logger.log(data={"b": 2}, sync=True, timestamp_ms=ms1)
    logger.log(data={"c": 3}, sync=True, timestamp_ms=ms2)
    status = logger.status()
    logger.close()

    profile = DatasetProfile(dataset_timestamp=datetime.datetime.fromtimestamp(ms / 1000.0, tz=tz.tzutc()))
    profile.track({"a": 1})

    profile1 = DatasetProfile(dataset_timestamp=datetime.datetime.fromtimestamp(ms1 / 1000.0, tz=tz.tzutc()))
    profile1.track({"b": 2})

    profile2 = DatasetProfile(dataset_timestamp=datetime.datetime.fromtimestamp(ms2 / 1000.0, tz=tz.tzutc()))
    profile2.track({"c": 3})

    assert_status_single(
        status,
        LoggerStatus(
            dataset_profiles=3,
            dataset_timestamps=3,
            pending_writables=0,
            segment_caches=0,
            writers=1,
            views=[
                profile.view().serialize(),
                profile1.view().serialize(),
                profile2.view().serialize(),
            ],
            pending_views=[],
        ),
        dataset_id,
    )

    assert writer.write_calls == 3
    assert len(writer.last_writables) == 3

    assert_profile(cast(DatasetProfileView, writer.last_writables[0]), ["a"])
    assert_profile(cast(DatasetProfileView, writer.last_writables[1]), ["b"])
    assert_profile(cast(DatasetProfileView, writer.last_writables[2]), ["c"])


def test_close_stops_accepting_logs(actor: Tuple[DataLogger, FakeWriter]) -> None:
    """
    Caling close on the logger should stop accepting logs and then synchronously wait
    for all of the remaining messages to be processed.
    """
    logger, writer = actor
    ms = 1689881671000

    logger.log(data={"a": 1}, sync=True, timestamp_ms=ms)
    logger.log(data={"b": 2}, sync=True, timestamp_ms=add_days(ms, 1))
    logger.log(data={"c": 3}, sync=True, timestamp_ms=add_days(ms, 2))

    logger.close()

    assert writer.write_calls == 3
    assert len(writer.last_writables) == 3

    assert_profile(cast(DatasetProfileView, writer.last_writables[0]), ["a"])
    assert_profile(cast(DatasetProfileView, writer.last_writables[1]), ["b"])
    assert_profile(cast(DatasetProfileView, writer.last_writables[2]), ["c"])

    if isinstance(logger, ProcessRollingLogger):
        match = "Logger process is no longer alive. It may have been killed."
    else:
        match = "Actor is closed, can't send message."
    with pytest.raises(Exception, match=match):
        logger.log(data={"a": 1}, sync=True, timestamp_ms=ms)


def test_multiple_datasets() -> None:
    writer = FakeWriter()

    class MyWriterFactory(WriterFactory):
        def create_writers(self, dataset_id: str) -> List[Writer]:
            return [writer]

    logger = ProcessRollingLogger(
        write_schedule=None,
        aggregate_by=TimeGranularity.Day,
        sync_enabled=True,
        writer_factory=MyWriterFactory(),
        queue_type=QueueType.MP,
    )
    logger.start()

    ms = 1689881671000
    ms1 = add_days(ms, 1)
    ms2 = add_days(ms, 2)
    ms3 = add_days(ms, 3)
    ms4 = add_days(ms, 4)

    logger.log(data={"a": 1}, sync=True, timestamp_ms=ms, dataset_id="dataset1")
    logger.log(data={"z": 7}, sync=True, timestamp_ms=ms, dataset_id="dataset1")
    logger.log(data={"b": 2}, sync=True, timestamp_ms=ms1, dataset_id="dataset1")
    logger.log(data={"c": 3}, sync=True, timestamp_ms=ms2, dataset_id="dataset1")

    logger.log(data={"d": 1}, sync=True, timestamp_ms=ms, dataset_id="dataset2")
    logger.log(data={"e": 2}, sync=True, timestamp_ms=ms3, dataset_id="dataset2")
    logger.log(data={"f": 3}, sync=True, timestamp_ms=ms4, dataset_id="dataset2")

    status = logger.status()
    logger.close()

    profile = DatasetProfile(dataset_timestamp=datetime.datetime.fromtimestamp(ms / 1000.0, tz=tz.tzutc()))
    profile.track({"a": 1})
    profile.track({"z": 7})

    profile1 = DatasetProfile(dataset_timestamp=datetime.datetime.fromtimestamp(ms1 / 1000.0, tz=tz.tzutc()))
    profile1.track({"b": 2})

    profile2 = DatasetProfile(dataset_timestamp=datetime.datetime.fromtimestamp(ms2 / 1000.0, tz=tz.tzutc()))
    profile2.track({"c": 3})

    profile_ms_2 = DatasetProfile(dataset_timestamp=datetime.datetime.fromtimestamp(ms / 1000.0, tz=tz.tzutc()))
    profile_ms_2.track({"d": 1})

    profile3 = DatasetProfile(dataset_timestamp=datetime.datetime.fromtimestamp(ms / 1000.0, tz=tz.tzutc()))
    profile3.track({"e": 2})

    profile4 = DatasetProfile(dataset_timestamp=datetime.datetime.fromtimestamp(ms3 / 1000.0, tz=tz.tzutc()))
    profile4.track({"f": 3})

    assert_status_single(
        status,
        LoggerStatus(
            dataset_profiles=3,
            dataset_timestamps=3,
            pending_writables=0,
            segment_caches=0,
            writers=1,
            views=[
                profile.view().serialize(),
                profile1.view().serialize(),
                profile2.view().serialize(),
            ],
            pending_views=[],
        ),
        "dataset1",
    )

    assert_status_single(
        status,
        LoggerStatus(
            dataset_profiles=3,
            dataset_timestamps=3,
            pending_writables=0,
            segment_caches=0,
            writers=1,
            views=[
                profile_ms_2.view().serialize(),
                profile3.view().serialize(),
                profile4.view().serialize(),
            ],
            pending_views=[],
        ),
        "dataset2",
    )

    assert writer.write_calls == 6

    assert_profile(cast(DatasetProfileView, writer.last_writables[0]), ["a", "z"])
    assert_profile(cast(DatasetProfileView, writer.last_writables[1]), ["b"])
    assert_profile(cast(DatasetProfileView, writer.last_writables[2]), ["c"])
    assert_profile(cast(DatasetProfileView, writer.last_writables[3]), ["d"])
    assert_profile(cast(DatasetProfileView, writer.last_writables[4]), ["e"])
    assert_profile(cast(DatasetProfileView, writer.last_writables[5]), ["f"])


def assert_profile(view: DatasetProfileView, expected_columns: List[str]) -> None:
    columns = view.get_columns().keys()
    assert len(columns) == len(expected_columns)
    for col in expected_columns:
        assert col in columns


def add_days(ms: int, days: int) -> int:
    date_time = datetime.datetime.fromtimestamp(ms / 1000.0)
    date_time_plus_day = date_time + datetime.timedelta(days=days)
    return int(date_time_plus_day.timestamp() * 1000)


def assert_status_single(
    status: Union[ProcessLoggerStatus, LoggerStatus],
    expected: LoggerStatus,
    dataset_id: str = "",
) -> None:
    def view_to_dict(view_bytes: bytes) -> Dict[Any, Any]:
        return DatasetProfileView.deserialize(view_bytes).to_pandas().to_dict()  # type: ignore

    expected_views: List[Dict[Any, Any]] = [view_to_dict(it) for it in expected.views]
    expected_pending_views: List[Dict[Any, Any]] = [view_to_dict(it) for it in expected.pending_views]

    expected_copy = {
        "dataset_timestamps": expected.dataset_timestamps,
        "dataset_profiles": expected.dataset_profiles,
        "pending_writables": expected.pending_writables,
        "segment_caches": expected.segment_caches,
        "writers": expected.writers,
        "views": pformat(expected_views, indent=2),
        "pending_views": expected_pending_views,
    }

    actual: LoggerStatus = status if isinstance(status, LoggerStatus) else status.statuses[dataset_id]
    actual_views: List[Dict[Any, Any]] = [view_to_dict(it) for it in actual.views]
    actual_pending_views: List[Dict[Any, Any]] = [view_to_dict(it) for it in actual.pending_views]

    actual_copy = {
        "dataset_timestamps": actual.dataset_timestamps,
        "dataset_profiles": actual.dataset_profiles,
        "pending_writables": actual.pending_writables,
        "segment_caches": actual.segment_caches,
        "writers": actual.writers,
        "views": pformat(actual_views, indent=2),
        "pending_views": actual_pending_views,
    }

    assert expected_copy == actual_copy
