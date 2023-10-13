import datetime
import logging
import multiprocessing as mp
import os
from typing import Any, Dict, Generator, List, Optional, Tuple, Type, Union, cast

import pytest

from whylogs.api.logger.experimental.logger.actor.data_logger import DataLogger
from whylogs.api.logger.experimental.logger.actor.process_actor import (
    ProcessActor,
    QueueType,
)
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
from whylogs.api.whylabs.session.session_manager import SessionManager, init
from whylogs.api.writer.writer import Writable, Writer
from whylogs.core.view.dataset_profile_view import DatasetProfileView

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
    def last_writables(self) -> List[Writable]:
        # files: List[str] = []
        while not self._writable_files.empty():
            self._files.append(self._writable_files.get())

        return [DatasetProfileView.read(file) for file in self._files]

    def write(
        self,
        file: Writable,
        dest: Optional[str] = None,
        **kwargs: Any,
    ) -> Tuple[bool, str]:
        with self._write_calls.get_lock():
            self._write_calls.value += 1  # type: ignore
            num = self._write_calls.value  # type: ignore

        file_path = f"/tmp/profile_{num}.bin"
        file.write(file_path)
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

    if issubclass(Act, ProcessActor):
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

    assert_status_single(
        status,
        LoggerStatus(dataset_profiles=1, dataset_timestamps=1, pending_writables=0, segment_caches=0, writers=1),
        dataset_id,
    )

    assert writer.write_calls == 1

    profile = cast(DatasetProfileView, writer.last_writables[0])
    assert_profile(profile, ["a", "b", "c"])


def test_actor_null_timestamp(actor: Tuple[DataLogger, FakeWriter]) -> None:
    logger, writer = actor

    logger.log(data={"a": 1}, sync=True)
    status = logger.status()
    logger.close()

    assert_status_single(
        status,
        LoggerStatus(dataset_profiles=1, dataset_timestamps=1, pending_writables=0, segment_caches=0, writers=1),
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
def test_multiple_writers(Act: Union[Type[ProcessRollingLogger], Type[ThreadRollingLogger]]) -> None:
    writer1 = FakeWriter()
    writer2 = FakeWriter()

    class DoubleWriterFactory(WriterFactory):
        def create_writers(self, dataset_id: str) -> List[Writer]:
            return [writer1, writer2]

    if issubclass(Act, ProcessActor):
        actor = Act(
            write_schedule=None,
            aggregate_by=TimeGranularity.Day,
            sync_enabled=True,
            writer_factory=DoubleWriterFactory(),
        )  # type: ignore
        actor.start()
    else:
        actor = Act(write_schedule=None, writers=[writer1, writer2], aggregate_by=TimeGranularity.Day)  # type: ignore

    ms = 1689881671000

    actor.log(data={"a": 1}, sync=True, timestamp_ms=ms)
    actor.log(data={"b": 2}, sync=True, timestamp_ms=ms)
    actor.log(data={"c": 3}, sync=True, timestamp_ms=ms)
    status = actor.status()
    actor.close()

    assert_status_single(
        status,
        LoggerStatus(dataset_profiles=1, dataset_timestamps=1, pending_writables=0, segment_caches=0, writers=2),
        dataset_id,
    )

    assert writer1.write_calls == 1
    assert writer2.write_calls == 1

    assert_profile(cast(DatasetProfileView, writer1.last_writables[0]), ["a", "b", "c"])
    assert_profile(cast(DatasetProfileView, writer2.last_writables[0]), ["a", "b", "c"])


@pytest.mark.parametrize("Act", [ProcessRollingLogger, ThreadRollingLogger])
def test_track_errors_throw(Act: Union[Type[ProcessRollingLogger], Type[ThreadRollingLogger]]) -> None:
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

    if issubclass(Act, ProcessActor):
        actor = Act(
            write_schedule=None,
            aggregate_by=TimeGranularity.Day,
            sync_enabled=True,
            current_time_fn=throw_error,
            writer_factory=MyWriterFactory(),
        )  # type: ignore
        actor.start()
    else:
        actor = Act(write_schedule=None, writers=[writer1], aggregate_by=TimeGranularity.Day, current_time_fn=throw_error)  # type: ignore
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

    logger.log(data={"a": 1}, sync=True, timestamp_ms=ms)
    logger.log(data={"b": 2}, sync=True, timestamp_ms=add_days(ms, 1))
    logger.log(data={"c": 3}, sync=True, timestamp_ms=add_days(ms, 2))
    status = logger.status()
    logger.close()

    assert_status_single(
        status,
        LoggerStatus(dataset_profiles=3, dataset_timestamps=3, pending_writables=0, segment_caches=0, writers=1),
        dataset_id,
    )

    assert writer.write_calls == 3
    assert len(writer.last_writables) == 3

    assert_profile(cast(DatasetProfileView, writer.last_writables[0]), ["a"])
    assert_profile(cast(DatasetProfileView, writer.last_writables[1]), ["b"])
    assert_profile(cast(DatasetProfileView, writer.last_writables[2]), ["c"])

    # Further calls after close should throw
    with pytest.raises(Exception, match="Actor is closed, can't send message."):
        logger.log(data={"a": 1}, sync=True, timestamp_ms=ms)

    # These shouldn't change
    assert writer.write_calls == 3
    assert len(writer.last_writables) == 3


def test_actor_multiple_days(actor: Tuple[DataLogger, FakeWriter]) -> None:
    logger, writer = actor
    ms = 1689881671000

    logger.log(data={"a": 1}, sync=True, timestamp_ms=ms)
    logger.log(data={"b": 2}, sync=True, timestamp_ms=add_days(ms, 1))
    logger.log(data={"c": 3}, sync=True, timestamp_ms=add_days(ms, 2))
    status = logger.status()
    logger.close()

    assert_status_single(
        status,
        LoggerStatus(dataset_profiles=3, dataset_timestamps=3, pending_writables=0, segment_caches=0, writers=1),
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

    with pytest.raises(Exception, match="Actor is closed, can't send message."):
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

    logger.log(data={"a": 1}, sync=True, timestamp_ms=ms, dataset_id="dataset1")
    logger.log(data={"z": 7}, sync=True, timestamp_ms=ms, dataset_id="dataset1")
    logger.log(data={"b": 2}, sync=True, timestamp_ms=add_days(ms, 1), dataset_id="dataset1")
    logger.log(data={"c": 3}, sync=True, timestamp_ms=add_days(ms, 2), dataset_id="dataset1")

    logger.log(data={"d": 1}, sync=True, timestamp_ms=ms, dataset_id="dataset2")
    logger.log(data={"e": 2}, sync=True, timestamp_ms=add_days(ms, 3), dataset_id="dataset2")
    logger.log(data={"f": 3}, sync=True, timestamp_ms=add_days(ms, 4), dataset_id="dataset2")

    status = logger.status()
    logger.close()

    assert status == {
        "dataset1": ProcessLoggerStatus(
            dataset_id="dataset1",
            status=LoggerStatus(
                dataset_profiles=3,
                dataset_timestamps=3,
                pending_writables=0,
                segment_caches=0,
                writers=1,
            ),
        ),
        "dataset2": ProcessLoggerStatus(
            dataset_id="dataset2",
            status=LoggerStatus(
                dataset_profiles=3,
                dataset_timestamps=3,
                pending_writables=0,
                segment_caches=0,
                writers=1,
            ),
        ),
    }

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
    status: Union[Dict[str, ProcessLoggerStatus], LoggerStatus], expected: LoggerStatus, dataset_id: str = ""
) -> None:
    if isinstance(status, dict):
        assert status[dataset_id] == ProcessLoggerStatus(dataset_id=dataset_id, status=expected)
    else:
        assert status == expected
